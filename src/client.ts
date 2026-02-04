import path from 'node:path';

import { ActrRef, ActrSystem } from '@actor-rtc/actr';
import type {
  ActrId,
  ContextBridge,
  DataStream,
  PayloadType,
  RpcEnvelopeBridge,
  Workload,
} from '@actor-rtc/actr';

import {
  ATTACH_ROUTE_KEY,
  PROMPT_ROUTE_KEY,
  decodeAssistantReply,
  decodeAttachResponse,
  encodeAttachRequest,
  encodeUsrPromptRequest,
} from './generated/ask.client.js';
import {
  Ask_AssistantReply,
  Ask_AttachRequest,
  Ask_AttachResponse,
  Ask_UsrPromptRequest,
} from './generated/ask.pb.js';

const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_PAYLOAD_TYPE: PayloadType = 0;

const ROUTES = [
  {
    routeKey: PROMPT_ROUTE_KEY,
    targetType: { manufacturer: 'askaway', name: 'AskService' },
  },
  {
    routeKey: ATTACH_ROUTE_KEY,
    targetType: { manufacturer: 'askaway', name: 'AskService' },
  },
] as const;

class AskClientWorkload implements Workload {
  private ctx?: ContextBridge;

  async onStart(ctx: ContextBridge): Promise<void> {
    this.ctx = ctx;
  }

  async onStop(_ctx: ContextBridge): Promise<void> {
    this.ctx = undefined;
  }

  getContext(): ContextBridge | undefined {
    return this.ctx;
  }

  async dispatch(ctx: ContextBridge, envelope: RpcEnvelopeBridge): Promise<Buffer> {
    const match = ROUTES.find((route) => route.routeKey === envelope.routeKey);
    if (!match) {
      throw new Error(`Unknown route: ${envelope.routeKey}`);
    }

    const targetId = await ctx.discover(match.targetType);
    return await ctx.callRaw(
      targetId,
      envelope.routeKey,
      DEFAULT_PAYLOAD_TYPE,
      envelope.payload,
      DEFAULT_TIMEOUT_MS
    );
  }
}

function resolveConfigPath(configPath?: string): string {
  if (configPath && configPath.trim().length > 0) {
    return configPath;
  }

  const configured = process.env.ACTR_CONFIG;
  if (configured && configured.trim().length > 0) {
    return configured;
  }

  return path.resolve(process.cwd(), 'Actr.toml');
}

export class AskServiceClient {
  private constructor(
    private readonly actorRef: ActrRef,
    private readonly workload: AskClientWorkload
  ) { }

  static async connect(configPath?: string): Promise<AskServiceClient> {
    const resolvedPath = resolveConfigPath(configPath);
    const system = await ActrSystem.fromConfig(resolvedPath);
    const workload = new AskClientWorkload();
    const node = system.attach(workload);
    const actorRef = await node.start();
    return new AskServiceClient(actorRef, workload);
  }

  actorId(): ActrId {
    return this.actorRef.actorId();
  }

  async prompt(
    request: Ask_UsrPromptRequest,
    timeoutMs: number = DEFAULT_TIMEOUT_MS
  ): Promise<Ask_AssistantReply> {
    const payload = encodeUsrPromptRequest(request);
    const responsePayload = await this.actorRef.call(
      PROMPT_ROUTE_KEY,
      DEFAULT_PAYLOAD_TYPE,
      payload,
      timeoutMs
    );
    const response = decodeAssistantReply(responsePayload);

    if (request.voiceStreamId) {
      void this.sendMockAudioStream(request.voiceStreamId);
    }

    // If we have a streamId, register a callback to print incoming data
    if (response.streamId) {
      const ctx = this.workload.getContext();
      if (ctx) {
        console.log(`Registering stream callback for: ${response.streamId}`);
        await ctx.registerStream(response.streamId, (chunk, sender) => {
          if (!chunk) {
            console.log(`Stream ${response.streamId} finished.`);
            return;
          }
          console.log(`Received stream chunk from ${JSON.stringify(sender)}:`, {
            streamId: chunk.streamId,
            sequence: chunk.sequence,
            payload: chunk.payload.toString('utf8'),
          });
        });
      } else {
        console.warn('Cannot register stream: ContextBridge not available');
      }
    }

    return response;
  }

  async attach(
    request: Ask_AttachRequest,
    timeoutMs: number = DEFAULT_TIMEOUT_MS
  ): Promise<Ask_AttachResponse> {
    const payload = encodeAttachRequest(request);
    const response = await this.actorRef.call(
      ATTACH_ROUTE_KEY,
      DEFAULT_PAYLOAD_TYPE,
      payload,
      timeoutMs
    );
    return decodeAttachResponse(response);
  }

  async close(): Promise<void> {
    await this.actorRef.stop();
  }

  private async sendMockAudioStream(streamId: string): Promise<void> {
    const ctx = this.workload.getContext();
    if (!ctx) {
      console.warn('Cannot send audio stream: ContextBridge not available');
      return;
    }

    try {
      const targetId = await ctx.discover(ROUTES[0].targetType);
      for (let i = 1; i <= 3; i += 1) {
        const chunk: DataStream = {
          streamId,
          sequence: i,
          payload: Buffer.from(`audio-chunk-${i}`),
          metadata: [{ key: 'content-type', value: 'audio/pcm' }],
        };

        await ctx.sendDataStream(targetId, chunk);
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
    } catch (error) {
      console.error(`Error sending audio stream ${streamId}:`, error);
    }
  }
}
