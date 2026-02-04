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
  decodeAssistantReply,
  decodeAttachResponse,
  encodeAttachRequest,
  encodeUsrPromptRequest,
} from './generated/ask.client.js';
import { dispatch as localDispatch } from './generated/local.actor.js';
import {
  Ask_AssistantReply,
  Ask_AttachRequest,
  Ask_AttachResponse,
  Ask_UsrPromptRequest,
} from './generated/ask.pb.js';

const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_PAYLOAD_TYPE: PayloadType = 0;

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
    return await localDispatch(this as any, ctx, envelope);
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
    const routeKey = (Ask_UsrPromptRequest as any).routeKey;
    if (!routeKey) {
      throw new Error('Ask_UsrPromptRequest has no routeKey associated');
    }
    const payload = encodeUsrPromptRequest(request);
    const responsePayload = await this.actorRef.call(
      routeKey,
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
    const routeKey = (Ask_AttachRequest as any).routeKey;
    if (!routeKey) {
      throw new Error('Ask_AttachRequest has no routeKey associated');
    }
    const payload = encodeAttachRequest(request);
    const response = await this.actorRef.call(
      routeKey,
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
      const targetType = { manufacturer: 'askaway1', name: 'AskService' };
      const targetId = await ctx.discover(targetType);
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
