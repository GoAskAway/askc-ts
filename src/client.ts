import path from 'node:path';

import { ActrRef, ActrSystem } from '@actor-rtc/actr';
import type {
  Context,
  PayloadType,
  RpcEnvelope,
  StreamSignal,
  Workload,
} from '@actor-rtc/actr';

import {
  PROMPT_ROUTE_KEY as REMOTE_PROMPT_ROUTE_KEY,
  ATTACH_ROUTE_KEY as REMOTE_ATTACH_ROUTE_KEY,
} from './generated/ask.client.js';
import {
  Ask_AssistantReply,
  Ask_AttachmentType,
  Ask_AttachRequest,
  Ask_AttachResponse,
  Ask_UsrPromptRequest,
} from './generated/ask.pb.js';
import { PROMPT_ROUTE_KEY, ATTACH_ROUTE_KEY } from './generated/local.actor.js';
import type { LocalHandler } from './generated/local.actor.js';
import { dispatch } from './generated/local.actor.js';
import {
  Client_PromptRequest,
  Client_PromptResponse,
  Client_AttachRequest,
  Client_AttachResponse,
} from './generated/client.pb.js';

const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_PAYLOAD_TYPE: PayloadType = 0;

const ASK_SERVICE_TYPE = { manufacturer: 'askaway1', name: 'AskService' };

// ---------------------------------------------------------------------------
// LocalHandler implementation — forwards local requests to the remote
// AskService actor via discover + callRaw.
// ---------------------------------------------------------------------------

class AskServiceHandler implements LocalHandler {
  async prompt(request: Client_PromptRequest, ctx: Context): Promise<Client_PromptResponse> {
    console.log('prompt request', request);
    const remoteRequest: Ask_UsrPromptRequest = {
      questionId: request.questionId,
      sessionId: request.sessionId,
      text: request.text,
      voiceStreamId: request.voiceStreamId,
      location:
        request.latitude || request.longitude
          ? {
            latitude: request.latitude,
            longitude: request.longitude,
            address: request.address,
            placeName: request.placeName,
          }
          : undefined,
      attachmentIds: request.attachmentIds,
      textResponseStreamId: request.textResponseStreamId,
      voiceResponseStreamId: request.voiceResponseStreamId,
    };

    if (request.textResponseStreamId) {
      const streamId = request.textResponseStreamId;
      await ctx.registerStream(streamId, (err: Error | null, signal: StreamSignal) => {
        if (err) {
          console.error('[textResponseStream] callback error:', err);
          return;
        }
        if (!signal) {
          console.log('[textResponseStream] signal is empty');
          return;
        }
        console.log('[textResponseStream] seq=', signal.chunk.sequence, 'sender=', signal.sender);
      });
    }

    const payload = Ask_UsrPromptRequest.encode(remoteRequest);
    const targetId = await ctx.discover(ASK_SERVICE_TYPE);
    console.log('find targetId', targetId);
    const responsePayload = await ctx.callRaw(
      targetId,
      REMOTE_PROMPT_ROUTE_KEY,
      DEFAULT_PAYLOAD_TYPE,
      payload,
      DEFAULT_TIMEOUT_MS,
    );
    const remoteReply = Ask_AssistantReply.decode(responsePayload);

    return {
      questionId: remoteReply.questionId,
      sessionId: remoteReply.sessionId,
      text: remoteReply.text,
      statusCode: remoteReply.statusCode,
      errorMessage: remoteReply.errorMessage,
    };
  }

  async attach(request: Client_AttachRequest, ctx: Context): Promise<Client_AttachResponse> {
    const remoteRequest: Ask_AttachRequest = {
      id: request.id,
      filename: request.filename,
      type: request.type as number as Ask_AttachmentType,
      data: request.data,
    };

    const payload = Ask_AttachRequest.encode(remoteRequest);
    const targetId = await ctx.discover(ASK_SERVICE_TYPE);
    console.log('targetId', targetId);
    const responsePayload = await ctx.callRaw(
      targetId,
      REMOTE_ATTACH_ROUTE_KEY,
      DEFAULT_PAYLOAD_TYPE,
      payload,
      DEFAULT_TIMEOUT_MS,
    );
    const remoteReply = Ask_AttachResponse.decode(responsePayload);

    return {
      id: remoteReply.id,
      statusCode: remoteReply.statusCode,
      errorMessage: remoteReply.errorMessage,
    };
  }
}

// ---------------------------------------------------------------------------
// Workload — wires dispatch to the generated local.actor.ts dispatcher.
// ---------------------------------------------------------------------------

class AskClientWorkload implements Workload {
  private readonly handler = new AskServiceHandler();

  async onStart(_ctx: Context): Promise<void> { }

  async onStop(_ctx: Context): Promise<void> { }

  async dispatch(ctx: Context, envelope: RpcEnvelope): Promise<Buffer> {
    return await dispatch(this.handler, ctx, envelope);
  }
}

// ---------------------------------------------------------------------------
// Public client API
// ---------------------------------------------------------------------------

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
  private constructor(private readonly actorRef: ActrRef) { }

  static async connect(configPath?: string): Promise<AskServiceClient> {
    const resolvedPath = resolveConfigPath(configPath);
    const system = await ActrSystem.fromConfig(resolvedPath);
    const workload = new AskClientWorkload();
    const node = system.attach(workload);
    const actorRef = await node.start();
    return new AskServiceClient(actorRef);
  }

  /**
   * Send a prompt through the local dispatch pipeline, which forwards
   * to the remote AskService actor.
   */
  async prompt(
    request: Client_PromptRequest,
    timeoutMs: number = DEFAULT_TIMEOUT_MS,
  ): Promise<Client_PromptResponse> {
    const payload = Client_PromptRequest.encode(request);
    const responsePayload = await this.actorRef.call(
      PROMPT_ROUTE_KEY,
      DEFAULT_PAYLOAD_TYPE,
      payload,
      timeoutMs,
    );
    return Client_PromptResponse.decode(responsePayload);
  }

  /**
   * Upload an attachment through the local dispatch pipeline, which forwards
   * to the remote AskService actor.
   */
  async attach(
    request: Client_AttachRequest,
    timeoutMs: number = DEFAULT_TIMEOUT_MS,
  ): Promise<Client_AttachResponse> {
    const payload = Client_AttachRequest.encode(request);
    const responsePayload = await this.actorRef.call(
      ATTACH_ROUTE_KEY,
      DEFAULT_PAYLOAD_TYPE,
      payload,
      timeoutMs,
    );
    return Client_AttachResponse.decode(responsePayload);
  }

  async close(): Promise<void> {
    await this.actorRef.stop();
  }
}
