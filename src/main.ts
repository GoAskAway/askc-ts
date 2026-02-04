import { randomUUID } from 'node:crypto';

import { AskServiceClient } from './client.js';

async function main(): Promise<void> {
  const client = await AskServiceClient.connect();
  try {
    const promptText = 'Hello from askc-ts';
    const streamId = randomUUID();
    const voiceStreamId = randomUUID();
    console.log('AskService request text:', promptText);
    const reply = await client.prompt({
      questionId: `q-${Date.now()}`,
      sessionId: `s-${Date.now()}`,
      text: promptText,
      voiceStreamId,
      location: undefined,
      attachmentIds: [],
      textResponseStreamId: streamId,
      voiceResponseStreamId: '',
    });

    console.log('AskService reply text:', reply.text);
    console.log('AskService reply:', reply);

    if (reply.streamId) {
      console.log(`Waiting for DataStream (streamId: ${reply.streamId})...`);
      // Wait longer for the stream chunks to arrive
      await new Promise((resolve) => setTimeout(resolve, 10000));
    }
  } catch (error) {
    console.error('RPC call failed:', error);
    throw error;
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error('askc-ts demo failed:', error);
  process.exitCode = 1;
});
