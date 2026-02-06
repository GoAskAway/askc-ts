import crypto from 'node:crypto';

import { AskServiceClient } from './client.js';

async function main(): Promise<void> {
  const client = await AskServiceClient.connect();
  try {
    const promptText = 'Hello from askc-ts';
    console.log('AskService request text:', promptText);
    const reply = await client.prompt({
      questionId: `q-${Date.now()}`,
      sessionId: `s-${Date.now()}`,
      text: promptText,
      voiceStreamId: '',
      latitude: 0,
      longitude: 0,
      address: '',
      placeName: '',
      attachmentIds: [],
      textResponseStreamId: crypto.randomUUID().toString(),
      voiceResponseStreamId: crypto.randomUUID().toString(),
    });

    console.log('AskService reply text:', reply.text);
    console.log('AskService reply:', reply);
    await new Promise((r) => setTimeout(r, 2000));
  } catch (error) {
    console.error('RPC call failed:', error);
    throw error;
  } finally {
    await client.close();
    process.exit(0);
  }
}

main().catch((error) => {
  console.error('askc-ts demo failed:', error);
  process.exitCode = 1;
});
