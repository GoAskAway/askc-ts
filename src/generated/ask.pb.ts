// DO NOT EDIT.
// @ts-nocheck
// Generated from askaway-proto/ask.proto

export enum Ask_AttachmentType {
  ATTACHMENT_TYPE_UNKNOWN = 0,
  IMAGE = 1,
  DOCUMENT = 2,
  AUDIO = 3,
  VIDEO = 4,
  OTHER = 99,
}

export interface Ask_UsrPromptRequest {
  questionId: string;
  sessionId: string;
  text: string;
  voiceStreamId: string;
  location: Ask_Location | undefined;
  attachmentIds: string[];
  textResponseStreamId: string;
  voiceResponseStreamId: string;
}

export interface Ask_AssistantReply {
  questionId: string;
  sessionId: string;
  text: string;
  streamId: string;
  statusCode: number;
  errorMessage: string;
}

export interface Ask_AttachRequest {
  id: string;
  filename: string;
  type: Ask_AttachmentType;
  data: Buffer;
}

export interface Ask_AttachResponse {
  id: string;
  statusCode: number;
  errorMessage: string;
}

export interface Ask_Location {
  latitude: number;
  longitude: number;
  address: string;
  placeName: string;
}

export const Ask_UsrPromptRequest = {
  encode(message: Ask_UsrPromptRequest): Buffer {
    const parts: Buffer[] = [];

    if (message.questionId !== undefined && message.questionId !== null) {
      const tag = 10;
      const message_questionIdBytes = Buffer.from(message.questionId, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_questionIdBytes.length));
      parts.push(message_questionIdBytes);
    }

    if (message.sessionId !== undefined && message.sessionId !== null) {
      const tag = 18;
      const message_sessionIdBytes = Buffer.from(message.sessionId, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_sessionIdBytes.length));
      parts.push(message_sessionIdBytes);
    }

    if (message.text !== undefined && message.text !== null) {
      const tag = 26;
      const message_textBytes = Buffer.from(message.text, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_textBytes.length));
      parts.push(message_textBytes);
    }

    if (message.voiceStreamId !== undefined && message.voiceStreamId !== null) {
      const tag = 34;
      const message_voiceStreamIdBytes = Buffer.from(message.voiceStreamId, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_voiceStreamIdBytes.length));
      parts.push(message_voiceStreamIdBytes);
    }

    if (message.location !== undefined && message.location !== null) {
      const tag = 42;
      const message_locationBytes = Ask_Location.encode(message.location);
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_locationBytes.length));
      parts.push(message_locationBytes);
    }

    for (const value of message.attachmentIds) {
      const tag = 50;
      const valueBytes = Buffer.from(value, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(valueBytes.length));
      parts.push(valueBytes);
    }

    if (message.textResponseStreamId !== undefined && message.textResponseStreamId !== null) {
      const tag = 58;
      const message_textResponseStreamIdBytes = Buffer.from(message.textResponseStreamId, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_textResponseStreamIdBytes.length));
      parts.push(message_textResponseStreamIdBytes);
    }

    if (message.voiceResponseStreamId !== undefined && message.voiceResponseStreamId !== null) {
      const tag = 66;
      const message_voiceResponseStreamIdBytes = Buffer.from(message.voiceResponseStreamId, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_voiceResponseStreamIdBytes.length));
      parts.push(message_voiceResponseStreamIdBytes);
    }

    return Buffer.concat(parts);
  },

  decode(buffer: Buffer): Ask_UsrPromptRequest {
    let offset = 0;
    let questionId = '';
    let sessionId = '';
    let text = '';
    let voiceStreamId = '';
    let location = undefined;
    let attachmentIds = [];
    let textResponseStreamId = '';
    let voiceResponseStreamId = '';

    while (offset < buffer.length) {
      const tagResult = decodeVarint(buffer, offset);
      const tag = Number(tagResult.value);
      offset += tagResult.length;

      const fieldNumber = tag >> 3;
      const wireType = tag & 0x07;

      if (wireType === 2) {
        const lengthResult = decodeVarint(buffer, offset);
        const length = varintToNumber(lengthResult.value, 'length');
        offset += lengthResult.length;

        const end = offset + length;
        const value = buffer.subarray(offset, end);
        offset = end;

        switch (fieldNumber) {
          case 1:
            questionId = value.toString('utf8');
            break;
          case 2:
            sessionId = value.toString('utf8');
            break;
          case 3:
            text = value.toString('utf8');
            break;
          case 4:
            voiceStreamId = value.toString('utf8');
            break;
          case 5:
            location = Ask_Location.decode(value);
            break;
          case 6:
            attachmentIds.push(value.toString('utf8'));
            break;
          case 7:
            textResponseStreamId = value.toString('utf8');
            break;
          case 8:
            voiceResponseStreamId = value.toString('utf8');
            break;
          default:
            break;
        }
        continue;
      }

      throw new Error(`Unsupported wire type: ${wireType}`);
    }

    return {
      questionId,
      sessionId,
      text,
      voiceStreamId,
      location,
      attachmentIds,
      textResponseStreamId,
      voiceResponseStreamId,
    };
  },
};

export const Ask_AssistantReply = {
  encode(message: Ask_AssistantReply): Buffer {
    const parts: Buffer[] = [];

    if (message.questionId !== undefined && message.questionId !== null) {
      const tag = 10;
      const message_questionIdBytes = Buffer.from(message.questionId, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_questionIdBytes.length));
      parts.push(message_questionIdBytes);
    }

    if (message.sessionId !== undefined && message.sessionId !== null) {
      const tag = 18;
      const message_sessionIdBytes = Buffer.from(message.sessionId, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_sessionIdBytes.length));
      parts.push(message_sessionIdBytes);
    }

    if (message.text !== undefined && message.text !== null) {
      const tag = 26;
      const message_textBytes = Buffer.from(message.text, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_textBytes.length));
      parts.push(message_textBytes);
    }

    if (message.streamId !== undefined && message.streamId !== null) {
      const tag = 34;
      const message_streamIdBytes = Buffer.from(message.streamId, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_streamIdBytes.length));
      parts.push(message_streamIdBytes);
    }

    if (message.statusCode !== undefined && message.statusCode !== null) {
      const tag = 40;
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message.statusCode));
    }

    if (message.errorMessage !== undefined && message.errorMessage !== null) {
      const tag = 50;
      const message_errorMessageBytes = Buffer.from(message.errorMessage, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_errorMessageBytes.length));
      parts.push(message_errorMessageBytes);
    }

    return Buffer.concat(parts);
  },

  decode(buffer: Buffer): Ask_AssistantReply {
    let offset = 0;
    let questionId = '';
    let sessionId = '';
    let text = '';
    let streamId = '';
    let statusCode = 0;
    let errorMessage = '';

    while (offset < buffer.length) {
      const tagResult = decodeVarint(buffer, offset);
      const tag = Number(tagResult.value);
      offset += tagResult.length;

      const fieldNumber = tag >> 3;
      const wireType = tag & 0x07;

      if (wireType === 2) {
        const lengthResult = decodeVarint(buffer, offset);
        const length = varintToNumber(lengthResult.value, 'length');
        offset += lengthResult.length;

        const end = offset + length;
        const value = buffer.subarray(offset, end);
        offset = end;

        switch (fieldNumber) {
          case 1:
            questionId = value.toString('utf8');
            break;
          case 2:
            sessionId = value.toString('utf8');
            break;
          case 3:
            text = value.toString('utf8');
            break;
          case 4:
            streamId = value.toString('utf8');
            break;
          case 6:
            errorMessage = value.toString('utf8');
            break;
          default:
            break;
        }
        continue;
      }

      if (wireType === 0) {
        const valueResult = decodeVarint(buffer, offset);
        offset += valueResult.length;

        switch (fieldNumber) {
          case 5:
            statusCode = varintToNumber(valueResult.value, 'int32');
            break;
          default:
            break;
        }
        continue;
      }

      throw new Error(`Unsupported wire type: ${wireType}`);
    }

    return {
      questionId,
      sessionId,
      text,
      streamId,
      statusCode,
      errorMessage,
    };
  },
};

export const Ask_AttachRequest = {
  encode(message: Ask_AttachRequest): Buffer {
    const parts: Buffer[] = [];

    if (message.id !== undefined && message.id !== null) {
      const tag = 10;
      const message_idBytes = Buffer.from(message.id, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_idBytes.length));
      parts.push(message_idBytes);
    }

    if (message.filename !== undefined && message.filename !== null) {
      const tag = 18;
      const message_filenameBytes = Buffer.from(message.filename, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_filenameBytes.length));
      parts.push(message_filenameBytes);
    }

    if (message.type !== undefined && message.type !== null) {
      const tag = 24;
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message.type));
    }

    if (message.data !== undefined && message.data !== null) {
      const tag = 34;
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message.data.length));
      parts.push(message.data);
    }

    return Buffer.concat(parts);
  },

  decode(buffer: Buffer): Ask_AttachRequest {
    let offset = 0;
    let id = '';
    let filename = '';
    let type = 0;
    let data = Buffer.alloc(0);

    while (offset < buffer.length) {
      const tagResult = decodeVarint(buffer, offset);
      const tag = Number(tagResult.value);
      offset += tagResult.length;

      const fieldNumber = tag >> 3;
      const wireType = tag & 0x07;

      if (wireType === 2) {
        const lengthResult = decodeVarint(buffer, offset);
        const length = varintToNumber(lengthResult.value, 'length');
        offset += lengthResult.length;

        const end = offset + length;
        const value = buffer.subarray(offset, end);
        offset = end;

        switch (fieldNumber) {
          case 1:
            id = value.toString('utf8');
            break;
          case 2:
            filename = value.toString('utf8');
            break;
          case 4:
            data = value;
            break;
          default:
            break;
        }
        continue;
      }

      if (wireType === 0) {
        const valueResult = decodeVarint(buffer, offset);
        offset += valueResult.length;

        switch (fieldNumber) {
          case 3:
            type = varintToNumber(valueResult.value, 'enum');
            break;
          default:
            break;
        }
        continue;
      }

      throw new Error(`Unsupported wire type: ${wireType}`);
    }

    return {
      id,
      filename,
      type,
      data,
    };
  },
};

export const Ask_AttachResponse = {
  encode(message: Ask_AttachResponse): Buffer {
    const parts: Buffer[] = [];

    if (message.id !== undefined && message.id !== null) {
      const tag = 10;
      const message_idBytes = Buffer.from(message.id, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_idBytes.length));
      parts.push(message_idBytes);
    }

    if (message.statusCode !== undefined && message.statusCode !== null) {
      const tag = 16;
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message.statusCode));
    }

    if (message.errorMessage !== undefined && message.errorMessage !== null) {
      const tag = 26;
      const message_errorMessageBytes = Buffer.from(message.errorMessage, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_errorMessageBytes.length));
      parts.push(message_errorMessageBytes);
    }

    return Buffer.concat(parts);
  },

  decode(buffer: Buffer): Ask_AttachResponse {
    let offset = 0;
    let id = '';
    let statusCode = 0;
    let errorMessage = '';

    while (offset < buffer.length) {
      const tagResult = decodeVarint(buffer, offset);
      const tag = Number(tagResult.value);
      offset += tagResult.length;

      const fieldNumber = tag >> 3;
      const wireType = tag & 0x07;

      if (wireType === 2) {
        const lengthResult = decodeVarint(buffer, offset);
        const length = varintToNumber(lengthResult.value, 'length');
        offset += lengthResult.length;

        const end = offset + length;
        const value = buffer.subarray(offset, end);
        offset = end;

        switch (fieldNumber) {
          case 1:
            id = value.toString('utf8');
            break;
          case 3:
            errorMessage = value.toString('utf8');
            break;
          default:
            break;
        }
        continue;
      }

      if (wireType === 0) {
        const valueResult = decodeVarint(buffer, offset);
        offset += valueResult.length;

        switch (fieldNumber) {
          case 2:
            statusCode = varintToNumber(valueResult.value, 'int32');
            break;
          default:
            break;
        }
        continue;
      }

      throw new Error(`Unsupported wire type: ${wireType}`);
    }

    return {
      id,
      statusCode,
      errorMessage,
    };
  },
};

export const Ask_Location = {
  encode(message: Ask_Location): Buffer {
    const parts: Buffer[] = [];

    if (message.latitude !== undefined && message.latitude !== null) {
      const tag = 9;
      parts.push(encodeVarint(tag));
      parts.push(encodeFloat64(message.latitude));
    }

    if (message.longitude !== undefined && message.longitude !== null) {
      const tag = 17;
      parts.push(encodeVarint(tag));
      parts.push(encodeFloat64(message.longitude));
    }

    if (message.address !== undefined && message.address !== null) {
      const tag = 26;
      const message_addressBytes = Buffer.from(message.address, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_addressBytes.length));
      parts.push(message_addressBytes);
    }

    if (message.placeName !== undefined && message.placeName !== null) {
      const tag = 34;
      const message_placeNameBytes = Buffer.from(message.placeName, 'utf8');
      parts.push(encodeVarint(tag));
      parts.push(encodeVarint(message_placeNameBytes.length));
      parts.push(message_placeNameBytes);
    }

    return Buffer.concat(parts);
  },

  decode(buffer: Buffer): Ask_Location {
    let offset = 0;
    let latitude = 0;
    let longitude = 0;
    let address = '';
    let placeName = '';

    while (offset < buffer.length) {
      const tagResult = decodeVarint(buffer, offset);
      const tag = Number(tagResult.value);
      offset += tagResult.length;

      const fieldNumber = tag >> 3;
      const wireType = tag & 0x07;

      if (wireType === 2) {
        const lengthResult = decodeVarint(buffer, offset);
        const length = varintToNumber(lengthResult.value, 'length');
        offset += lengthResult.length;

        const end = offset + length;
        const value = buffer.subarray(offset, end);
        offset = end;

        switch (fieldNumber) {
          case 3:
            address = value.toString('utf8');
            break;
          case 4:
            placeName = value.toString('utf8');
            break;
          default:
            break;
        }
        continue;
      }

      if (wireType === 1) {
        const value = buffer.subarray(offset, offset + 8);
        offset += 8;
        switch (fieldNumber) {
          case 1:
            latitude = readFloat64(value);
            break;
          case 2:
            longitude = readFloat64(value);
            break;
          default:
            break;
        }
        continue;
      }

      throw new Error(`Unsupported wire type: ${wireType}`);
    }

    return {
      latitude,
      longitude,
      address,
      placeName,
    };
  },
};

function encodeVarint(value) {
  let v = value >>> 0;
  const bytes = [];
  while (v >= 0x80) {
    bytes.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  bytes.push(v);
  return Buffer.from(bytes);
}

function encodeVarintBigint(value) {
  let v = BigInt(value);
  const bytes = [];
  while (v >= 0x80n) {
    bytes.push(Number((v & 0x7fn) | 0x80n));
    v >>= 7n;
  }
  bytes.push(Number(v));
  return Buffer.from(bytes);
}

function decodeVarint(buffer, offset) {
  let result = 0n;
  let shift = 0n;
  let i = 0;
  while (offset + i < buffer.length) {
    const byte = BigInt(buffer[offset + i]);
    result |= (byte & 0x7fn) << shift;
    i += 1;
    if ((byte & 0x80n) === 0n) {
      return { value: result, length: i };
    }
    shift += 7n;
  }
  throw new Error('Invalid varint: buffer ended unexpectedly');
}

function varintToNumber(value, label) {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`Varint ${label} exceeds safe integer range`);
  }
  return Number(value);
}

function encodeZigZag32(value) {
  return (value << 1) ^ (value >> 31);
}

function encodeZigZag64(value) {
  const v = BigInt(value);
  return (v << 1n) ^ (v >> 63n);
}

function decodeZigZag32(value) {
  return (value >>> 1) ^ -(value & 1);
}

function decodeZigZag64(value) {
  return (value >> 1n) ^ (-(value & 1n));
}

function encodeFixed32(value) {
  const buf = Buffer.alloc(4);
  buf.writeUInt32LE(value >>> 0, 0);
  return buf;
}

function encodeFixed64(value) {
  const buf = Buffer.alloc(8);
  const v = BigInt(value);
  buf.writeBigUInt64LE(v, 0);
  return buf;
}

function encodeFloat32(value) {
  const buf = Buffer.alloc(4);
  buf.writeFloatLE(value, 0);
  return buf;
}

function encodeFloat64(value) {
  const buf = Buffer.alloc(8);
  buf.writeDoubleLE(value, 0);
  return buf;
}

function readFixed32(buffer) {
  return buffer.readUInt32LE(0);
}

function readFixed64(buffer) {
  return buffer.readBigUInt64LE(0);
}

function readFloat32(buffer) {
  return buffer.readFloatLE(0);
}

function readFloat64(buffer) {
  return buffer.readDoubleLE(0);
}