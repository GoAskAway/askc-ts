#!/usr/bin/env node

'use strict';

const fs = require('fs');
const path = require('path');
const protobuf = require('protobufjs');
const toml = require('@iarna/toml');

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith('--')) continue;
    const key = arg.slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith('--')) {
      args[key] = next;
      i += 1;
    } else {
      args[key] = true;
    }
  }
  return args;
}

function usage() {
  const message = [
    'Usage: generate-generated --config <Actr.toml> --remote <dir> [--local <dir>] [--out <generated-dir>] [--dist-import <path>] [--emit-js]',
    '',
    'Options:',
    '  --config       Path to Actr.toml configuration file',
    '  --remote       Directory containing remote service .proto files',
    '  --local        Directory containing local service .proto files (optional)',
    '  --out          Output directory for generated files (default: generated/)',
    '  --dist-import  Import path for actr runtime (default: relative to dist/)',
    '  --emit-js      Also emit CommonJS .js files',
    '',
    'Example:',
    '  node scripts/generate-generated.cjs --config Actr.toml --remote askaway-proto/ask-service --local askaway-proto/client-service --out src/generated',
  ].join('\n');
  console.error(message);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function removeFileIfExists(filePath) {
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
}

function readToml(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  return toml.parse(raw);
}

function findRepoRoot(startDir) {
  let dir = startDir;
  while (true) {
    if (fs.existsSync(path.join(dir, 'package.json'))) return dir;
    const parent = path.dirname(dir);
    if (parent === dir) return startDir;
    dir = parent;
  }
}

function toPosix(p) {
  return p.split(path.sep).join('/');
}

function toRelativeImport(fromDir, targetPath) {
  let rel = toPosix(path.relative(fromDir, targetPath));
  if (!rel.startsWith('.')) rel = `./${rel}`;
  return rel;
}

function parseActrType(actrType) {
  if (!actrType) return null;
  const match = String(actrType).split('+');
  if (match.length < 2) return null;
  return { manufacturer: match[0], name: match.slice(1).join('+') };
}

function loadDependencies(configPath) {
  const dependencies = [];

  if (fs.existsSync(configPath)) {
    const config = readToml(configPath);
    if (config.package && config.package.actr_type) {
      dependencies.push({
        name: config.package.actr_type.name,
        actrType: config.package.actr_type,
      });
    }

    const deps = config.dependencies || {};
    for (const name of Object.keys(deps)) {
      dependencies.push({
        name,
        actrType: parseActrType(deps[name].actr_type),
      });
    }
  }

  return dependencies;
}

function collectProtoFiles(protoDir) {
  const files = [];

  function walk(dir) {
    if (!fs.existsSync(dir)) return;
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(fullPath);
      } else if (entry.isFile() && entry.name.endsWith('.proto')) {
        files.push(fullPath);
      }
    }
  }

  walk(protoDir);
  return files;
}

function pascalCase(value) {
  return String(value)
    .split(/[^a-zA-Z0-9]+/g)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
}

function toLowerCamel(value) {
  const str = String(value);
  if (!str) return str;
  return str.charAt(0).toLowerCase() + str.slice(1);
}

function getPackageName(obj) {
  let node = obj.parent;
  const parts = [];
  while (node && !(node instanceof protobuf.Root)) {
    if (node instanceof protobuf.Namespace && !(node instanceof protobuf.Type)) {
      if (node.name) parts.unshift(node.name);
    }
    node = node.parent;
  }
  return parts.join('.');
}

function getTypePath(type) {
  const names = [type.name];
  let node = type.parent;
  while (node && node instanceof protobuf.Type) {
    names.unshift(node.name);
    node = node.parent;
  }
  return names.join('_');
}

function getEnumPath(enumType) {
  const names = [enumType.name];
  let node = enumType.parent;
  while (node && node instanceof protobuf.Type) {
    names.unshift(node.name);
    node = node.parent;
  }
  return names.join('_');
}

function buildTypeNameMap(packages) {
  const map = new Map();
  for (const [pkg, data] of packages.entries()) {
    const prefix = pkg ? `${pascalCase(pkg)}_` : '';
    for (const type of data.types) {
      const name = `${prefix}${getTypePath(type)}`;
      map.set(type.fullName, name);
    }
    for (const enumType of data.enums) {
      const name = `${prefix}${getEnumPath(enumType)}`;
      map.set(enumType.fullName, name);
    }
  }
  return map;
}

function collectPackages(root) {
  const packages = new Map();

  function ensure(pkg) {
    if (!packages.has(pkg)) packages.set(pkg, { types: [], enums: [], services: [] });
    return packages.get(pkg);
  }

  function visit(namespace) {
    if (!namespace.nestedArray) return;
    for (const nested of namespace.nestedArray) {
      if (nested instanceof protobuf.Type) {
        const pkg = getPackageName(nested);
        ensure(pkg).types.push(nested);
      }
      if (nested instanceof protobuf.Enum) {
        const pkg = getPackageName(nested);
        ensure(pkg).enums.push(nested);
      }
      if (nested instanceof protobuf.Service) {
        const pkg = getPackageName(nested);
        ensure(pkg).services.push(nested);
      }
      if (nested.nestedArray) visit(nested);
    }
  }

  visit(root);
  return packages;
}

function tsFieldType(field, typeNameMap) {
  const fieldType = field.resolvedType ? typeNameMap.get(field.resolvedType.fullName) : field.type;
  const scalar = field.resolvedType ? fieldType : null;
  let base;
  switch (scalar || field.type) {
    case 'string':
      base = 'string';
      break;
    case 'bool':
      base = 'boolean';
      break;
    case 'bytes':
      base = 'Buffer';
      break;
    case 'double':
    case 'float':
    case 'int32':
    case 'uint32':
    case 'sint32':
    case 'fixed32':
    case 'sfixed32':
      base = 'number';
      break;
    case 'int64':
    case 'uint64':
    case 'sint64':
    case 'fixed64':
    case 'sfixed64':
      base = 'bigint';
      break;
    default:
      base = fieldType;
  }

  if (field.repeated) return `${base}[]`;
  if (field.resolvedType && field.resolvedType instanceof protobuf.Type) {
    return `${base} | undefined`;
  }
  return base;
}

function defaultValue(field) {
  if (field.repeated) return '[]';
  if (field.resolvedType && field.resolvedType instanceof protobuf.Enum) {
    return '0';
  }
  switch (field.type) {
    case 'string':
      return "''";
    case 'bool':
      return 'false';
    case 'bytes':
      return 'Buffer.alloc(0)';
    case 'int64':
    case 'uint64':
    case 'sint64':
    case 'fixed64':
    case 'sfixed64':
      return '0n';
    case 'double':
    case 'float':
    case 'int32':
    case 'uint32':
    case 'sint32':
    case 'fixed32':
    case 'sfixed32':
      return '0';
    default:
      if (field.resolvedType && field.resolvedType instanceof protobuf.Type) {
        return 'undefined';
      }
      return 'undefined';
  }
}

function fieldWireType(field) {
  switch (field.type) {
    case 'string':
    case 'bytes':
      return 2;
    case 'double':
    case 'fixed64':
    case 'sfixed64':
      return 1;
    case 'float':
    case 'fixed32':
    case 'sfixed32':
      return 5;
    default:
      if (field.resolvedType && field.resolvedType instanceof protobuf.Enum) return 0;
      if (field.resolvedType && field.resolvedType instanceof protobuf.Type) return 2;
      return 0;
  }
}

function encodeExpression(field, valueExpr, typeNameMap) {
  const typeName = field.resolvedType ? typeNameMap.get(field.resolvedType.fullName) : null;
  const bytesVar = `${valueExpr.replace(/[^a-zA-Z0-9_]/g, '_')}Bytes`;
  if (field.resolvedType && field.resolvedType instanceof protobuf.Enum) {
    return [
      'parts.push(encodeVarint(tag));',
      `parts.push(encodeVarint(${valueExpr}));`,
    ];
  }
  switch (field.type) {
    case 'string':
      return [
        `const ${bytesVar} = Buffer.from(${valueExpr}, 'utf8');`,
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeVarint(${bytesVar}.length));`,
        `parts.push(${bytesVar});`,
      ];
    case 'bytes':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeVarint(${valueExpr}.length));`,
        `parts.push(${valueExpr});`,
      ];
    case 'bool':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeVarint(${valueExpr} ? 1 : 0));`,
      ];
    case 'int32':
    case 'uint32':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeVarint(${valueExpr}));`,
      ];
    case 'sint32':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeVarint(encodeZigZag32(${valueExpr})));`,
      ];
    case 'int64':
    case 'uint64':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeVarintBigint(${valueExpr}));`,
      ];
    case 'sint64':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeVarintBigint(encodeZigZag64(${valueExpr})));`,
      ];
    case 'float':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeFloat32(${valueExpr}));`,
      ];
    case 'double':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeFloat64(${valueExpr}));`,
      ];
    case 'fixed32':
    case 'sfixed32':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeFixed32(${valueExpr}));`,
      ];
    case 'fixed64':
    case 'sfixed64':
      return [
        'parts.push(encodeVarint(tag));',
        `parts.push(encodeFixed64(${valueExpr}));`,
      ];
    default:
      if (field.resolvedType && field.resolvedType instanceof protobuf.Type) {
        return [
          `const ${bytesVar} = ${typeName}.encode(${valueExpr});`,
          'parts.push(encodeVarint(tag));',
          `parts.push(encodeVarint(${bytesVar}.length));`,
          `parts.push(${bytesVar});`,
        ];
      }
      return [];
  }
}

function decodeValueExpression(field, typeNameMap) {
  const typeName = field.resolvedType ? typeNameMap.get(field.resolvedType.fullName) : null;
  if (field.resolvedType && field.resolvedType instanceof protobuf.Enum) {
    return 'varintToNumber(valueResult.value, \'enum\')';
  }
  switch (field.type) {
    case 'string':
      return 'value.toString(\'utf8\')';
    case 'bytes':
      return 'value';
    case 'bool':
      return 'varintToNumber(valueResult.value, \'bool\') !== 0';
    case 'int32':
    case 'uint32':
      return 'varintToNumber(valueResult.value, \'int32\')';
    case 'sint32':
      return 'decodeZigZag32(varintToNumber(valueResult.value, \'sint32\'))';
    case 'int64':
    case 'uint64':
      return 'valueResult.value';
    case 'sint64':
      return 'decodeZigZag64(valueResult.value)';
    case 'float':
      return 'readFloat32(value)';
    case 'double':
      return 'readFloat64(value)';
    case 'fixed32':
    case 'sfixed32':
      return 'readFixed32(value)';
    case 'fixed64':
    case 'sfixed64':
      return 'readFixed64(value)';
    default:
      if (field.resolvedType && field.resolvedType instanceof protobuf.Type) {
        return `${typeName}.decode(value)`;
      }
      return 'undefined';
  }
}

function renderPbTs(pkg, enums, types, typeNameMap, protoLabel) {
  const lines = [];
  lines.push('// DO NOT EDIT.');
  lines.push('// @ts-nocheck');
  lines.push(`// Generated from ${protoLabel}`);
  lines.push('');

  for (const enumType of enums) {
    const exportName = typeNameMap.get(enumType.fullName);
    lines.push(`export enum ${exportName} {`);
    for (const [name, value] of Object.entries(enumType.values)) {
      lines.push(`  ${name} = ${value},`);
    }
    lines.push('}');
    lines.push('');
  }

  for (const type of types) {
    const exportName = typeNameMap.get(type.fullName);
    lines.push(`export interface ${exportName} {`);
    for (const field of type.fieldsArray) {
      lines.push(`  ${field.name}: ${tsFieldType(field, typeNameMap)};`);
    }
    lines.push('}');
    lines.push('');
  }

  for (const type of types) {
    const exportName = typeNameMap.get(type.fullName);
    lines.push(`export const ${exportName} = {`);
    lines.push(`  encode(message: ${exportName}): Buffer {`);
    lines.push('    const parts: Buffer[] = [];');
    lines.push('');

    for (const field of type.fieldsArray) {
      const tag = (field.id << 3) | fieldWireType(field);
      const valueExpr = field.repeated ? 'value' : `message.${field.name}`;
      if (field.repeated) {
        lines.push(`    for (const value of message.${field.name}) {`);
        lines.push(`      const tag = ${tag};`);
        const encodeLines = encodeExpression(field, valueExpr, typeNameMap);
        for (const line of encodeLines) {
          lines.push(`      ${line}`);
        }
        lines.push('    }');
        lines.push('');
      } else {
        lines.push(`    if (message.${field.name} !== undefined && message.${field.name} !== null) {`);
        lines.push(`      const tag = ${tag};`);
        const encodeLines = encodeExpression(field, valueExpr, typeNameMap);
        for (const line of encodeLines) {
          lines.push(`      ${line}`);
        }
        lines.push('    }');
        lines.push('');
      }
    }

    lines.push('    return Buffer.concat(parts);');
    lines.push('  },');
    lines.push('');
    lines.push(`  decode(buffer: Buffer): ${exportName} {`);
    lines.push('    let offset = 0;');

    for (const field of type.fieldsArray) {
      lines.push(`    let ${field.name} = ${defaultValue(field)};`);
    }

    lines.push('');
    lines.push('    while (offset < buffer.length) {');
    lines.push('      const tagResult = decodeVarint(buffer, offset);');
    lines.push('      const tag = Number(tagResult.value);');
    lines.push('      offset += tagResult.length;');
    lines.push('');
    lines.push('      const fieldNumber = tag >> 3;');
    lines.push('      const wireType = tag & 0x07;');
    lines.push('');

    const lengthDelimited = type.fieldsArray.filter((field) => fieldWireType(field) === 2);
    const varintFields = type.fieldsArray.filter((field) => fieldWireType(field) === 0);
    const fixed32Fields = type.fieldsArray.filter((field) => fieldWireType(field) === 5);
    const fixed64Fields = type.fieldsArray.filter((field) => fieldWireType(field) === 1);

    if (lengthDelimited.length > 0) {
      lines.push('      if (wireType === 2) {');
      lines.push('        const lengthResult = decodeVarint(buffer, offset);');
      lines.push('        const length = varintToNumber(lengthResult.value, \'length\');');
      lines.push('        offset += lengthResult.length;');
      lines.push('');
      lines.push('        const end = offset + length;');
      lines.push('        const value = buffer.subarray(offset, end);');
      lines.push('        offset = end;');
      lines.push('');
      lines.push('        switch (fieldNumber) {');
      for (const field of lengthDelimited) {
        const decodeValue = decodeValueExpression(field, typeNameMap);
        if (field.repeated) {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name}.push(${decodeValue});`);
          lines.push('            break;');
        } else {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name} = ${decodeValue};`);
          lines.push('            break;');
        }
      }
      lines.push('          default:');
      lines.push('            break;');
      lines.push('        }');
      lines.push('        continue;');
      lines.push('      }');
      lines.push('');
    }

    if (varintFields.length > 0) {
      lines.push('      if (wireType === 0) {');
      lines.push('        const valueResult = decodeVarint(buffer, offset);');
      lines.push('        offset += valueResult.length;');
      lines.push('');
      lines.push('        switch (fieldNumber) {');
      for (const field of varintFields) {
        const decodeValue = decodeValueExpression(field, typeNameMap);
        if (field.repeated) {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name}.push(${decodeValue});`);
          lines.push('            break;');
        } else {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name} = ${decodeValue};`);
          lines.push('            break;');
        }
      }
      lines.push('          default:');
      lines.push('            break;');
      lines.push('        }');
      lines.push('        continue;');
      lines.push('      }');
      lines.push('');
    }

    if (fixed32Fields.length > 0) {
      lines.push('      if (wireType === 5) {');
      lines.push('        const value = buffer.subarray(offset, offset + 4);');
      lines.push('        offset += 4;');
      lines.push('        switch (fieldNumber) {');
      for (const field of fixed32Fields) {
        const decodeValue = decodeValueExpression(field, typeNameMap);
        if (field.repeated) {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name}.push(${decodeValue});`);
          lines.push('            break;');
        } else {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name} = ${decodeValue};`);
          lines.push('            break;');
        }
      }
      lines.push('          default:');
      lines.push('            break;');
      lines.push('        }');
      lines.push('        continue;');
      lines.push('      }');
      lines.push('');
    }

    if (fixed64Fields.length > 0) {
      lines.push('      if (wireType === 1) {');
      lines.push('        const value = buffer.subarray(offset, offset + 8);');
      lines.push('        offset += 8;');
      lines.push('        switch (fieldNumber) {');
      for (const field of fixed64Fields) {
        const decodeValue = decodeValueExpression(field, typeNameMap);
        if (field.repeated) {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name}.push(${decodeValue});`);
          lines.push('            break;');
        } else {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name} = ${decodeValue};`);
          lines.push('            break;');
        }
      }
      lines.push('          default:');
      lines.push('            break;');
      lines.push('        }');
      lines.push('        continue;');
      lines.push('      }');
      lines.push('');
    }

    lines.push('      throw new Error(`Unsupported wire type: ${wireType}`);');
    lines.push('    }');
    lines.push('');
    lines.push('    return {');
    for (const field of type.fieldsArray) {
      lines.push(`      ${field.name},`);
    }
    lines.push('    };');
    lines.push('  },');
    lines.push('};');
    lines.push('');
  }

  lines.push('function encodeVarint(value) {');
  lines.push('  let v = value >>> 0;');
  lines.push('  const bytes = [];');
  lines.push('  while (v >= 0x80) {');
  lines.push('    bytes.push((v & 0x7f) | 0x80);');
  lines.push('    v >>>= 7;');
  lines.push('  }');
  lines.push('  bytes.push(v);');
  lines.push('  return Buffer.from(bytes);');
  lines.push('}');
  lines.push('');
  lines.push('function encodeVarintBigint(value) {');
  lines.push('  let v = BigInt(value);');
  lines.push('  const bytes = [];');
  lines.push('  while (v >= 0x80n) {');
  lines.push('    bytes.push(Number((v & 0x7fn) | 0x80n));');
  lines.push('    v >>= 7n;');
  lines.push('  }');
  lines.push('  bytes.push(Number(v));');
  lines.push('  return Buffer.from(bytes);');
  lines.push('}');
  lines.push('');
  lines.push('function decodeVarint(buffer, offset) {');
  lines.push('  let result = 0n;');
  lines.push('  let shift = 0n;');
  lines.push('  let i = 0;');
  lines.push('  while (offset + i < buffer.length) {');
  lines.push('    const byte = BigInt(buffer[offset + i]);');
  lines.push('    result |= (byte & 0x7fn) << shift;');
  lines.push('    i += 1;');
  lines.push('    if ((byte & 0x80n) === 0n) {');
  lines.push('      return { value: result, length: i };');
  lines.push('    }');
  lines.push('    shift += 7n;');
  lines.push('  }');
  lines.push("  throw new Error('Invalid varint: buffer ended unexpectedly');");
  lines.push('}');
  lines.push('');
  lines.push('function varintToNumber(value, label) {');
  lines.push('  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {');
  lines.push('    throw new Error(`Varint ${label} exceeds safe integer range`);');
  lines.push('  }');
  lines.push('  return Number(value);');
  lines.push('}');
  lines.push('');
  lines.push('function encodeZigZag32(value) {');
  lines.push('  return (value << 1) ^ (value >> 31);');
  lines.push('}');
  lines.push('');
  lines.push('function encodeZigZag64(value) {');
  lines.push('  const v = BigInt(value);');
  lines.push('  return (v << 1n) ^ (v >> 63n);');
  lines.push('}');
  lines.push('');
  lines.push('function decodeZigZag32(value) {');
  lines.push('  return (value >>> 1) ^ -(value & 1);');
  lines.push('}');
  lines.push('');
  lines.push('function decodeZigZag64(value) {');
  lines.push('  return (value >> 1n) ^ (-(value & 1n));');
  lines.push('}');
  lines.push('');
  lines.push('function encodeFixed32(value) {');
  lines.push('  const buf = Buffer.alloc(4);');
  lines.push('  buf.writeUInt32LE(value >>> 0, 0);');
  lines.push('  return buf;');
  lines.push('}');
  lines.push('');
  lines.push('function encodeFixed64(value) {');
  lines.push('  const buf = Buffer.alloc(8);');
  lines.push('  const v = BigInt(value);');
  lines.push('  buf.writeBigUInt64LE(v, 0);');
  lines.push('  return buf;');
  lines.push('}');
  lines.push('');
  lines.push('function encodeFloat32(value) {');
  lines.push('  const buf = Buffer.alloc(4);');
  lines.push('  buf.writeFloatLE(value, 0);');
  lines.push('  return buf;');
  lines.push('}');
  lines.push('');
  lines.push('function encodeFloat64(value) {');
  lines.push('  const buf = Buffer.alloc(8);');
  lines.push('  buf.writeDoubleLE(value, 0);');
  lines.push('  return buf;');
  lines.push('}');
  lines.push('');
  lines.push('function readFixed32(buffer) {');
  lines.push('  return buffer.readUInt32LE(0);');
  lines.push('}');
  lines.push('');
  lines.push('function readFixed64(buffer) {');
  lines.push('  return buffer.readBigUInt64LE(0);');
  lines.push('}');
  lines.push('');
  lines.push('function readFloat32(buffer) {');
  lines.push('  return buffer.readFloatLE(0);');
  lines.push('}');
  lines.push('');
  lines.push('function readFloat64(buffer) {');
  lines.push('  return buffer.readDoubleLE(0);');
  lines.push('}');

  return lines.join('\n');
}

function renderPbJs(pkg, enums, types, typeNameMap, protoLabel) {
  const exportsList = [];
  const lines = [];
  lines.push('"use strict";');
  lines.push('// DO NOT EDIT.');
  lines.push(`// Generated from ${protoLabel}`);
  lines.push('Object.defineProperty(exports, "__esModule", { value: true });');

  for (const enumType of enums) {
    const exportName = typeNameMap.get(enumType.fullName);
    exportsList.push(exportName);
  }

  for (const type of types) {
    const exportName = typeNameMap.get(type.fullName);
    exportsList.push(exportName);
  }

  if (exportsList.length > 0) {
    lines.push(`exports.${exportsList.join(' = exports.')} = void 0;`);
  }
  lines.push('');

  for (const enumType of enums) {
    const exportName = typeNameMap.get(enumType.fullName);
    lines.push(`const ${exportName} = {`);
    for (const [name, value] of Object.entries(enumType.values)) {
      lines.push(`  ${name}: ${value},`);
    }
    lines.push('};');
    lines.push(`exports.${exportName} = ${exportName};`);
    lines.push('');
  }

  for (const type of types) {
    const exportName = typeNameMap.get(type.fullName);
    lines.push(`const ${exportName} = {`);
    lines.push('  encode(message) {');
    lines.push('    const parts = [];');
    lines.push('');

    for (const field of type.fieldsArray) {
      const tag = (field.id << 3) | fieldWireType(field);
      const valueExpr = field.repeated ? 'value' : `message.${field.name}`;
      if (field.repeated) {
        lines.push(`    for (const value of message.${field.name}) {`);
        lines.push(`      const tag = ${tag};`);
        const encodeLines = encodeExpression(field, valueExpr, typeNameMap);
        for (const line of encodeLines) {
          lines.push(`      ${line}`);
        }
        lines.push('    }');
        lines.push('');
      } else {
        lines.push(`    if (message.${field.name} !== undefined && message.${field.name} !== null) {`);
        lines.push(`      const tag = ${tag};`);
        const encodeLines = encodeExpression(field, valueExpr, typeNameMap);
        for (const line of encodeLines) {
          lines.push(`      ${line}`);
        }
        lines.push('    }');
        lines.push('');
      }
    }

    lines.push('    return Buffer.concat(parts);');
    lines.push('  },');
    lines.push('');
    lines.push('  decode(buffer) {');
    lines.push('    let offset = 0;');

    for (const field of type.fieldsArray) {
      lines.push(`    let ${field.name} = ${defaultValue(field)};`);
    }

    lines.push('');
    lines.push('    while (offset < buffer.length) {');
    lines.push('      const tagResult = decodeVarint(buffer, offset);');
    lines.push('      const tag = Number(tagResult.value);');
    lines.push('      offset += tagResult.length;');
    lines.push('');
    lines.push('      const fieldNumber = tag >> 3;');
    lines.push('      const wireType = tag & 0x07;');
    lines.push('');

    const lengthDelimited = type.fieldsArray.filter((field) => fieldWireType(field) === 2);
    const varintFields = type.fieldsArray.filter((field) => fieldWireType(field) === 0);
    const fixed32Fields = type.fieldsArray.filter((field) => fieldWireType(field) === 5);
    const fixed64Fields = type.fieldsArray.filter((field) => fieldWireType(field) === 1);

    if (lengthDelimited.length > 0) {
      lines.push('      if (wireType === 2) {');
      lines.push('        const lengthResult = decodeVarint(buffer, offset);');
      lines.push('        const length = varintToNumber(lengthResult.value, "length");');
      lines.push('        offset += lengthResult.length;');
      lines.push('');
      lines.push('        const end = offset + length;');
      lines.push('        const value = buffer.subarray(offset, end);');
      lines.push('        offset = end;');
      lines.push('');
      lines.push('        switch (fieldNumber) {');
      for (const field of lengthDelimited) {
        const decodeValue = decodeValueExpression(field, typeNameMap);
        if (field.repeated) {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name}.push(${decodeValue});`);
          lines.push('            break;');
        } else {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name} = ${decodeValue};`);
          lines.push('            break;');
        }
      }
      lines.push('          default:');
      lines.push('            break;');
      lines.push('        }');
      lines.push('        continue;');
      lines.push('      }');
      lines.push('');
    }

    if (varintFields.length > 0) {
      lines.push('      if (wireType === 0) {');
      lines.push('        const valueResult = decodeVarint(buffer, offset);');
      lines.push('        offset += valueResult.length;');
      lines.push('');
      lines.push('        switch (fieldNumber) {');
      for (const field of varintFields) {
        const decodeValue = decodeValueExpression(field, typeNameMap);
        if (field.repeated) {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name}.push(${decodeValue});`);
          lines.push('            break;');
        } else {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name} = ${decodeValue};`);
          lines.push('            break;');
        }
      }
      lines.push('          default:');
      lines.push('            break;');
      lines.push('        }');
      lines.push('        continue;');
      lines.push('      }');
      lines.push('');
    }

    if (fixed32Fields.length > 0) {
      lines.push('      if (wireType === 5) {');
      lines.push('        const value = buffer.subarray(offset, offset + 4);');
      lines.push('        offset += 4;');
      lines.push('        switch (fieldNumber) {');
      for (const field of fixed32Fields) {
        const decodeValue = decodeValueExpression(field, typeNameMap);
        if (field.repeated) {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name}.push(${decodeValue});`);
          lines.push('            break;');
        } else {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name} = ${decodeValue};`);
          lines.push('            break;');
        }
      }
      lines.push('          default:');
      lines.push('            break;');
      lines.push('        }');
      lines.push('        continue;');
      lines.push('      }');
      lines.push('');
    }

    if (fixed64Fields.length > 0) {
      lines.push('      if (wireType === 1) {');
      lines.push('        const value = buffer.subarray(offset, offset + 8);');
      lines.push('        offset += 8;');
      lines.push('        switch (fieldNumber) {');
      for (const field of fixed64Fields) {
        const decodeValue = decodeValueExpression(field, typeNameMap);
        if (field.repeated) {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name}.push(${decodeValue});`);
          lines.push('            break;');
        } else {
          lines.push(`          case ${field.id}:`);
          lines.push(`            ${field.name} = ${decodeValue};`);
          lines.push('            break;');
        }
      }
      lines.push('          default:');
      lines.push('            break;');
      lines.push('        }');
      lines.push('        continue;');
      lines.push('      }');
      lines.push('');
    }

    lines.push('      throw new Error(`Unsupported wire type: ${wireType}`);');
    lines.push('    }');
    lines.push('');
    lines.push('    return {');
    for (const field of type.fieldsArray) {
      lines.push(`      ${field.name},`);
    }
    lines.push('    };');
    lines.push('  },');
    lines.push('};');
    lines.push('');
    lines.push(`exports.${exportName} = ${exportName};`);
    lines.push('');
  }

  lines.push('function encodeVarint(value) {');
  lines.push('  let v = value >>> 0;');
  lines.push('  const bytes = [];');
  lines.push('  while (v >= 0x80) {');
  lines.push('    bytes.push((v & 0x7f) | 0x80);');
  lines.push('    v >>>= 7;');
  lines.push('  }');
  lines.push('  bytes.push(v);');
  lines.push('  return Buffer.from(bytes);');
  lines.push('}');
  lines.push('');
  lines.push('function encodeVarintBigint(value) {');
  lines.push('  let v = BigInt(value);');
  lines.push('  const bytes = [];');
  lines.push('  while (v >= 0x80n) {');
  lines.push('    bytes.push(Number((v & 0x7fn) | 0x80n));');
  lines.push('    v >>= 7n;');
  lines.push('  }');
  lines.push('  bytes.push(Number(v));');
  lines.push('  return Buffer.from(bytes);');
  lines.push('}');
  lines.push('');
  lines.push('function decodeVarint(buffer, offset) {');
  lines.push('  let result = 0n;');
  lines.push('  let shift = 0n;');
  lines.push('  let i = 0;');
  lines.push('  while (offset + i < buffer.length) {');
  lines.push('    const byte = BigInt(buffer[offset + i]);');
  lines.push('    result |= (byte & 0x7fn) << shift;');
  lines.push('    i += 1;');
  lines.push('    if ((byte & 0x80n) === 0n) {');
  lines.push('      return { value: result, length: i };');
  lines.push('    }');
  lines.push('    shift += 7n;');
  lines.push('  }');
  lines.push("  throw new Error('Invalid varint: buffer ended unexpectedly');");
  lines.push('}');
  lines.push('');
  lines.push('function varintToNumber(value, label) {');
  lines.push('  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {');
  lines.push('    throw new Error(`Varint ${label} exceeds safe integer range`);');
  lines.push('  }');
  lines.push('  return Number(value);');
  lines.push('}');
  lines.push('');
  lines.push('function encodeZigZag32(value) {');
  lines.push('  return (value << 1) ^ (value >> 31);');
  lines.push('}');
  lines.push('');
  lines.push('function encodeZigZag64(value) {');
  lines.push('  const v = BigInt(value);');
  lines.push('  return (v << 1n) ^ (v >> 63n);');
  lines.push('}');
  lines.push('');
  lines.push('function decodeZigZag32(value) {');
  lines.push('  return (value >>> 1) ^ -(value & 1);');
  lines.push('}');
  lines.push('');
  lines.push('function decodeZigZag64(value) {');
  lines.push('  return (value >> 1n) ^ (-(value & 1n));');
  lines.push('}');
  lines.push('');
  lines.push('function encodeFixed32(value) {');
  lines.push('  const buf = Buffer.alloc(4);');
  lines.push('  buf.writeUInt32LE(value >>> 0, 0);');
  lines.push('  return buf;');
  lines.push('}');
  lines.push('');
  lines.push('function encodeFixed64(value) {');
  lines.push('  const buf = Buffer.alloc(8);');
  lines.push('  const v = BigInt(value);');
  lines.push('  buf.writeBigUInt64LE(v, 0);');
  lines.push('  return buf;');
  lines.push('}');
  lines.push('');
  lines.push('function encodeFloat32(value) {');
  lines.push('  const buf = Buffer.alloc(4);');
  lines.push('  buf.writeFloatLE(value, 0);');
  lines.push('  return buf;');
  lines.push('}');
  lines.push('');
  lines.push('function encodeFloat64(value) {');
  lines.push('  const buf = Buffer.alloc(8);');
  lines.push('  buf.writeDoubleLE(value, 0);');
  lines.push('  return buf;');
  lines.push('}');
  lines.push('');
  lines.push('function readFixed32(buffer) {');
  lines.push('  return buffer.readUInt32LE(0);');
  lines.push('}');
  lines.push('');
  lines.push('function readFixed64(buffer) {');
  lines.push('  return buffer.readBigUInt64LE(0);');
  lines.push('}');
  lines.push('');
  lines.push('function readFloat32(buffer) {');
  lines.push('  return buffer.readFloatLE(0);');
  lines.push('}');
  lines.push('');
  lines.push('function readFloat64(buffer) {');
  lines.push('  return buffer.readDoubleLE(0);');
  lines.push('}');

  return lines.join('\n');
}

function unique(arr) {
  return Array.from(new Set(arr));
}

function renderClientTs(pkg, services, typeNameMap) {
  const routeKeys = [];
  const importTypes = new Set();

  const methodNames = [];
  for (const service of services) {
    for (const method of service.methodsArray) {
      methodNames.push(method.name);
    }
  }
  const hasDuplicateMethod = methodNames.length !== new Set(methodNames).size;

  for (const service of services) {
    for (const method of service.methodsArray) {
      const routeKey = `${pkg}.${service.name}.${method.name}`;
      const constBase = hasDuplicateMethod ? `${service.name}_${method.name}` : method.name;
      const constName = `${constBase.replace(/[^a-zA-Z0-9]/g, '_').toUpperCase()}_ROUTE_KEY`;
      routeKeys.push({ constName, routeKey, method });
      const reqType = method.resolvedRequestType;
      const resType = method.resolvedResponseType;
      if (reqType) importTypes.add(typeNameMap.get(reqType.fullName));
      if (resType) importTypes.add(typeNameMap.get(resType.fullName));
    }
  }

  const imports = Array.from(importTypes).filter(Boolean).sort();
  const lines = [];
  lines.push('// DO NOT EDIT.');
  lines.push('// Generated by actr-ts-codegen.');
  lines.push('');
  if (imports.length > 0) {
    lines.push(`import { ${imports.join(', ')} } from './${pkg.replace(/\./g, '-')}.pb.js';`);
    lines.push('');
  }

  for (const entry of routeKeys) {
    lines.push(`export const ${entry.constName} = '${entry.routeKey}';`);
  }
  if (routeKeys.length > 0) lines.push('');

  for (const entry of routeKeys) {
    const reqType = entry.method.resolvedRequestType;
    if (reqType) {
      const typeName = typeNameMap.get(reqType.fullName);
      // Add routeKey to the type's companion object
      lines.push(`((${typeName} as any).routeKey = ${entry.constName});`);
    }
  }
  if (routeKeys.length > 0) lines.push('');

  return lines.join('\n');
}

function renderClientJs(pkg, services, typeNameMap) {
  const routeKeys = [];

  const methodNames = [];
  for (const service of services) {
    for (const method of service.methodsArray) {
      methodNames.push(method.name);
    }
  }
  const hasDuplicateMethod = methodNames.length !== new Set(methodNames).size;

  for (const service of services) {
    for (const method of service.methodsArray) {
      const routeKey = `${pkg}.${service.name}.${method.name}`;
      const constBase = hasDuplicateMethod ? `${service.name}_${method.name}` : method.name;
      const constName = `${constBase.replace(/[^a-zA-Z0-9]/g, '_').toUpperCase()}_ROUTE_KEY`;
      routeKeys.push({ constName, routeKey, method });
    }
  }

  const lines = [];
  lines.push('"use strict";');
  lines.push('// DO NOT EDIT.');
  lines.push('// Generated by actr-ts-codegen.');
  lines.push('Object.defineProperty(exports, "__esModule", { value: true });');

  const exportNames = [];
  for (const entry of routeKeys) exportNames.push(entry.constName);
  if (exportNames.length > 0) {
    lines.push(`exports.${exportNames.join(' = exports.')} = void 0;`);
  }
  lines.push('');

  for (const entry of routeKeys) {
    lines.push(`const ${entry.constName} = '${entry.routeKey}';`);
    lines.push(`exports.${entry.constName} = ${entry.constName};`);
  }
  if (routeKeys.length > 0) lines.push('');

  return lines.join('\n');
}

function getRouteConstName(serviceName, methodName, hasDuplicateMethod) {
  const constBase = hasDuplicateMethod ? `${serviceName}_${methodName}` : methodName;
  return `${constBase.replace(/[^a-zA-Z0-9]/g, '_').toUpperCase()}_ROUTE_KEY`;
}

function renderLocalActorTs(localServices, typeNameMap, packageMethodDuplicates, distImport) {
  const lines = [];
  lines.push('// DO NOT EDIT.');
  lines.push('// Generated by actr-ts-codegen.');
  lines.push('');
  lines.push(`import type { Context, RpcEnvelope } from '${distImport}';`);

  const localRoutes = [];
  const typeImports = new Map();

  for (const entry of localServices) {
    const fileBase = entry.pkg ? entry.pkg.replace(/\./g, '-') : 'root';
    const hasDuplicateMethod = packageMethodDuplicates.get(entry.pkg) || false;

    for (const method of entry.service.methodsArray) {
      const routeKey = `${entry.pkg}.${entry.service.name}.${method.name}`;
      const constName = getRouteConstName(entry.service.name, method.name, hasDuplicateMethod);
      const methodName = toLowerCamel(method.name);
      const reqType = typeNameMap.get(method.resolvedRequestType.fullName);
      const resType = typeNameMap.get(method.resolvedResponseType.fullName);

      localRoutes.push({
        constName,
        routeKey,
        methodName,
        reqType,
        resType,
      });

      if (!typeImports.has(fileBase)) typeImports.set(fileBase, new Set());
      typeImports.get(fileBase).add(reqType);
      typeImports.get(fileBase).add(resType);
    }
  }

  for (const [fileBase, types] of typeImports.entries()) {
    const names = Array.from(types).filter(Boolean).sort();
    if (names.length === 0) continue;
    lines.push(`import { ${names.join(', ')} } from './${fileBase}.pb.js';`);
  }

  lines.push('');

  for (const route of localRoutes) {
    lines.push(`export const ${route.constName} = '${route.routeKey}';`);
  }
  if (localRoutes.length > 0) lines.push('');

  for (const route of localRoutes) {
    lines.push(`((${route.reqType} as any).routeKey = ${route.constName});`);
  }
  if (localRoutes.length > 0) lines.push('');

  if (localRoutes.length > 0) {
    lines.push('export interface LocalHandler {');
    for (const route of localRoutes) {
      lines.push(`  ${route.methodName}(request: ${route.reqType}, ctx: Context): Promise<${route.resType}>;`);
    }
    lines.push('}');
    lines.push('');
  }

  lines.push('export async function dispatch(');
  if (localRoutes.length > 0) {
    lines.push('  handler: LocalHandler,');
  }
  lines.push('  ctx: Context,');
  lines.push('  envelope: RpcEnvelope');
  lines.push('): Promise<Buffer> {');

  for (const route of localRoutes) {
    lines.push(`  if (envelope.routeKey === ${route.constName}) {`);
    lines.push(`    const request = ${route.reqType}.decode(envelope.payload);`);
    lines.push(`    const response = await handler.${route.methodName}(request, ctx);`);
    lines.push(`    return ${route.resType}.encode(response);`);
    lines.push('  }');
    lines.push('');
  }

  lines.push('  throw new Error(`Unknown route: ${envelope.routeKey}`);');
  lines.push('}');

  return lines.join('\n');
}

function renderLocalActorJs(localServices, typeNameMap, packageMethodDuplicates) {
  const lines = [];
  lines.push('"use strict";');
  lines.push('// DO NOT EDIT.');
  lines.push('// Generated by actr-ts-codegen.');
  lines.push('Object.defineProperty(exports, "__esModule", { value: true });');

  const localRoutes = [];
  const requireImports = new Map();

  for (const entry of localServices) {
    const fileBase = entry.pkg ? entry.pkg.replace(/\./g, '-') : 'root';
    const hasDuplicateMethod = packageMethodDuplicates.get(entry.pkg) || false;

    for (const method of entry.service.methodsArray) {
      const routeKey = `${entry.pkg}.${entry.service.name}.${method.name}`;
      const constName = getRouteConstName(entry.service.name, method.name, hasDuplicateMethod);
      const methodName = toLowerCamel(method.name);
      const reqType = typeNameMap.get(method.resolvedRequestType.fullName);
      const resType = typeNameMap.get(method.resolvedResponseType.fullName);

      localRoutes.push({ constName, routeKey, methodName, reqType, resType, fileBase });

      if (!requireImports.has(fileBase)) requireImports.set(fileBase, new Set());
      requireImports.get(fileBase).add(reqType);
      requireImports.get(fileBase).add(resType);
    }
  }

  const exportNames = localRoutes.map((r) => r.constName);
  exportNames.push('dispatch');
  lines.push(`exports.${exportNames.join(' = exports.')} = void 0;`);
  lines.push('');

  for (const [fileBase, types] of requireImports.entries()) {
    const names = Array.from(types).filter(Boolean).sort();
    if (names.length === 0) continue;
    lines.push(`const pb_${fileBase.replace(/-/g, '_')} = require('./${fileBase}.pb');`);
  }
  lines.push('');

  for (const route of localRoutes) {
    lines.push(`const ${route.constName} = '${route.routeKey}';`);
    lines.push(`exports.${route.constName} = ${route.constName};`);
  }
  if (localRoutes.length > 0) lines.push('');

  lines.push('async function dispatch(handler, ctx, envelope) {');

  for (const route of localRoutes) {
    const pbAlias = `pb_${route.fileBase.replace(/-/g, '_')}`;
    lines.push(`  if (envelope.routeKey === ${route.constName}) {`);
    lines.push(`    const request = ${pbAlias}.${route.reqType}.decode(envelope.payload);`);
    lines.push(`    const response = await handler.${route.methodName}(request, ctx);`);
    lines.push(`    return ${pbAlias}.${route.resType}.encode(response);`);
    lines.push('  }');
    lines.push('');
  }

  lines.push('  throw new Error(`Unknown route: ${envelope.routeKey}`);');
  lines.push('}');
  lines.push('exports.dispatch = dispatch;');

  return lines.join('\n');
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.config || !args.remote) {
    usage();
    process.exit(1);
  }

  const configPath = path.resolve(args.config);
  const configDir = path.dirname(configPath);
  const outDir = path.resolve(args.out || path.join(configDir, 'generated'));
  const remoteDir = path.resolve(args.remote);
  const localDir = args.local ? path.resolve(args.local) : null;
  const repoRoot = findRepoRoot(configDir);
  const distImport = args['dist-import'] || args.distImport || toRelativeImport(outDir, path.join(repoRoot, 'dist', 'index.js'));
  const emitJs = Boolean(args['emit-js'] || args.emitJs);

  const dependencies = loadDependencies(configPath);

  // Collect remote proto files (required)
  const remoteProtoFiles = collectProtoFiles(remoteDir);
  if (remoteProtoFiles.length === 0) {
    throw new Error(`No .proto files found under ${remoteDir}`);
  }

  // Collect local proto files (optional, may be empty)
  const localProtoFiles = localDir ? collectProtoFiles(localDir) : [];

  const allProtoFiles = [...remoteProtoFiles, ...localProtoFiles];

  const root = await protobuf.load(allProtoFiles);
  root.resolveAll();

  const packages = collectPackages(root);
  const typeNameMap = buildTypeNameMap(packages);

  // Determine which packages come from local protos
  const localRoot = localProtoFiles.length > 0 ? await protobuf.load(localProtoFiles) : null;
  if (localRoot) localRoot.resolveAll();
  const localPackageNames = new Set();
  if (localRoot) {
    const localPackages = collectPackages(localRoot);
    for (const pkg of localPackages.keys()) {
      localPackageNames.add(pkg);
    }
  }

  ensureDir(outDir);

  const localServices = [];
  const packageMethodDuplicates = new Map();

  for (const [pkg, data] of packages.entries()) {
    if (data.types.length === 0 && data.services.length === 0) continue;

    const fileBase = pkg ? pkg.replace(/\./g, '-') : 'root';
    // Use only the proto files that define this package (remote vs local), not all inputs
    const packageProtoFiles = localPackageNames.has(pkg) ? localProtoFiles : remoteProtoFiles;
    const protoLabel = packageProtoFiles.map((file) => toPosix(path.relative(configDir, file))).join(', ');

    // Generate pb.ts for all packages (both remote and local)
    const pbTs = renderPbTs(pkg, data.enums, data.types, typeNameMap, protoLabel);
    const pbTsPath = path.join(outDir, `${fileBase}.pb.ts`);
    const pbJsPath = path.join(outDir, `${fileBase}.pb.js`);
    fs.writeFileSync(pbTsPath, pbTs);
    if (emitJs) {
      const pbJs = renderPbJs(pkg, data.enums, data.types, typeNameMap, protoLabel);
      fs.writeFileSync(pbJsPath, pbJs);
    } else {
      removeFileIfExists(pbJsPath);
    }

    // Generate client.ts for packages with services (skip for local packages; their route keys live in local.actor)
    if (data.services.length > 0) {
      const methodNames = data.services.flatMap((svc) => svc.methodsArray.map((method) => method.name));
      const hasDuplicateMethod = methodNames.length !== new Set(methodNames).size;
      packageMethodDuplicates.set(pkg, hasDuplicateMethod);

      if (!localPackageNames.has(pkg)) {
        const clientTs = renderClientTs(pkg, data.services, typeNameMap);
        const clientTsPath = path.join(outDir, `${fileBase}.client.ts`);
        const clientJsPath = path.join(outDir, `${fileBase}.client.js`);
        fs.writeFileSync(clientTsPath, clientTs);
        if (emitJs) {
          const clientJs = renderClientJs(pkg, data.services, typeNameMap);
          fs.writeFileSync(clientJsPath, clientJs);
        } else {
          removeFileIfExists(clientJsPath);
        }
      } else {
        removeFileIfExists(path.join(outDir, `${fileBase}.client.ts`));
        removeFileIfExists(path.join(outDir, `${fileBase}.client.js`));
      }

      if (localPackageNames.has(pkg)) {
        for (const service of data.services) {
          localServices.push({ pkg, service });
        }
      }
    }
  }

  // Generate local.actor.ts only if there are local services
  if (localServices.length > 0) {
    const localActorTs = renderLocalActorTs(
      localServices,
      typeNameMap,
      packageMethodDuplicates,
      distImport
    );
    const localActorTsPath = path.join(outDir, 'local.actor.ts');
    const localActorJsPath = path.join(outDir, 'local.actor.js');
    fs.writeFileSync(localActorTsPath, localActorTs);
    if (emitJs) {
      const localActorJs = renderLocalActorJs(localServices, typeNameMap, packageMethodDuplicates);
      fs.writeFileSync(localActorJsPath, localActorJs);
    } else {
      removeFileIfExists(localActorJsPath);
    }
  } else {
    // No local services — clean up stale local.actor files
    removeFileIfExists(path.join(outDir, 'local.actor.ts'));
    removeFileIfExists(path.join(outDir, 'local.actor.js'));
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
