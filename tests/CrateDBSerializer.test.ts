import { describe, it, expect } from 'vitest';
import { Serializer } from '../src/Serializer';
import { DeserializationConfig } from '../src/interfaces';
import { DeserializationError } from '../src/utils/Error';

const bigintStr = '9223372036854775808';
const config: DeserializationConfig = { long: 'bigint', timestamp: 'number', date: 'number' };

describe('CrateDBSerializer', () => {
  it('should serialize bigint', () => {
    const obj = { a: 1, b: BigInt(bigintStr), c: bigintStr };
    const str = Serializer.serialize(obj);
    expect(str).toBe(`{"a":1,"b":${bigintStr},"c":"${bigintStr}"}`);
  });

  it('should deserialize bigint from string representation', () => {
    const str = `{"a":1,"b":"${bigintStr}"}`;
    const obj = Serializer.deserialize(str, config);
    expect(obj).toEqual({ a: 1, b: bigintStr });
  });

  it('should throw an error when deserialization fails', () => {
    const str = `{"a":1,b":"${bigintStr}"}`; // Invalid JSON syntax

    expect(() => {
      Serializer.deserialize(str, config);
    }).toThrow(DeserializationError);
  });

  it('should deserialize nested bigint', () => {
    const str = '{"a":{"b":9223372036854775808}}';
    const obj = Serializer.deserialize(str, config);
    expect(obj).toEqual({ a: { b: BigInt(bigintStr) } });
  });

  it('should deserialize CrateDB result', () => {
    const result =
      '{"col_types":[9,10,6],"cols":["a","b","c"],"rows":[[1,9223372036854775808,9223372036854775808.1],[1,1,1.0]],"rowcount":1}';
    const obj = Serializer.deserialize(result, config);
    expect(obj).toEqual({
      col_types: [9, 10, 6],
      cols: ['a', 'b', 'c'],
      rows: [
        [1, BigInt('9223372036854775808'), 9223372036854776000],
        [1, BigInt(1), 1.0],
      ],
      rowcount: 1,
    });
  });
});
