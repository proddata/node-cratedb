import { describe, it, expect } from 'vitest';
import { CrateDBSerializer } from '../src/CrateDBSerializer';

const bigintStr = '9223372036854775808';

describe('CrateDBSerializer', () => {
  it('should stringify bigint', async () => {
    const obj = { a: 1, b: BigInt(bigintStr), c: bigintStr };
    const str = await CrateDBSerializer.stringify(obj);
    expect(str).toBe(`{"a":1,"b":${bigintStr},"c":"${bigintStr}"}`);
  });

  it('should parse bigint', async () => {
    const str = `{"a":1,"b":"${bigintStr}"}`;
    const obj = await CrateDBSerializer.parse(str);
    expect(obj).toEqual({ a: 1, b: bigintStr });
  });

  it('should parse bigint', async () => {
    const str = '{"a":{"b":9223372036854775808}}';
    const obj = await CrateDBSerializer.parse(str);
    expect(obj).toEqual({ a: { b: BigInt(bigintStr) } });
  });

  it('should deserialize CrateDB result', async () => {
    const result =
      '{"col_types":[9,10,6],"cols":["a","b","c"],"rows":[[1,9223372036854775808,9223372036854775808.1],[1,1,1.0]],"rowcount":1}';
    const obj = await CrateDBSerializer.deserialize(result);
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
