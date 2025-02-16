import { CrateDBTypes } from './CrateDBTypes';
import { CrateDBBaseResponse } from './interfaces';

export class CrateDBSerializer {
  static stringify(obj: unknown): string {
    return JSON.stringify(obj, replacer);
  }

  static parse(str: string): CrateDBBaseResponse {
    return JSON.parse(str, reviver);
  }

  static deserialize(str: string) {
    const obj = this.parse(str);
    obj.col_types?.forEach((type: number, index: number) => {
      switch (type) {
        case CrateDBTypes.BIGINT:
          obj.rows?.forEach((row: Array<unknown>) => {
            if (typeof row[index] === 'number') {
              row[index] = BigInt(String(row[index]));
            }
          });
          break;
        default:
      }
    });
    return obj;
  }
}

function replacer(_: unknown, value: unknown) {
  if (typeof value === 'bigint') {
    return JSON.rawJSON(value);
  }
  return value;
}

type Context = {
  source: string;
};

function reviver(_: unknown, value: unknown, context: Context | null = null): unknown {
  //if number is greater than Number.MAX_SAFE_INTEGER and not a float
  if (
    typeof value === 'number' &&
    value > Number.MAX_SAFE_INTEGER &&
    context !== null &&
    !context.source.includes('.')
  ) {
    return BigInt(context.source);
  }
  return value;
}
