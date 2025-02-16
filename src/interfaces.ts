export interface CrateDBConfig {
  user: string;
  password: string;
  jwt: string | null;
  host: string;
  port: number;
  defaultSchema: string | null;
  connectionString: string | null;
  ssl: boolean;
  keepAlive: boolean;
  maxConnections: number;
}

export interface CrateDBBaseResponse {
  cols?: string[];
  col_types?: number[];
  rows?: Array<Array<unknown>>;
  duration?: number;
  durations: {
    cratedb?: number;
    request: number;
    preparation?: number;
  };
  sizes: {
    request: number;
    response: number;
  };
  error?: { code: number; message: string };
}

export interface CrateDBResponse extends CrateDBBaseResponse {
  rows?: Array<Array<unknown>>;
  rowcount?: number;
}

export interface CrateDBBulkResponse extends CrateDBBaseResponse {
  results?: Array<CrateDBBulkRecord>;
  bulk_errors?: number[];
}

export interface CrateDBBulkRecord {
  rowcount: number;
  error?: {
    code: number;
    message: string;
  };
}

export type CrateDBRecord = Record<string, unknown>;
