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
  deserialization: DeserializationConfig;
}

export type DeserializationConfig = {
  long: 'bigint' | 'number';
  timestamp: 'date' | 'number';
  date: 'date' | 'number';
};

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
  results?: Array<CrateDBBulkRecord>;
}

export interface CrateDBResponse extends CrateDBBaseResponse {
  cols: string[];
  col_types: number[];
  rows: unknown[][];
  rowcount: number;
  duration: number;
}

export interface CrateDBBulkResponse extends CrateDBBaseResponse {
  results: Array<CrateDBBulkRecord>;
  bulk_errors?: number[];
  duration: number;
}

export interface CrateDBBulkRecord {
  rowcount: number;
  error?: {
    code: number;
    message: string;
  };
}

export interface CrateDBErrorResponse {
  error: {
    message: string;
    code: number;
  };
  error_trace?: string;
}

export type CrateDBRecord = Record<string, unknown>;

export type OptimizeOptions = {
  max_num_segments?: number; // Defines the number of segments to merge to
  only_expunge_deletes?: boolean; // If true, only segments with deletes are merged
  flush?: boolean; // If false, prevents automatic flushing after optimization
};

/**
 * Defines the structure of a column in a CrateDB table.
 */
export type ColumnDefinition = {
  type: string;
  primaryKey?: boolean;
  notNull?: boolean;
  defaultValue?: unknown;
};

/**
 * Defines additional table options such as clustering, partitioning, and replicas.
 */
export type TableOptions = {
  clusteredBy?: string; // Column to cluster by
  partitionedBy?: string[]; // List of partition key columns
  numberOfShards?: number; // Replication factor
  numberOfReplicas?: string | number; // Replication factor
};
