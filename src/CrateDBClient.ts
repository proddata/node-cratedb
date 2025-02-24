'use strict';
import http, { AgentOptions } from 'http';
import https from 'https';
import { URL } from 'url';
import { Cursor } from './Cursor.js';
import { Serializer } from './Serializer.js';
import {
  CrateDBConfig,
  CrateDBBaseResponse,
  CrateDBResponse,
  CrateDBBulkResponse,
  CrateDBRecord,
  CrateDBErrorResponse,
  OptimizeOptions,
  ColumnDefinition,
  TableOptions,
  QueryConfig,
} from './interfaces';
import { CrateDBError, DeserializationError, RequestError } from './utils/Error.js';
import { StatementGenerator } from './StatementGenerator.js';
import zlib from 'zlib';
import { promisify } from 'util';

// Configuration options with CrateDB-specific environment variables
const defaultConfig: CrateDBConfig = {
  user: process.env.CRATEDB_USER || 'crate',
  password: process.env.CRATEDB_PASSWORD || '',
  jwt: null, // JWT token for Bearer authentication
  host: process.env.CRATEDB_HOST || 'localhost',
  port: process.env.CRATEDB_PORT ? parseInt(process.env.CRATEDB_PORT, 10) : 4200, // Default CrateDB port
  connectionString: null,
  ssl: false,
  defaultSchema: process.env.CRATEDB_DEFAULT_SCHEMA || null, // Default schema for queries
  keepAlive: true, // Enable persistent connections by default
  maxConnections: 20,
  deserialization: {
    long: 'number',
    timestamp: 'date',
    date: 'date',
  },
  rowMode: 'array',
  enableCompression: true,
  compressionThreshold: 1024, // Default to 1KB
};

export class CrateDBClient {
  private cfg: CrateDBConfig;
  private httpAgent: http.Agent | https.Agent;
  private protocol: 'http' | 'https';
  private httpOptions: http.RequestOptions;

  constructor(config = {}) {
    const cfg: CrateDBConfig = { ...defaultConfig, ...config };

    // Parse connection string if provided
    if (cfg.connectionString) {
      const parsed = new URL(cfg.connectionString);
      cfg.user = cfg.user || parsed.username;
      cfg.password = cfg.password || parsed.password;
      cfg.host = parsed.hostname;
      cfg.port = cfg.port || parseInt(parsed.port, 10);
      cfg.ssl = cfg.ssl || parsed.protocol === 'https:';
    }

    // Set up HTTP(S) agent options based on configuration
    const agentOptions: AgentOptions = {
      keepAlive: cfg.keepAlive,
      maxSockets: cfg.maxConnections,
      maxFreeSockets: cfg.maxConnections,
      scheduling: 'fifo',
    };

    this.cfg = cfg;
    this.httpAgent = cfg.ssl ? new https.Agent(agentOptions) : new http.Agent(agentOptions);
    this.protocol = cfg.ssl ? 'https' : 'http';

    // Determine authentication header: use JWT if available, else basic auth
    let authHeader = {};
    if (cfg.jwt) {
      authHeader = { Authorization: `Bearer ${cfg.jwt}` };
    } else if (cfg.user && cfg.password) {
      authHeader = {
        Authorization: `Basic ${Buffer.from(`${cfg.user}:${cfg.password}`).toString('base64')}`,
      };
    }

    this.httpOptions = {
      hostname: cfg.host,
      port: cfg.port,
      path: '/_sql?types',
      method: 'POST',
      headers: {
        Connection: 'keep-alive',
        'Content-Type': 'application/json',
        Accept: 'application/json',
        ...authHeader,
        ...(cfg.defaultSchema ? { 'Default-Schema': cfg.defaultSchema } : {}),
      },
      auth: cfg.jwt ? undefined : cfg.user && cfg.password ? `${cfg.user}:${cfg.password}` : undefined,
      agent: this.httpAgent,
    };
  }

  createCursor(sql: string): Cursor {
    return new Cursor(this, sql);
  }

  async *streamQuery(sql: string, batchSize: number = 100): AsyncGenerator<CrateDBRecord, void, unknown> {
    const cursor = this.createCursor(sql);

    try {
      await cursor.open();
      yield* cursor.iterate(batchSize);
    } finally {
      await cursor.close();
    }
  }

  async execute(stmt: string, args?: unknown[], config?: QueryConfig): Promise<CrateDBResponse> {
    const startRequestTime = Date.now();
    const payload = args ? { stmt, args } : { stmt };
    let body: string;
    try {
      body = Serializer.serialize(payload);
    } catch (serializationError: unknown) {
      const msg = serializationError instanceof Error ? serializationError.message : String(serializationError);
      throw new RequestError(`Serialization failed: ${msg}`);
    }

    const options = {
      ...this.httpOptions,
      ...config?.httpOptions,
      body,
    };

    try {
      const response = await this._makeRequest(options);
      const transformedResponse = this._transformResponse(response, config?.rowMode ?? this.cfg.rowMode);
      return this._addDurations(startRequestTime, transformedResponse) as CrateDBResponse;
    } catch (error: unknown) {
      if (error instanceof CrateDBError || error instanceof DeserializationError) {
        throw error;
      } else if (error instanceof Error) {
        throw new RequestError(`CrateDB request failed: ${error.message}`, { cause: error });
      }
      throw new RequestError('CrateDB request failed with an unknown error');
    }
  }

  async executeMany(stmt: string, bulk_args: unknown[][]): Promise<CrateDBBulkResponse> {
    const startRequestTime = Date.now();
    let body: string;
    try {
      body = Serializer.serialize({ stmt, bulk_args });
    } catch (serializationError: unknown) {
      const msg = serializationError instanceof Error ? serializationError.message : String(serializationError);
      throw new RequestError(`Serialization failed: ${msg}`);
    }

    const options = { ...this.httpOptions, body };

    try {
      const response = await this._makeRequest(options);
      const res = this._addDurations(startRequestTime, response) as CrateDBBulkResponse;
      // Mark bulk errors for each result where rowcount is -2
      res.bulk_errors = (res.results || [])
        .map((result, i) => (result.rowcount === -2 ? i : null))
        .filter((i) => i !== null);
      return res;
    } catch (error: unknown) {
      if (error instanceof CrateDBError || error instanceof DeserializationError) {
        throw error;
      } else if (error instanceof Error) {
        throw new RequestError(`CrateDB bulk request failed: ${error.message}`, { cause: error });
      }
      throw new RequestError('CrateDB bulk request failed with an unknown error');
    }
  }

  /**
   * Inserts a single row into a specified table with optional primary key conflict resolution.
   *
   * If primaryKeys are provided, conflicts will be handled by updating the non-primary key
   * fields of the conflicting row. If no primaryKeys are provided, conflicting rows will be skipped.
   *
   * @param {string} tableName - The name of the table to insert the row into.
   * @param {Record<string, unknown>} obj - An object representing the row to insert.
   * @param {string[] | null} [primaryKeys=null] - Array of column names to use as primary keys for conflict resolution.
   * @returns {Promise<CrateDBResponse>} A promise resolving to the response from CrateDB.
   * @throws {Error} If the input parameters are invalid.
   */
  async insert(
    tableName: string,
    obj: Record<string, unknown>,
    primaryKeys: string[] | null = null
  ): Promise<CrateDBResponse> {
    // Validate inputs
    if (!tableName || typeof tableName !== 'string') {
      throw new Error('tableName must be a valid string');
    }
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
      throw new Error('obj must be a valid non-array object');
    }
    if (primaryKeys && !Array.isArray(primaryKeys)) {
      throw new Error('primaryKeys must be an array or null');
    }

    const keys = Object.keys(obj);
    const query = StatementGenerator.insert(tableName, keys, primaryKeys);
    const args = Object.values(obj);

    // Execute the query
    return this.execute(query, args);
  }

  /**
   * Inserts multiple rows into a table with optional primary key conflict resolution.
   *
   * If primaryKeys are provided, conflicts will be handled by updating the non-primary key
   * fields of conflicting rows. If no primaryKeys are provided, conflicting rows will be skipped.
   * The method automatically handles varying column sets across the input objects.
   *
   * @param {string} tableName - The name of the table to insert rows into.
   * @param {Record<string, unknown>[]} objectArray - Array of objects representing rows to insert.
   * @param {string[] | null} [primaryKeys=null] - Array of column names to use as primary keys for conflict resolution.
   * @returns {Promise<CrateDBBulkResponse>} A promise resolving to the bulk operation response from CrateDB.
   * @throws {Error} If the input parameters are invalid or if the operation fails.
   */
  async insertMany(
    tableName: string,
    objectArray: Record<string, unknown>[],
    primaryKeys: string[] | null = null
  ): Promise<CrateDBBulkResponse> {
    const startInsertMany = Date.now();
    // Validate inputs
    if (!tableName || typeof tableName !== 'string') {
      throw new Error('tableName must be a valid string.');
    }
    if (!Array.isArray(objectArray) || objectArray.length === 0) {
      throw new Error('insertMany requires a non-empty array of objects.');
    }
    if (primaryKeys && !Array.isArray(primaryKeys)) {
      throw new Error('primaryKeys must be an array or null.');
    }

    // Extract unique keys from all objects
    const uniqueKeys = Array.from(
      objectArray.reduce((keys: Set<string>, obj: Record<string, unknown>) => {
        Object.keys(obj).forEach((key) => keys.add(key));
        return keys;
      }, new Set<string>())
    );

    // Generate bulk arguments
    const bulkArgs = objectArray.map((obj) =>
      uniqueKeys.map((key) => (Object.prototype.hasOwnProperty.call(obj, key) ? obj[key] : null))
    );

    const query = StatementGenerator.insert(tableName, uniqueKeys, primaryKeys);

    // Execute the query with bulk arguments
    const response = await this.executeMany(query, bulkArgs);
    const elapsedTime = Date.now() - startInsertMany;
    response.durations.preparation = elapsedTime - response.durations.request - (response.durations.cratedb ?? 0);

    return response;
  }

  /**
   * Drops a table if it exists in CrateDB.
   *
   * Constructs and executes a `DROP TABLE IF EXISTS` SQL statement.
   *
   * @param {string} tableName - The name of the table to drop.
   * @returns {Promise<CrateDBResponse>} A promise resolving to the response from CrateDB.
   */
  async drop(tableName: string): Promise<CrateDBResponse> {
    const query = StatementGenerator.dropTable(tableName);
    return this.execute(query);
  }

  async createTable(
    tableName: string,
    schema: Record<string, ColumnDefinition>,
    options?: TableOptions
  ): Promise<CrateDBResponse> {
    const query = StatementGenerator.createTable(tableName, schema, options);
    return this.execute(query);
  }

  /**
   * Refreshes a given table by refreshing it in CrateDB.
   *
   * The `REFRESH TABLE` command makes recently committed changes available for querying
   * without waiting for automatic refresh intervals.
   *
   * @param {string} tableName - The name of the table to refresh.
   * @returns {Promise<CrateDBResponse>} A promise resolving to the response from CrateDB.
   */
  async refresh(tableName: string): Promise<CrateDBResponse> {
    const query = StatementGenerator.refresh(tableName);
    return this.execute(query);
  }

  /**
   * Optimizes a given table or specific partitions in CrateDB by merging table segments.
   *
   * The `OPTIMIZE TABLE` command reduces the number of segments in a table, improving
   * query performance and reducing storage overhead. It supports optimizing the entire table
   * or specific partitions and allows additional optimization parameters.
   *
   * @param {string} tableName - The name of the table to optimize.
   * @param {OptimizeOptions} [options] - Optional parameters for table optimization.
   * @param {Record<string, string | number>} [partitions] - Optional key-value pairs specifying partition columns and values.
   * @returns {Promise<CrateDBResponse>} A promise resolving to the response from CrateDB.
   */
  async optimize(
    tableName: string,
    options?: OptimizeOptions,
    partitions?: Record<string, string | number>
  ): Promise<CrateDBResponse> {
    const query = StatementGenerator.optimize(tableName, options, partitions);
    return this.execute(query);
  }

  /**
   * Retrieves the primary key columns for a given table.
   *
   * Queries the information_schema to get the primary key columns
   * of the specified table in their defined order.
   *
   * @param {string} tableName - The name of the table to get primary keys for.
   * @returns {Promise<string[]>} A promise resolving to an array of primary key column names.
   * @throws {Error} If the table doesn't exist or if there's an error retrieving the information.
   */
  async getPrimaryKeys(tableName: string): Promise<string[]> {
    if (!tableName || typeof tableName !== 'string') {
      throw new Error('tableName must be a valid string');
    }

    // Split schema and table name
    const [schema = 'doc', table] = tableName.split('.');
    const actualTable = table || schema;
    const actualSchema = table ? schema : 'doc';

    const query = StatementGenerator.getPrimaryKeys();
    const response = await this.execute(query, [actualSchema, actualTable]);

    if (!response.rows || response.rows.length === 0) {
      return [];
    }

    return response.rows.map((row) => row[0] as string);
  }

  private _addDurations(startRequestTime: number, response: CrateDBBaseResponse): CrateDBBaseResponse {
    const totalRequestTime = Date.now() - startRequestTime;
    if (typeof response.duration === 'number') {
      response.durations = {
        cratedb: response.duration,
        request: totalRequestTime - response.duration,
      };
    } else {
      response.durations = {
        cratedb: 0,
        request: totalRequestTime,
      };
    }
    return response;
  }

  async _makeRequest(options: http.RequestOptions & { body?: string | Buffer }): Promise<CrateDBBaseResponse> {
    return new Promise((resolve, reject) => {
      try {
        let requestBody = options.body;
        const headers = { ...options.headers };
        const requestBodySize = requestBody ? Buffer.byteLength(requestBody) : 0;
        let compressedSize = requestBodySize;

        if (this.cfg.enableCompression && requestBody && requestBodySize > (this.cfg.compressionThreshold ?? 1024)) {
          promisify(zlib.gzip)(requestBody)
            .then((compressed) => {
              requestBody = compressed;
              compressedSize = Buffer.byteLength(requestBody);
              headers['Content-Encoding'] = 'gzip';

              const req = (this.protocol === 'https' ? https : http).request({ ...options, headers }, (response) => {
                const data: Buffer[] = [];
                response.on('data', (chunk: Buffer) => data.push(chunk));
                response.on('end', () => {
                  const rawResponse = Buffer.concat(data);
                  const responseBodySize = rawResponse.length;

                  try {
                    const parsedResponse = Serializer.deserialize(rawResponse.toString(), this.cfg.deserialization);

                    if (response.statusCode === 200) {
                      resolve({
                        ...parsedResponse,
                        sizes: {
                          response: responseBodySize,
                          request: compressedSize,
                          requestUncompressed: requestBodySize,
                        },
                      });
                    } else {
                      reject(CrateDBError.fromResponse(parsedResponse as CrateDBErrorResponse, response.statusCode));
                    }
                  } catch (parseErr: unknown) {
                    if (parseErr instanceof Error) {
                      reject(
                        new Error(
                          `Failed to parse response: ${parseErr.message}. Raw response: ${rawResponse.toString()}`
                        )
                      );
                    } else {
                      reject(new Error(`Failed to parse response. Raw response: ${rawResponse.toString()}`));
                    }
                  }
                });
              });

              req.on('error', (err) => reject(new Error(`Request failed: ${err.message}`)));
              req.end(requestBody);
            })
            .catch((error) => reject(error));
        } else {
          const req = (this.protocol === 'https' ? https : http).request({ ...options, headers }, (response) => {
            const data: Buffer[] = [];
            response.on('data', (chunk: Buffer) => data.push(chunk));
            response.on('end', () => {
              const rawResponse = Buffer.concat(data);
              const responseBodySize = rawResponse.length;

              try {
                const parsedResponse = Serializer.deserialize(rawResponse.toString(), this.cfg.deserialization);

                if (response.statusCode === 200) {
                  resolve({
                    ...parsedResponse,
                    sizes: {
                      response: responseBodySize,
                      request: compressedSize,
                      requestUncompressed: requestBodySize,
                    },
                  });
                } else {
                  reject(CrateDBError.fromResponse(parsedResponse as CrateDBErrorResponse, response.statusCode));
                }
              } catch (parseErr: unknown) {
                if (parseErr instanceof Error) {
                  reject(
                    new Error(`Failed to parse response: ${parseErr.message}. Raw response: ${rawResponse.toString()}`)
                  );
                } else {
                  reject(new Error(`Failed to parse response. Raw response: ${rawResponse.toString()}`));
                }
              }
            });
          });

          req.on('error', (err) => reject(new Error(`Request failed: ${err.message}`)));
          req.end(requestBody);
        }
      } catch (error) {
        reject(error);
      }
    });
  }

  protected _transformResponse(
    response: CrateDBBaseResponse,
    rowMode: 'array' | 'object' = 'object'
  ): CrateDBBaseResponse {
    // Return early if not transforming to object mode
    if (rowMode !== 'object') {
      return response;
    }

    // Create a shallow copy of the response
    const transformedResponse = { ...response };

    // Only transform if we have both rows and column names
    if (Array.isArray(transformedResponse.rows) && Array.isArray(transformedResponse.cols)) {
      transformedResponse.rows = transformedResponse.rows.map((row) => {
        // Skip transformation if row is null or not an array
        if (!Array.isArray(row)) {
          return row;
        }

        const obj: Record<string, unknown> = {};
        transformedResponse.cols?.forEach((col, index) => {
          // Only set property if column name is a string
          if (typeof col === 'string') {
            // Preserve null/undefined values
            obj[col] = row[index];
          }
        });
        return obj;
      });
    }

    return transformedResponse;
  }

  public getConfig(): Readonly<CrateDBConfig> {
    return this.cfg;
  }
  public getHttpOptions(): Readonly<http.RequestOptions> {
    return this.httpOptions;
  }
}
