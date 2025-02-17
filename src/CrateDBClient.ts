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
} from './interfaces';
import { CrateDBError, DeserializationError, RequestError } from './utils/Error.js';

// Configuration options with CrateDB-specific environment variables
const defaultConfig: CrateDBConfig = {
  user: process.env.CRATEDB_USER || 'crate',
  password: process.env.CRATEDB_PASSWORD || '',
  jwt: null, // JWT token for Bearer authentication
  host: process.env.CRATEDB_HOST || 'localhost',
  port: process.env.CRATEDB_PORT ? parseInt(process.env.CRATEDB_PORT, 10) : 4200, // Default CrateDB port
  defaultSchema: process.env.CRATEDB_DEFAULT_SCHEMA || null, // Default schema for queries
  connectionString: null,
  ssl: false,
  keepAlive: true, // Enable persistent connections by default
  maxConnections: 20,
  deserialization: {
    long: 'bigint',
    timestamp: 'date',
    date: 'date',
  },
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

  async execute(stmt: string, args?: unknown[]): Promise<CrateDBResponse> {
    const startRequestTime = Date.now();
    const payload = args ? { stmt, args } : { stmt };
    let body: string;
    try {
      body = Serializer.serialize(payload);
    } catch (serializationError: unknown) {
      const msg = serializationError instanceof Error ? serializationError.message : String(serializationError);
      throw new RequestError(`Serialization failed: ${msg}`);
    }

    const options = { ...this.httpOptions, body };

    try {
      const response = await this._makeRequest(options);
      return this._addDurations(startRequestTime, response) as CrateDBResponse;
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

  // Convenience methods for common SQL operations
  private _generateInsertQuery(tableName: string, keys: string[], primaryKeys: string[] | null): string {
    const placeholders = keys.map(() => '?').join(', ');
    let query = `INSERT INTO ${tableName} (${keys.map((key) => `"${key}"`).join(', ')}) VALUES (${placeholders})`;

    if (primaryKeys && primaryKeys.length > 0) {
      const keysWithoutPrimary = keys.filter((key) => !primaryKeys.includes(key));
      const updates = keysWithoutPrimary.map((key) => `"${key}" = excluded."${key}"`).join(', ');
      query += ` ON CONFLICT (${primaryKeys.map((key) => `"${key}"`).join(', ')}) DO UPDATE SET ${updates}`;
    } else {
      query += ' ON CONFLICT DO NOTHING';
    }

    query += ';'; // Ensure the query ends with a semicolon
    return query;
  }

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
    const query = this._generateInsertQuery(tableName, keys, primaryKeys);
    const args = Object.values(obj);

    // Execute the query
    return this.execute(query, args);
  }

  async insertMany(
    tableName: string,
    jsonArray: Record<string, unknown>[],
    primaryKeys: string[] | null = null
  ): Promise<CrateDBBulkResponse> {
    const startInsertMany = Date.now();
    // Validate inputs
    if (!tableName || typeof tableName !== 'string') {
      throw new Error('tableName must be a valid string.');
    }
    if (!Array.isArray(jsonArray) || jsonArray.length === 0) {
      throw new Error('insertMany requires a non-empty array of objects.');
    }
    if (primaryKeys && !Array.isArray(primaryKeys)) {
      throw new Error('primaryKeys must be an array or null.');
    }

    // Extract unique keys from all objects
    const uniqueKeys = Array.from(
      jsonArray.reduce((keys: Set<string>, obj: Record<string, unknown>) => {
        Object.keys(obj).forEach((key) => keys.add(key));
        return keys;
      }, new Set<string>())
    );

    // Generate bulk arguments
    const bulkArgs = jsonArray.map((obj) =>
      uniqueKeys.map((key) => (Object.prototype.hasOwnProperty.call(obj, key) ? obj[key] : null))
    );

    const query = this._generateInsertQuery(tableName, uniqueKeys, primaryKeys);

    // Execute the query with bulk arguments
    const response = await this.executeMany(query, bulkArgs);
    const elapsedTime = Date.now() - startInsertMany;
    response.durations.preparation = elapsedTime - response.durations.request - (response.durations.cratedb ?? 0);

    return response;
  }

  async update(tableName: string, options: Record<string, unknown>, whereClause: string): Promise<CrateDBResponse> {
    const { keys, values, args } = this._prepareOptions(options);
    const setClause = keys.map((key, i) => `${key}=${values[i]}`).join(', ');
    const query = `UPDATE ${tableName} SET ${setClause} WHERE ${whereClause}`;
    return this.execute(query, args);
  }

  async delete(tableName: string, whereClause: string): Promise<CrateDBResponse> {
    const query = `DELETE FROM ${tableName} WHERE ${whereClause}`;
    return this.execute(query);
  }

  async drop(tableName: string): Promise<CrateDBResponse> {
    const query = `DROP TABLE IF EXISTS ${tableName}`;
    return this.execute(query);
  }

  async refresh(tableName: string): Promise<CrateDBResponse> {
    const query = `REFRESH TABLE ${tableName}`;
    return this.execute(query);
  }

  async createTable(schema: Record<string, Record<string, string>>): Promise<CrateDBResponse> {
    const tableName = Object.keys(schema)[0];
    const columns = Object.entries(schema[tableName])
      .map(([col, type]) => `"${col}" ${type}`)
      .join(', ');
    const query = `CREATE TABLE ${tableName} (${columns})`;
    return this.execute(query);
  }

  private _prepareOptions(options: Record<string, unknown>): {
    keys: string[];
    values: string[];
    args: unknown[];
  } {
    const keys = Object.keys(options).map((key) => `"${key}"`);
    const values = keys.map(() => '?');
    const args = Object.values(options);
    return { keys, values, args };
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

  async _makeRequest(options: http.RequestOptions & { body?: string }): Promise<CrateDBBaseResponse> {
    return new Promise((resolve, reject) => {
      const requestBodySize = options.body ? Buffer.byteLength(options.body) : 0;
      const req = (this.protocol === 'https' ? https : http).request(options, (response) => {
        const data: Buffer[] = [];
        response.on('data', (chunk: Buffer) => data.push(chunk));
        response.on('end', () => {
          const rawResponse = Buffer.concat(data); // Raw response data as a buffer
          const responseBodySize = rawResponse.length;

          try {
            const parsedResponse = Serializer.deserialize(rawResponse.toString(), this.cfg.deserialization);
            if (response.statusCode === 200) {
              resolve({
                ...parsedResponse,
                sizes: { response: responseBodySize, request: requestBodySize },
              });
            } else {
              reject(CrateDBError.fromResponse(parsedResponse as CrateDBErrorResponse, response.statusCode));
            }
          } catch (parseErr: unknown) {
            // Handle parsing errors
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
      req.end(options.body || null);
    });
  }

  public getConfig(): Readonly<CrateDBConfig> {
    return this.cfg;
  }
  public getHttpOptions(): Readonly<http.RequestOptions> {
    return this.httpOptions;
  }
}
