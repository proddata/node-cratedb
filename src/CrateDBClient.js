'use strict';

import http from 'http';
import https from 'https';
import { URL } from 'url';
import { CrateDBCursor } from './CrateDBCursor.js';

// Configuration options with CrateDB-specific environment variables
const defaultConfig = {
  user: process.env.CRATEDB_USER || 'crate',
  password: process.env.CRATEDB_PASSWORD || '',
  host: process.env.CRATEDB_HOST || 'localhost',
  port: process.env.CRATEDB_PORT ? parseInt(process.env.CRATEDB_PORT, 10) : 4200, // Default CrateDB port
  defaultSchema: process.env.CRATEDB_HOST || 'doc', // Default schema for queries
  connectionString: null,
  ssl: false,
  keepAlive: true, // Enable persistent connections by default
  maxConnections: 20,
};

class CrateDBClient {
  constructor(config = {}) {
    const cfg = { ...defaultConfig, ...config };

    // Parse connection string if provided
    if (cfg.connectionString) {
      const parsed = new URL(cfg.connectionString);
      cfg.user = cfg.user || parsed.username;
      cfg.password = cfg.password || parsed.password;
      cfg.host = parsed.hostname;
      cfg.port = cfg.port || parsed.port;
      cfg.ssl = cfg.ssl || parsed.protocol === 'https:';
    }

    // Set up HTTP(S) agent options based on configuration
    const agentOptions = {
      keepAlive: cfg.keepAlive,
      maxSockets: cfg.maxConnections,
      maxFreeSockets: cfg.maxConnections,
      scheduling: 'fifo',
    };

    this.cfg = cfg;
    this.httpAgent = cfg.ssl ? new https.Agent(agentOptions) : new http.Agent(agentOptions);
    this.protocol = cfg.ssl ? 'https' : 'http';

    this.httpOptions = {
      hostname: cfg.host,
      port: cfg.port,
      path: '/_sql?types',
      method: 'POST',
      headers: {
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        ...(cfg.user && cfg.password
          ? { Authorization: `Basic ${Buffer.from(`${cfg.user}:${cfg.password}`).toString('base64')}` }
          : {}),
        ...(cfg.defaultSchema ? { 'Default-Schema': cfg.defaultSchema } : {}),
      },
      auth: cfg.user && cfg.password ? `${cfg.user}:${cfg.password}` : undefined,
      agent: this.httpAgent
    };

  }

  createCursor(sql) {
    return new CrateDBCursor(this, sql);
  }

  async *streamQuery(sql, batchSize = 100) {
    const cursor = this.createCursor(sql);
  
    try {
      await cursor.open();
      yield* cursor.iterate(batchSize);
    } finally {
      await cursor.close();
    }
  }

  async execute(stmt, args = []) {
    return this._execute(stmt, args);
  }

  async executeMany(stmt, bulk_args = []) {
    return this._execute(stmt, null, bulk_args);
  }

  async _execute(stmt, args = null, bulk_args = null) {
    const startRequestTime = Date.now();
  
    const body = JSON.stringify(args ? { stmt, args } : { stmt, bulk_args });

    const options = { ...this.httpOptions, body };
    const response = await this._makeRequest(options, this.protocol);
  
    const totalRequestTime = Date.now() - startRequestTime;
    response.durations = {
      cratedb: response.duration,
      request: totalRequestTime - response.duration,
    };
  
    return response;
  }

  // Convenience methods for common SQL operations

  _generateInsertQuery(tableName, keys, primaryKeys) {
    const placeholders = keys.map(() => "?").join(", ");
    let query = `INSERT INTO ${tableName} (${keys.map((key) => `"${key}"`).join(", ")}) VALUES (${placeholders})`;

    if (primaryKeys && primaryKeys.length > 0) {
      const keysWithoutPrimary = keys.filter((key) => !primaryKeys.includes(key));
      const updates = keysWithoutPrimary.map((key) => `"${key}" = excluded."${key}"`).join(", ");
      query += ` ON CONFLICT (${primaryKeys.map((key) => `"${key}"`).join(", ")}) DO UPDATE SET ${updates}`;
    } else {
      query += " ON CONFLICT DO NOTHING";
    }

    query += ";"; // Ensure the query ends with a semicolon
    return query;
  }

  async insert(tableName, obj, primaryKeys = null) {
    // Validate inputs
    if (!tableName || typeof tableName !== "string") {
      throw new Error("tableName must be a valid string");
    }
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
      throw new Error("obj must be a valid non-array object");
    }
    if (primaryKeys && !Array.isArray(primaryKeys)) {
      throw new Error("primaryKeys must be an array or null");
    }
    
    const keys = Object.keys(obj);
    let query = this._generateInsertQuery(tableName, keys, primaryKeys);
    const args = Object.values(obj);

    // Execute the query
    return await this.execute(query, args);
  }

  async insertMany(tableName, jsonArray, primaryKeys = null) {
    const startInsertMany = Date.now();
    // Validate inputs
    if (!tableName || typeof tableName !== "string") {
      throw new Error("tableName must be a valid string.");
    }
    if (!Array.isArray(jsonArray) || jsonArray.length === 0) {
      throw new Error("insertMany requires a non-empty array of objects.");
    }
    if (primaryKeys && !Array.isArray(primaryKeys)) {
      throw new Error("primaryKeys must be an array or null.");
    }
  
    // Extract unique keys from all objects
    const uniqueKeys = Array.from(
      jsonArray.reduce((keys, obj) => {
        Object.keys(obj).forEach((key) => keys.add(key));
        return keys;
      }, new Set())
    );
  
    // Generate bulk arguments
    const bulkArgs = jsonArray.map((obj) =>
      uniqueKeys.map((key) => (obj.hasOwnProperty(key) ? obj[key] : null))
    );

    const query = this._generateInsertQuery(tableName, uniqueKeys, primaryKeys);
  
    // Execute the query with bulk arguments
    const response = await this.executeMany(query, bulkArgs);
    const elapsedTime = Date.now() - startInsertMany;
    response.durations.preparation = elapsedTime - response.durations.request - response.durations.cratedb;

    return response;
  }

  async update(tableName, options, whereClause) {
    const { keys, values, args } = this._prepareOptions(options);
    const setClause = keys.map((key, i) => `${key}=${values[i]}`).join(', ');
    const query = `UPDATE ${tableName} SET ${setClause} WHERE ${whereClause}`;
    return await this.execute(query, args);
  }

  async delete(tableName, whereClause) {
    const query = `DELETE FROM ${tableName} WHERE ${whereClause}`;
    return await this.execute(query);
  }

  async drop(tableName) {
    const query = `DROP TABLE IF EXISTS ${tableName}`;
    return await this.execute(query);
  }

  async refresh(tableName) {
    const query = `REFRESH TABLE ${tableName}`;
    return await this.execute(query);
  }

  async createTable(schema) {
    const tableName = Object.keys(schema)[0];
    const columns = Object.entries(schema[tableName])
      .map(([col, type]) => `"${col}" ${type}`)
      .join(', ');
    const query = `CREATE TABLE ${tableName} (${columns})`;
    return await this.execute(query);
  }

  _prepareOptions(options) {
    const keys = Object.keys(options).map(key => `"${key}"`);
    const values = keys.map(() => '?');
    const args = Object.values(options);
    return { keys, values, args };
  }

  async _makeRequest(options) {
    return new Promise((resolve, reject) => {
      const requestBodySize = options.body ? Buffer.byteLength(options.body) : 0;
      const req = (this.protocol === 'https' ? https : http).request(options, (response) => {
        let data = [];
        response.on('data', (chunk) => data.push(chunk));
        response.on('end', () => {
          const rawResponse = Buffer.concat(data); // Raw response data as a buffer
          const responseBodySize = rawResponse.length;
          resolve({
            ...(JSON.parse(rawResponse.toString())),
            sizes: {response: responseBodySize, request: requestBodySize}
          });
        });
      });
      req.on('error', (err) => reject(new Error(`Request failed: ${err.message}`)));
      req.end(options.body || null);
    });
  }
}

// Export CrateDBClient class
export { CrateDBClient };

// Usage example
// import { CrateDBClient } from './CrateDBClient.js';
// const client = new CrateDBClient({
//   user: 'database-user',
//   password: 'secretpassword!!',
//   host: 'my.database-server.com',
//   port: 5334,
//   ssl: true,             // Use HTTPS
//   keepAlive: true,       // Enable persistent connections
//   maxConnections: 20,         // Limit to 10 concurrent sockets
//   defaultSchema: 'my_schema' // Default schema for queries
// });