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
    const options = { ...this.httpOptions, body: JSON.stringify({ stmt, args }) };
    const response = await this._makeRequest(options, this.protocol);
    return JSON.parse(response);
  }

  async executeMany(stmt, bulk_args = []) {
    const options = { ...this.httpOptions, body: JSON.stringify({ stmt, bulk_args }) };
    const response = await this._makeRequest(options, this.protocol);
    return JSON.parse(response);
  }

  // Convenience methods for common SQL operations

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
  
    const keys = Object.keys(obj).map((key) => `"${key}"`);
    const values = keys.map(() => "?");
    const args = Object.values(obj);
  
    let query = `INSERT INTO ${tableName} (${keys.join(", ")}) VALUES (${values.join(", ")})`;
  
    if (primaryKeys && primaryKeys.length > 0) {
      const quotedPrimaryKeys = primaryKeys.map((key) => `"${key}"`);
      const keysWithoutPrimary = keys.filter((key) => !quotedPrimaryKeys.includes(key));
      const updates = keysWithoutPrimary.map((key) => `${key} = excluded.${key}`).join(", ");
      query += ` ON CONFLICT (${primaryKeys.map((key) => `"${key}"`).join(", ")}) DO UPDATE SET ${updates}`;
    } else {
      query += " ON CONFLICT DO NOTHING";
    }
  
    query += ";"; // Ensure query ends with a semicolon
  
    // Execute the query
    return await this.execute(query, args);
  }

  async insertMany(tableName, jsonArray, primaryKeys = null) {
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
  
    const placeholders = uniqueKeys.map(() => "?").join(", ");
    let query = `INSERT INTO ${tableName} (${uniqueKeys.map((key) => `"${key}"`).join(", ")}) VALUES (${placeholders})`;
  
    if (primaryKeys && primaryKeys.length > 0) {
      // Handle upsert logic with conflict resolution
      const quotedPrimaryKeys = primaryKeys.map((key) => `"${key}"`);
      const keysWithoutPrimary = uniqueKeys.filter((key) => !primaryKeys.includes(key));
      const updates = keysWithoutPrimary.map((key) => `"${key}" = excluded."${key}"`).join(", ");
      query += ` ON CONFLICT (${quotedPrimaryKeys.join(", ")}) DO UPDATE SET ${updates}`;
    } else {
      // Skip rows that cause conflicts
      query += " ON CONFLICT DO NOTHING";
    }
  
    query += ";"; // Ensure the query ends with a semicolon
  
    // Execute the query with bulk arguments
    return await this.executeMany(query, bulkArgs);
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
      const req = (this.protocol === 'https' ? https : http).request(options, (response) => {
        let data = [];
        response.on('data', (chunk) => data.push(chunk));
        response.on('end', () => resolve(Buffer.concat(data).toString()));
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