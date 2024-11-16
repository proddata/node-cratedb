'use strict';

import http from 'http';
import https from 'https';
import { URL } from 'url';

// Configuration options with CrateDB-specific environment variables
const defaultConfig = {
  user: process.env.CRATEDB_USER || 'crate',
  password: process.env.CRATEDB_PASSWORD || '',
  host: process.env.CRATEDB_HOST || 'localhost',
  port: process.env.CRATEDB_PORT ? parseInt(process.env.CRATEDB_PORT, 10) : 4200, // Default CrateDB port
  connectionString: null,
  ssl: false,
  keepAlive: true, // Enable persistent connections by default
  maxSockets: Infinity // Default to unlimited sockets
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
      maxSockets: cfg.maxSockets
    };

    this.httpAgent = cfg.ssl ? new https.Agent(agentOptions) : new http.Agent(agentOptions);

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
      },
      auth: cfg.user && cfg.password ? `${cfg.user}:${cfg.password}` : undefined,
      agent: this.httpAgent
    };

    this.protocol = cfg.ssl ? 'https' : 'http';

  }

  async executeSql(sql, args = []) {
    const options = { ...this.httpOptions, body: JSON.stringify({ stmt: sql, args }) };
    const response = await this._makeRequest(options, this.protocol);
    return JSON.parse(response);
  }

  async insert(tableName, options) {
    const { keys, values, args } = this._prepareOptions(options);
    const query = `INSERT INTO ${tableName} (${keys}) VALUES (${values})`;
    return await this.executeSql(query, args);
  }

  async update(tableName, options, whereClause) {
    const { keys, values, args } = this._prepareOptions(options);
    const setClause = keys.map((key, i) => `${key}=${values[i]}`).join(', ');
    const query = `UPDATE ${tableName} SET ${setClause} WHERE ${whereClause}`;
    return await this.executeSql(query, args);
  }

  async delete(tableName, whereClause) {
    const query = `DELETE FROM ${tableName} WHERE ${whereClause}`;
    return await this.executeSql(query);
  }

  async drop(tableName) {
    const query = `DROP TABLE ${tableName}`;
    return await this.executeSql(query);
  }

  async createTable(schema) {
    const tableName = Object.keys(schema)[0];
    const columns = Object.entries(schema[tableName])
      .map(([col, type]) => `"${col}" ${type}`)
      .join(', ');
    const query = `CREATE TABLE ${tableName} (${columns})`;
    return await this.executeSql(query);
  }

  _prepareOptions(options) {
    const keys = Object.keys(options).map(key => `"${key}"`);
    const values = keys.map(() => '?');
    const args = Object.values(options);
    return { keys, values, args };
  }

  async _makeRequest(options, protocol) {
    return new Promise((resolve, reject) => {
      const req = (protocol === 'https' ? https : http).request(options, (response) => {
        let data = [];
        response.on('data', chunk => data.push(chunk));
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
//   maxSockets: 10         // Limit to 10 concurrent sockets
// });