'use strict';

import http from 'http';
import https from 'https';

export class CrateDBCursor {
  constructor(client, sql) {
    this.client = client; // Reference to the CrateDBClient instance
    this.sql = sql; // The SQL statement for the cursor
    this.cursorName = `cursor_${Date.now()}`; // Unique cursor name
    this.isOpen = false; // Cursor state

    const agentOptions = {
      keepAlive: true,
      maxSockets: 1
    };
    
    // Create a new agent with its own socket for this cursor
    this.agent = client.cfg.ssl ? new https.Agent(agentOptions) : new http.Agent(agentOptions);
  
    this.connectionOptions = { ...client.httpOptions, agent: this.agent };
  }

  async open() {
    if (this.isOpen) {
      throw new Error('Cursor is already open');
    }

    // Start a transaction and declare the cursor
    await this._execute('BEGIN');
    await this._execute(`DECLARE ${this.cursorName} NO SCROLL CURSOR FOR ${this.sql}`);
    this.isOpen = true;
  }

  async fetchone() {
    this._ensureOpen();
    const result = await this._execute(`FETCH NEXT FROM ${this.cursorName}`);
    return result ? result[0] : result; // Return the first row or null
  }

  async fetchmany(size = 10) {
    if(size < 1) {  // Return an empty array if size is less than 1
      return [];
    }
    this._ensureOpen();
    return await this._execute(`FETCH ${size} FROM ${this.cursorName}`);
  }

  async fetchall() {
    this._ensureOpen();
    return await this._execute(`FETCH ALL FROM ${this.cursorName}`);
  }

  async *iterate(size = 100) {
    this._ensureOpen();
  
    while (true) {
      const rows = await this.fetchmany(size);
  
      if (!rows || rows.length === 0) {
        break; // Stop iteration when no more rows are returned
      }
  
      for (const row of rows) {
        yield row; // Yield one row at a time
      }
    }
  }

  async close() {
    this._ensureOpen();

    // Close the cursor and end the transaction
    await this._execute(`CLOSE ${this.cursorName}`);
    await this._execute('COMMIT');
    this.isOpen = false;

    // Destroy the agent's socket connections to ensure the TCP connection is closed
    this.agent.destroy();
  }

  async _execute(sql) {
    const options = { ...this.connectionOptions, body: JSON.stringify({ stmt: sql }) };
    try {
      const response = await this.client._makeRequest(options, this.client.protocol);
      const { cols, rows, rowcount } = JSON.parse(response);
      return rowcount > 0 ? this._rebuildObjects(cols, rows) : null;
    } catch (error) {
      throw new Error(`Error executing SQL: ${sql}. Details: ${error.message}`);
    }
  }

  _rebuildObjects(cols, rows) {
    return rows.map((row) => {
      const obj = {};
      cols.forEach((col, index) => {
        obj[col] = row[index];
      });
      return obj;
    });
  }

  _ensureOpen() {
    if (!this.isOpen) {
      throw new Error('Cursor is not open. Call open() before performing this operation.');
    }
  }
}