'use strict';

export class CrateDBCursor {
  constructor(client, sql) {
    this.client = client; // Reference to the CrateDBClient instance
    this.sql = sql; // The SQL statement for the cursor
    this.cursorName = `cursor_${Date.now()}`; // Unique cursor name
    this.isOpen = false; // Cursor state
    
    // Create a new agent with its own socket for this cursor
    this.agent = new client.httpAgent.constructor({
        keepAlive: true,
        maxSockets: 1,
      });
  
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
    return result.length > 0 ? result[0] : null;
  }

  async fetchmany(size = 10) {
    this._ensureOpen();
    return await this._execute(`FETCH ${size} FROM ${this.cursorName}`);
  }

  async fetchall() {
    this._ensureOpen();
    return await this._execute(`FETCH ALL FROM ${this.cursorName}`);
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
      return this._rebuildObjects(JSON.parse(response)) || [];
    } catch (error) {
      throw new Error(`Error executing SQL: ${sql}. Details: ${error.message}`);
    }
  }

  _rebuildObjects(data) {
    const { cols, rows } = data;
    if (!rows || rows.length === 0) {
      return [];
    }
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