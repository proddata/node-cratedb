'use strict';

import http from 'http';
import https from 'https';
import { CrateDBClient } from './CrateDBClient.js';
import { CrateDBResponse, CrateDBRecord } from './interfaces';

export class CrateDBCursor {
  public client: CrateDBClient;
  public sql: string;
  public cursorName: string;
  public isOpen: boolean;
  public agent: http.Agent | https.Agent;
  public connectionOptions: http.RequestOptions;

  constructor(client: CrateDBClient, sql: string) {
    this.client = client; // Reference to the CrateDBClient instance
    this.sql = sql; // The SQL statement for the cursor
    this.cursorName = `cursor_${Date.now()}`; // Unique cursor name
    this.isOpen = false; // Cursor state

    const agentOptions = {
      keepAlive: true,
      maxSockets: 1,
    };

    // Create a new agent with its own socket for this cursor
    this.agent = client.cfg.ssl ? new https.Agent(agentOptions) : new http.Agent(agentOptions);
    this.connectionOptions = { ...client.httpOptions, agent: this.agent };
  }

  async open(): Promise<void> {
    if (this.isOpen) {
      throw new Error('Cursor is already open');
    }
    // Start a transaction and declare the cursor
    await this._execute('BEGIN');
    await this._execute(`DECLARE ${this.cursorName} NO SCROLL CURSOR WITH HOLD FOR ${this.sql}`);
    this.isOpen = true;
  }

  async fetchone(): Promise<CrateDBRecord | null> {
    this._ensureOpen();
    const result = await this._execute(`FETCH NEXT FROM ${this.cursorName}`);
    return result.length > 0 ? result[0] : null; // Return the first row or null
  }

  async fetchmany(size = 10): Promise<Array<CrateDBRecord>> {
    if (size < 1) {
      // Return an empty array if size is less than 1
      return [];
    }
    this._ensureOpen();
    return await this._execute(`FETCH ${size} FROM ${this.cursorName}`);
  }

  async fetchall(): Promise<Array<CrateDBRecord>> {
    this._ensureOpen();
    return await this._execute(`FETCH ALL FROM ${this.cursorName}`);
  }

  async *iterate(size = 100): AsyncGenerator<CrateDBRecord, void, unknown> {
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

  async close(): Promise<void> {
    this._ensureOpen();
    // Close the cursor and end the transaction
    await this._execute(`CLOSE ${this.cursorName}`);
    await this._execute('COMMIT');
    this.isOpen = false;
    // Destroy the agent's socket connections to ensure the TCP connection is closed
    this.agent.destroy();
  }

  async _execute(sql: string): Promise<Array<CrateDBRecord>> {
    const options = { ...this.connectionOptions, body: JSON.stringify({ stmt: sql }) };
    try {
      const response: CrateDBResponse = await this.client._makeRequest(options);
      const { cols, rows, rowcount } = response;
      return rowcount && rowcount > 0 ? this._rebuildObjects(cols || [], rows as unknown[]) : [];
    } catch (error: unknown) {
      if (error instanceof Error) {
        throw new Error(`Error executing SQL: ${sql}. Details: ${error.message}`);
      }
      throw error;
    }
  }

  _rebuildObjects(cols: Array<string>, rows: Array<unknown>): Array<CrateDBRecord> {
    return rows.map((row) => {
      const obj: Record<string, unknown> = {};
      cols.forEach((col, index) => {
        obj[col] = (row as unknown[])[index];
      });
      return obj;
    });
  }

  _ensureOpen(): void {
    if (!this.isOpen) {
      throw new Error('Cursor is not open. Call open() before performing this operation.');
    }
  }
}
