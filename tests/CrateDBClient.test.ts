import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import { GenericContainer } from 'testcontainers';
import { CrateDBClient } from '../src/CrateDBClient';
import { CrateDBRecord } from '../src/interfaces';
import { CrateDBError, RequestError } from '../src/utils/Error.js';

describe('CrateDBClient Integration Tests', () => {
  let container;
  let client: CrateDBClient;
  let systemClient: CrateDBClient;

  // Utility: Wait for database readiness
  const waitForDatabase = async (client: CrateDBClient, retries = 20, interval = 1000) => {
    for (let i = 0; i < retries; i++) {
      try {
        await client.execute('SELECT 1');
        return;
      } catch {
        await new Promise((resolve) => setTimeout(resolve, interval));
      }
    }
    throw new Error('Database did not start in time');
  };

  beforeAll(async () => {
    container = await new GenericContainer('crate').withExposedPorts(4200).start();
    const mappedPort = container.getMappedPort(4200);
    const host = container.getHost();

    client = new CrateDBClient({
      host,
      port: mappedPort,
    });

    systemClient = new CrateDBClient({
      host,
      port: mappedPort,
      defaultSchema: 'sys',
      maxConnections: 1,
    });

    await waitForDatabase(client);
  }, 30000);

  afterAll(async () => {
    try {
      if (container) {
        await container.stop();
      }
    } catch (error) {
      console.error('Error during container teardown:', error);
    }
  });

  /* ---------------------------------------------
   * Error Handling Tests
   * --------------------------------------------- */
  describe('Error Handling', () => {
    it('should throw CrateDBError for invalid table queries', async () => {
      try {
        // Use a client with default config (likely pointing to container)
        await client.execute('SELECT * FROM invalid_table;');
      } catch (error) {
        expect(error).toBeInstanceOf(CrateDBError);
        expect(error.message).toContain('Relation');
        expect(error.code).toBe(4041);
      }
    });

    it('should throw a RequestError (or ConnectionError) on network failure', async () => {
      // Simulate network error by using an invalid host
      const invalidClient = new CrateDBClient({ host: 'invalid.host', port: 4200 });
      try {
        await invalidClient.execute('SELECT 1');
      } catch (error) {
        // Depending on the implementation, it could be a RequestError or ConnectionError
        expect(error).toBeInstanceOf(RequestError);
      }
    });
  });

  /* ---------------------------------------------
   * Authentication Tests
   * --------------------------------------------- */
  describe('Authentication', () => {
    it('should prioritize JWT authentication over basic auth', async () => {
      const jwt = 'dummy.jwt.token';
      const jwtClient = new CrateDBClient({
        host: container.getHost(),
        port: container.getMappedPort(4200),
        user: 'ignoredUser',
        password: 'ignoredPass',
        jwt,
      });

      expect(jwtClient.getHttpOptions().headers?.Authorization).toBe(`Bearer ${jwt}`);
      expect(jwtClient.getHttpOptions().auth).toBeUndefined();
    });
  });

  /* ---------------------------------------------
   * Basic Query Tests
   * --------------------------------------------- */
  describe('Basic Queries', () => {
    it('should execute a basic SELECT query and include durations and sizes', async () => {
      const response = await client.execute('SELECT 1');
      expect(response.rows).toEqual([[1]]);
      expect(response.cols).toEqual(['1']);
      expect(response.durations).toBeDefined();
      expect(response.durations.request).toBeDefined();
      expect(response.durations.cratedb).toBeDefined();
      expect(response.sizes).toBeDefined();
      expect(response.sizes.request).toBeGreaterThan(0);
      expect(response.sizes.response).toBeGreaterThan(0);
    });

    it('should validate the sys.summits table with a separate connection', async () => {
      const result = await systemClient.execute('SELECT COUNT(*) FROM sys.summits');
      expect(result.rows[0][0]).toBeGreaterThanOrEqual(0);
    });
  });

  /* ---------------------------------------------
   * Upsert & Insert Tests
   * --------------------------------------------- */
  describe('Upsert and Insert Operations', () => {
    it('should handle upsert conflicts correctly when inserting data', async () => {
      const tableName = 'my_schema.insert_test';
      await client.createTable({
        [tableName]: {
          id: 'INT PRIMARY KEY',
          name: 'TEXT',
        },
      });
      // Initial insert
      await client.insert(tableName, { id: 1, name: 'test' }, ['id']);
      // Upsert: update existing row
      await client.insert(tableName, { id: 1, name: 'updated_test' }, ['id']);
      await client.refresh(tableName);
      // Insert without primary key conflict resolution (should not update existing row)
      await client.insert(tableName, { id: 1, name: 'should_not_update' }, null);
      await client.refresh(tableName);
      const result = await client.execute(`SELECT * FROM ${tableName}`);
      expect(result.rows).toEqual([[1, 'updated_test']]);
      await client.drop(tableName);
    });

    it('should ignore updates when primary keys are not provided in bulk insert', async () => {
      const tableName = 'my_schema.no_primary_key_test';
      await client.createTable({
        [tableName]: {
          id: 'INT PRIMARY KEY',
          name: 'TEXT',
        },
      });

      const initialData = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ];
      const conflictingData = [
        { id: 1, name: 'Alice Updated' },
        { id: 3, name: 'Charlie' },
      ];

      await client.insertMany(tableName, initialData, ['id']);
      await client.refresh(tableName);
      await client.insertMany(tableName, conflictingData);
      await client.refresh(tableName);
      const result = await client.execute(`SELECT * FROM ${tableName} ORDER BY id`);
      expect(result.rows).toEqual([
        [1, 'Alice'],
        [2, 'Bob'],
        [3, 'Charlie'],
      ]);
      await client.drop(tableName);
    });
  });

  /* ---------------------------------------------
   * Bulk Insert Tests
   * --------------------------------------------- */
  describe('Bulk Insert Operations', () => {
    it('should insert and query bulk data', async () => {
      const tableName = 'my_schema.bulk_insert_test';
      await client.createTable({
        [tableName]: {
          id: 'INT PRIMARY KEY',
          name: 'TEXT',
        },
      });

      const insertStatement = `INSERT INTO ${tableName} (id, name) VALUES (?, ?)`;
      const bulkArgs = [
        [1, 'test'],
        [2, 'test'],
      ];

      const insertManyResult = await client.executeMany(insertStatement, bulkArgs);
      expect(insertManyResult.results.length).toEqual(bulkArgs.length);
      expect(insertManyResult.durations).toBeDefined();
      expect(insertManyResult.durations.request).toBeDefined();
      expect(insertManyResult.durations.cratedb).toBeDefined();

      await client.refresh(tableName);
      const result = await client.execute(`SELECT * FROM ${tableName}`);
      expect(result.rows).toEqual(bulkArgs);
      await client.drop(tableName);
    });

    it('should handle primary key conflicts during bulk insert', async () => {
      const tableName = 'my_schema.primary_key_test';
      await client.createTable({
        [tableName]: {
          id: 'INT PRIMARY KEY',
          name: 'TEXT',
          description: 'TEXT',
        },
      });

      const initialData = [
        { id: 1, name: 'Alice', description: 'Original description' },
        { id: 2, name: 'Bob', description: 'Another description' },
      ];
      const conflictingData = [
        { id: 1, name: 'Alice Updated', description: 'Updated description' },
        { id: 3, name: 'Charlie', description: 'New description' },
      ];

      await client.insertMany(tableName, initialData, ['id']);
      await client.refresh(tableName);
      await client.insertMany(tableName, conflictingData, ['id']);
      await client.refresh(tableName);
      const result = await client.execute(`SELECT * FROM ${tableName} ORDER BY id`);
      expect(result.rows).toEqual([
        [1, 'Alice Updated', 'Updated description'],
        [2, 'Bob', 'Another description'],
        [3, 'Charlie', 'New description'],
      ]);
      await client.drop(tableName);
    });

    it('should perform and validate a bulk insert', async () => {
      const tableName = 'my_schema.bulk_table';
      await client.createTable({
        [tableName]: {
          id: 'INT PRIMARY KEY',
          name: 'TEXT',
          kind: 'TEXT',
          description: 'TEXT',
        },
      });

      const jsonArray = [
        { id: 1, name: 'Earth', kind: 'Planet', description: 'An awesome place to live.' },
        { id: 2, name: 'Sun', kind: 'Star' },
        { id: 3, kind: 'Moon', description: 'Titan, where it rains fossil fuels.' },
      ];

      const { results } = await client.insertMany(tableName, jsonArray);
      expect(results).toEqual([{ rowcount: 1 }, { rowcount: 1 }, { rowcount: 1 }]);
      await client.refresh(tableName);
      const result = await client.execute(`SELECT * FROM ${tableName} ORDER BY id`);
      expect(result.rows).toEqual([
        [1, 'Earth', 'Planet', 'An awesome place to live.'],
        [2, 'Sun', 'Star', null],
        [3, null, 'Moon', 'Titan, where it rains fossil fuels.'],
      ]);
      await client.execute(`DROP TABLE ${tableName}`);
    });
  });

  /* ---------------------------------------------
   * Cursor and Streaming Tests
   * --------------------------------------------- */
  describe('Cursor and Streaming', () => {
    it('should stream query results and validate them', async () => {
      const tableName = 'stream_test_table';
      await client.createTable({
        [tableName]: {
          id: 'INT PRIMARY KEY',
          name: 'TEXT',
          value: 'TEXT',
        },
      });

      const testData = [
        { id: 1, name: 'Alice', value: 'Test 1' },
        { id: 2, name: 'Bob', value: 'Test 2' },
        { id: 3, name: 'Charlie', value: 'Test 3' },
        { id: 4, name: 'Diana', value: 'Test 4' },
        { id: 5, name: 'Eve', value: 'Test 5' },
        { id: 6, name: 'Frank', value: 'Test 6' },
      ];

      await client.insertMany(tableName, testData);
      await client.refresh(tableName);

      const results: CrateDBRecord[] = [];
      for await (const row of client.streamQuery(`SELECT * FROM ${tableName} ORDER BY id`, 2)) {
        results.push(row);
      }
      expect(results).toEqual(testData);
      await client.drop(tableName);
    });
  });
});
