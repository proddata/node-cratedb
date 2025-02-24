import { describe, it, beforeAll, afterAll, expect, beforeEach, afterEach } from 'vitest';
import { GenericContainer } from 'testcontainers';
import { CrateDBClient } from '../src/CrateDBClient';
import { CrateDBRecord, CrateDBBaseResponse } from '../src/interfaces';
import { CrateDBError, RequestError } from '../src/utils/Error.js';

// Add this before the main describe block
class TestableClient extends CrateDBClient {
  public transformResponse(response: CrateDBBaseResponse, rowMode: 'array' | 'object'): CrateDBBaseResponse {
    return this._transformResponse(response, rowMode);
  }
}

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
    beforeAll(async () => {
      // Create a test user with password
      await systemClient.execute(`
        CREATE USER test_user WITH (
          password = 'test_password'
        )
      `);
    });

    afterAll(async () => {
      // Clean up the test user
      await systemClient.execute('DROP USER IF EXISTS test_user');
    });

    it('should authenticate with username and password', async () => {
      const authenticatedClient = new CrateDBClient({
        host: container.getHost(),
        port: container.getMappedPort(4200),
        user: 'test_user',
        password: 'test_password',
      });

      const result = await authenticatedClient.execute('SELECT CURRENT_USER');
      expect(result.rows[0][0]).toBe('test_user');
    });

    it('should fail with incorrect password', async () => {
      const invalidClient = new CrateDBClient({
        host: container.getHost(),
        port: container.getMappedPort(4200),
        user: 'test_user2',
        password: 'wrong_password',
      });

      await expect(invalidClient.execute('SELECT 1')).rejects.toThrow();
    });

    it('should authenticate with JWT if provided', async () => {
      // Note: This is a mock test since CrateDB CE doesn't support JWT
      // You would need Enterprise Edition to fully test JWT authentication
      const jwtClient = new CrateDBClient({
        host: container.getHost(),
        port: container.getMappedPort(4200),
        jwt: 'mock.jwt.token',
      });

      // Verify that JWT header is set
      const headers = jwtClient.getHttpOptions().headers;
      expect(headers?.Authorization).toBe('Bearer mock.jwt.token');
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
      await client.createTable(tableName, {
        id: { type: 'INT', primaryKey: true },
        name: { type: 'TEXT' },
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
      await client.createTable(tableName, {
        id: { type: 'INT PRIMARY KEY' },
        name: { type: 'TEXT' },
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
      //const tableName = 'bulk_insert_test';
      await client.createTable(tableName, {
        id: { type: 'INT', primaryKey: true },
        name: { type: 'TEXT' },
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
      await client.createTable(tableName, {
        id: { type: 'INT PRIMARY KEY' },
        name: { type: 'TEXT' },
        description: { type: 'TEXT' },
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
      await client.createTable(tableName, {
        id: { type: 'INT PRIMARY KEY' },
        name: { type: 'TEXT' },
        kind: { type: 'TEXT' },
        description: { type: 'TEXT' },
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
      await client.createTable(tableName, {
        id: { type: 'INT PRIMARY KEY' },
        name: { type: 'TEXT' },
        value: { type: 'TEXT' },
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

  /* ---------------------------------------------
   * Response Transformation Tests
   * --------------------------------------------- */
  describe('Response Transformation', () => {
    let client: TestableClient;
    let mockResponse: CrateDBBaseResponse;

    beforeEach(() => {
      client = new TestableClient();
      mockResponse = {
        cols: ['id', 'name', 'age'],
        rows: [
          [1, 'Alice', 30],
          [2, 'Bob', 25],
        ],
        rowcount: 2,
        duration: 1.234,
        durations: { request: 0, cratedb: 1.234 },
        sizes: { request: 0, response: 0 },
      };
    });

    it('should transform array rows to objects when rowMode is object', () => {
      const transformed = client.transformResponse(mockResponse, 'object');
      expect(transformed.rows).toEqual([
        { id: 1, name: 'Alice', age: 30 },
        { id: 2, name: 'Bob', age: 25 },
      ]);
    });

    it('should keep array format when rowMode is array', () => {
      const transformed = client.transformResponse(mockResponse, 'array');
      expect(transformed.rows).toEqual([
        [1, 'Alice', 30],
        [2, 'Bob', 25],
      ]);
    });

    it('should handle null values correctly', () => {
      mockResponse.rows = [
        [1, null, 30],
        [2, 'Bob', null],
      ];
      const transformed = client.transformResponse(mockResponse, 'object');
      expect(transformed.rows).toEqual([
        { id: 1, name: null, age: 30 },
        { id: 2, name: 'Bob', age: null },
      ]);
    });

    it('should handle empty result sets', () => {
      mockResponse.rows = [];
      mockResponse.rowcount = 0;
      const transformed = client.transformResponse(mockResponse, 'object');
      expect(transformed.rows).toEqual([]);
      expect(transformed.rowcount).toBe(0);
    });

    it('should preserve non-row properties', () => {
      const transformed = client.transformResponse(mockResponse, 'object');
      expect(transformed.duration).toBe(1.234);
      expect(transformed.rowcount).toBe(2);
      expect(transformed.cols).toEqual(['id', 'name', 'age']);
    });
  });

  describe('Object Column Types', () => {
    const tableName = 'test_objects';

    afterEach(async () => {
      await client.execute(`DROP TABLE IF EXISTS ${tableName}`);
    });

    it('should create and query table with nested OBJECT columns', async () => {
      await client.createTable(tableName, {
        id: { type: 'integer', primaryKey: true },
        metadata: {
          type: 'object',
          mode: 'strict',
          properties: {
            name: { type: 'text' },
            location: {
              type: 'object',
              mode: 'strict',
              properties: {
                lat: { type: 'double' },
                lon: { type: 'double' },
              },
            },
          },
        },
        tags: {
          type: 'object',
          mode: 'dynamic',
          properties: {
            category: { type: 'text' },
          },
        },
      });

      await client.insert(tableName, {
        id: 1,
        metadata: {
          name: 'Test Point',
          location: {
            lat: 40.7128,
            lon: -74.006,
          },
        },
        tags: {
          category: 'test',
          dynamic_field: 'should work', // Dynamic field
        },
      });

      await client.refresh(tableName);

      const result = await client.execute(`SELECT * FROM ${tableName}`);
      expect(result.rows[0]).toMatchObject([
        1,
        {
          name: 'Test Point',
          location: {
            lat: 40.7128,
            lon: -74.006,
          },
        },
        {
          category: 'test',
          dynamic_field: 'should work',
        },
      ]);
    });
  });

  /* ---------------------------------------------
   * Schema Information Tests
   * --------------------------------------------- */
  describe('Schema Information', () => {
    it('should retrieve primary keys for a table', async () => {
      const tableName = 'my_schema.primary_keys_test';

      // Create a table with multiple primary keys
      await client.createTable(tableName, {
        id: { type: 'INTEGER', primaryKey: true },
        email: { type: 'TEXT', primaryKey: true },
        name: { type: 'TEXT' },
        created_at: { type: 'TIMESTAMP' },
      });

      // Get primary keys
      const primaryKeys = await client.getPrimaryKeys(tableName);

      // Verify primary keys are returned in correct order
      expect(primaryKeys).toEqual(['id', 'email']);

      // Clean up
      await client.drop(tableName);
    });

    it('should return empty array for table without primary keys', async () => {
      const tableName = 'my_schema.no_pk_test';

      // Create a table without primary keys
      await client.createTable(tableName, {
        id: { type: 'INTEGER' },
        name: { type: 'TEXT' },
      });

      // Get primary keys
      const primaryKeys = await client.getPrimaryKeys(tableName);

      // Verify empty array is returned
      expect(primaryKeys).toEqual([]);

      // Clean up
      await client.drop(tableName);
    });

    it('should handle schema-qualified and unqualified table names', async () => {
      const schemaTableName = 'my_schema.qualified_test';
      const unqualifiedTableName = 'unqualified_test';

      // Create tables in different schemas
      await client.createTable(schemaTableName, {
        id: { type: 'INTEGER', primaryKey: true },
      });

      await client.createTable(unqualifiedTableName, {
        id: { type: 'INTEGER', primaryKey: true },
        email: { type: 'TEXT', primaryKey: true },
      });

      // Test schema-qualified table
      const qualifiedKeys = await client.getPrimaryKeys(schemaTableName);
      expect(qualifiedKeys).toEqual(['id']);

      // Test unqualified table (should use default 'doc' schema)
      const unqualifiedKeys = await client.getPrimaryKeys(unqualifiedTableName);
      expect(unqualifiedKeys).toEqual(['id', 'email']);

      // Clean up
      await client.drop(schemaTableName);
      await client.drop(unqualifiedTableName);
    });
  });

  describe('Compression', () => {
    it('should compress large requests when enabled', async () => {
      // Create a large payload that will trigger compression
      const largeData = Array(100).fill('test data').join(' ');
      const tableName = 'compression_test';

      await client.createTable(tableName, {
        id: { type: 'INTEGER', primaryKey: true },
        data: { type: 'TEXT' },
      });

      const response = await client.insert(tableName, {
        id: 1,
        data: largeData,
      });

      // Verify compression metrics
      expect(response.sizes.requestUncompressed).toBeGreaterThan(response.sizes.request);
      expect(response.sizes.request).toBeGreaterThan(0);

      await client.drop(tableName);
    });

    it('should not compress small requests', async () => {
      const smallData = 'small test data';
      const tableName = 'small_data_test';

      await client.createTable(tableName, {
        id: { type: 'INTEGER', primaryKey: true },
        data: { type: 'TEXT' },
      });

      const response = await client.insert(tableName, {
        id: 1,
        data: smallData,
      });

      // Verify no compression was applied
      expect(response.sizes.request).toBe(response.sizes.requestUncompressed);
      expect(response.sizes.request).toBeLessThan(1024);

      await client.drop(tableName);
    });

    it('should respect compression setting when disabled', async () => {
      const clientWithoutCompression = new CrateDBClient({
        host: container.getHost(),
        port: container.getMappedPort(4200),
        enableCompression: false,
      });

      const largeData = Array(1000).fill('test data').join(' ');
      const tableName = 'compression_disabled_test';

      await clientWithoutCompression.createTable(tableName, {
        id: { type: 'INTEGER', primaryKey: true },
        data: { type: 'TEXT' },
      });

      const response = await clientWithoutCompression.insert(tableName, {
        id: 1,
        data: largeData,
      });

      // Verify no compression was applied despite large payload
      expect(response.sizes.request).toBe(response.sizes.requestUncompressed);

      await clientWithoutCompression.drop(tableName);
    });
  });
});
