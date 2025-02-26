import { describe, it, expect } from 'vitest';
import { StatementGenerator } from '../src/StatementGenerator';
import { ColumnDefinition } from '../src/interfaces';

describe('StatementGenerator SQL Generation', () => {
  // CREATE TABLE tests
  it('should generate a CREATE TABLE query with full options', () => {
    const tableName = 'my_schema.my_table';
    const columns = {
      id: { type: 'integer', primaryKey: true },
      name: { type: 'string', primaryKey: true },
    };
    const options = {
      primaryKeys: ['id', 'name'],
      clusteredBy: 'id',
      partitionedBy: ['name'],
      numberOfShards: 1,
      numberOfReplicas: '1-all',
    };
    expect(StatementGenerator.createTable(tableName, columns, options)).toBe(
      `CREATE TABLE "my_schema"."my_table" ("id" INTEGER, "name" STRING, PRIMARY KEY("id", "name")) PARTITIONED BY ("name") CLUSTERED BY ("id") INTO 1 SHARDS WITH (number_of_replicas = '1-all');`
    );
  });

  it('should generate a CREATE TABLE query without additional options', () => {
    const tableName = 'simple_table';
    const columns = {
      id: { type: 'integer', primaryKey: false, notNull: false },
      description: { type: 'string', primaryKey: false, notNull: false },
    };
    expect(StatementGenerator.createTable(tableName, columns)).toBe(
      `CREATE TABLE "simple_table" ("id" INTEGER, "description" STRING);`
    );
  });

  it('should generate a CREATE TABLE query with default values and NOT NULL constraints', () => {
    const tableName = 'default_table';
    const columns: Record<string, ColumnDefinition> = {
      id: { type: 'integer', primaryKey: true, notNull: true, defaultValue: 1 },
      name: { type: 'string', primaryKey: false, notNull: false, defaultValue: "'unknown'" },
    };
    expect(StatementGenerator.createTable(tableName, columns)).toBe(
      `CREATE TABLE "default_table" ("id" INTEGER NOT NULL DEFAULT 1, "name" STRING DEFAULT 'unknown', PRIMARY KEY("id"));`
    );
  });

  it('should generate a CREATE TABLE query with generated columns', () => {
    const tableName = 'metrics';
    const columns = {
      id: { type: 'integer', primaryKey: true },
      value: { type: 'double' },
      squared: {
        type: 'double',
        generatedAlways: 'value * value',
      },
      stored_calc: {
        type: 'double',
        generatedAlways: 'value * 2',
        stored: true,
      },
    };

    expect(StatementGenerator.createTable(tableName, columns)).toBe(
      `CREATE TABLE "metrics" (` +
        `"id" INTEGER, ` +
        `"value" DOUBLE, ` +
        `"squared" DOUBLE GENERATED ALWAYS AS (value * value), ` +
        `"stored_calc" DOUBLE GENERATED ALWAYS AS (value * 2), ` +
        `PRIMARY KEY("id")` +
        `);`
    );
  });

  it('should throw error when column has both DEFAULT and GENERATED ALWAYS', () => {
    const tableName = 'invalid_table';
    const columns = {
      id: { type: 'integer', primaryKey: true },
      invalid_col: {
        type: 'double',
        defaultValue: 0,
        generatedAlways: 'value * 2',
      },
    };

    expect(() => StatementGenerator.createTable(tableName, columns)).toThrow(
      'Column "invalid_col" cannot have both DEFAULT and GENERATED ALWAYS values'
    );
  });

  it('should generate an INSERT query with conflict update when primary keys are provided', () => {
    const tableName = 'my_table';
    const keys = ['id', 'name', 'value'];
    const primaryKeys = ['id'];
    expect(StatementGenerator.insert(tableName, keys, primaryKeys)).toBe(
      `INSERT INTO "my_table" ("id", "name", "value") VALUES (?, ?, ?) ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name", "value" = excluded."value";`
    );
  });

  it('should generate an INSERT query with conflict do nothing when primary keys are not provided', () => {
    const tableName = 'my_table';
    const keys = ['id', 'name', 'value'];
    expect(StatementGenerator.insert(tableName, keys, null)).toBe(
      `INSERT INTO "my_table" ("id", "name", "value") VALUES (?, ?, ?) ON CONFLICT DO NOTHING;`
    );
  });

  // Maintenance commands (DROP, REFRESH)
  it('should generate a DROP TABLE query', () => {
    const tableName = 'my_table';
    expect(StatementGenerator.dropTable(tableName)).toBe(`DROP TABLE IF EXISTS "my_table";`);
  });

  it('should generate a REFRESH query', () => {
    const tableName = 'my_table';
    expect(StatementGenerator.refresh(tableName)).toBe(`REFRESH TABLE "my_table";`);
  });

  // Optimization tests
  it('should generate an OPTIMIZE query', () => {
    const tableName = 'my_table';
    const options = { max_num_segments: 1 };
    const partitions = { partition: '1', partition2: 2 };
    expect(StatementGenerator.optimize(tableName, options, partitions)).toBe(
      `OPTIMIZE TABLE "my_table" WITH (max_num_segments=1) PARTITION (partition='1', partition2=2);`
    );
  });

  it('should generate an OPTIMIZE query without options', () => {
    const tableName = 'my_table';
    expect(StatementGenerator.optimize(tableName)).toBe(`OPTIMIZE TABLE "my_table";`);
  });

  describe('OBJECT column types', () => {
    it('should generate a CREATE TABLE query with STRICT object columns', () => {
      const tableName = 'strict_objects';
      const columns = {
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
      };

      expect(StatementGenerator.createTable(tableName, columns)).toBe(
        'CREATE TABLE "strict_objects" (' +
          '"id" INTEGER, ' +
          '"metadata" OBJECT(STRICT) AS (' +
          '"name" TEXT, ' +
          '"location" OBJECT(STRICT) AS ("lat" DOUBLE, "lon" DOUBLE)' +
          '), ' +
          'PRIMARY KEY("id"));'
      );
    });

    it('should generate a CREATE TABLE query with DYNAMIC object columns', () => {
      const tableName = 'dynamic_objects';
      const columns = {
        id: { type: 'integer', primaryKey: true },
        data: {
          type: 'object',
          mode: 'dynamic',
          properties: {
            known_field: { type: 'text' },
          },
        },
      };

      expect(StatementGenerator.createTable(tableName, columns)).toBe(
        'CREATE TABLE "dynamic_objects" (' +
          '"id" INTEGER, ' +
          '"data" OBJECT(DYNAMIC) AS ("known_field" TEXT), ' +
          'PRIMARY KEY("id"));'
      );
    });

    it('should generate a CREATE TABLE query with IGNORED object columns', () => {
      const tableName = 'ignored_objects';
      const columns = {
        id: { type: 'integer', primaryKey: true },
        metadata: {
          type: 'object',
          mode: 'ignored',
          properties: {
            base_info: { type: 'text' },
          },
        },
      };

      expect(StatementGenerator.createTable(tableName, columns)).toBe(
        'CREATE TABLE "ignored_objects" (' +
          '"id" INTEGER, ' +
          '"metadata" OBJECT(IGNORED) AS ("base_info" TEXT), ' +
          'PRIMARY KEY("id"));'
      );
    });

    it('should generate a CREATE TABLE query with default (no mode) object columns', () => {
      const tableName = 'default_objects';
      const columns = {
        id: { type: 'integer', primaryKey: true },
        data: {
          type: 'object',
          properties: {
            field: { type: 'text' },
          },
        },
      };

      expect(StatementGenerator.createTable(tableName, columns)).toBe(
        'CREATE TABLE "default_objects" (' +
          '"id" INTEGER, ' +
          '"data" OBJECT AS ("field" TEXT), ' +
          'PRIMARY KEY("id"));'
      );
    });
  });
});
