import { describe, it, expect } from 'vitest';
import { StatementGenerator } from '../src/StatementGenerator';

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
    const columns = {
      id: { type: 'integer', primaryKey: true, notNull: true, defaultValue: 1 },
      name: { type: 'string', primaryKey: false, notNull: false, defaultValue: "'unknown'" },
    };
    expect(StatementGenerator.createTable(tableName, columns)).toBe(
      `CREATE TABLE "default_table" ("id" INTEGER NOT NULL DEFAULT 1, "name" STRING DEFAULT 'unknown', PRIMARY KEY("id"));`
    );
  });

  // DML tests (DELETE, UPDATE, INSERT)
  it('should generate a DELETE query when a where clause is provided', () => {
    const tableName = 'my_table';
    const whereClause = 'id = 1';
    expect(StatementGenerator.delete(tableName, whereClause)).toBe(`DELETE FROM "my_table" WHERE id = 1;`);
  });

  it('should generate an UPDATE query', () => {
    const tableName = 'my_table';
    const updateOptions = { col1: 'value1', col2: 42 };
    const whereClause = 'id = 1';
    expect(StatementGenerator.update(tableName, updateOptions, whereClause)).toBe(
      `UPDATE "my_table" SET "col1"=?, "col2"=? WHERE id = 1;`
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
    expect(StatementGenerator.dropTable(tableName)).toBe(`DROP TABLE "my_table";`);
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
});
