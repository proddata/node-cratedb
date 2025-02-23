import { OptimizeOptions, BaseColumnDefinition, ColumnDefinition, TableOptions } from './interfaces';
import { Serializer } from './Serializer';

export class StatementGenerator {
  public static createTable(
    tableName: string,
    schema: Record<string, ColumnDefinition>,
    options?: TableOptions
  ): string {
    // Validate column definitions
    Object.entries(schema).forEach(([col, definition]) => {
      if ('type' in definition && definition.type !== 'object') {
        if (
          (definition as BaseColumnDefinition).defaultValue !== undefined &&
          (definition as BaseColumnDefinition).generatedAlways
        ) {
          throw new Error(`Column "${col}" cannot have both DEFAULT and GENERATED ALWAYS values`);
        }
      }
    });

    // Build column definitions
    const columns = Object.entries(schema)
      .map(([col, definition]) => this.buildColumnDefinition(col, definition))
      .join(', ');

    // Build primary key clause if any
    const primaryKeys = Object.keys(schema)
      .filter((col) => (schema[col] as BaseColumnDefinition).primaryKey)
      .map((col) => `"${col}"`)
      .join(', ');
    const primaryKeyClause = primaryKeys ? `, PRIMARY KEY(${primaryKeys})` : '';

    // Start query construction
    let query = `CREATE TABLE ${this.quoteIdentifier(tableName)} (${columns}${primaryKeyClause})`;

    // Partition clause
    if (options?.partitionedBy?.length) {
      const partitionCols = options.partitionedBy.map((col) => `"${col}"`).join(', ');
      query += ` PARTITIONED BY (${partitionCols})`;
    }

    // Cluster clause
    if (options?.clusteredBy || options?.numberOfShards) {
      if (options.clusteredBy && options.numberOfShards) {
        query += ` CLUSTERED BY ("${options.clusteredBy}") INTO ${options.numberOfShards} SHARDS`;
      } else if (options.clusteredBy) {
        query += ` CLUSTERED BY ("${options.clusteredBy}")`;
      } else if (options.numberOfShards) {
        query += ` CLUSTERED INTO ${options.numberOfShards} SHARDS`;
      }
    }

    // Replicas clause
    if (options?.numberOfReplicas !== undefined) {
      query += ` WITH (number_of_replicas = '${options.numberOfReplicas}')`;
    }

    return query + ';';
  }

  public static dropTable(tableName: string): string {
    return `DROP TABLE ${this.quoteIdentifier(tableName)};`;
  }

  public static insert(tableName: string, keys: string[], primaryKeys: string[] | null): string {
    const placeholders = keys.map(() => '?').join(', ');
    const columns = keys.map((key) => `"${key}"`).join(', ');
    let query = `INSERT INTO ${this.quoteIdentifier(tableName)} (${columns}) VALUES (${placeholders})`;

    if (primaryKeys && primaryKeys.length > 0) {
      const nonPrimaryKeys = keys.filter((key) => !primaryKeys.includes(key));
      const updates = nonPrimaryKeys.map((key) => `"${key}" = excluded."${key}"`).join(', ');
      const pkClause = primaryKeys.map((key) => `"${key}"`).join(', ');
      query += ` ON CONFLICT (${pkClause}) DO UPDATE SET ${updates}`;
    } else {
      query += ' ON CONFLICT DO NOTHING';
    }

    return query + ';';
  }

  public static refresh(tableName: string): string {
    return `REFRESH TABLE ${this.quoteIdentifier(tableName)};`;
  }

  public static optimize(
    tableName: string,
    options?: OptimizeOptions,
    partitions?: Record<string, string | number>
  ): string {
    let query = `OPTIMIZE TABLE ${this.quoteIdentifier(tableName)}`;

    // Build options clause
    if (options && Object.keys(options).length > 0) {
      const optionsClauses = Object.entries(options)
        .map(([key, value]) => `${key}=${Serializer.serialize(value)}`)
        .join(', ');
      query += ` WITH (${optionsClauses})`;
    }

    // Build partitions clause
    if (partitions && Object.keys(partitions).length > 0) {
      const partitionClauses = Object.entries(partitions)
        .map(([key, value]) => {
          const val = typeof value === 'string' ? `'${value}'` : value;
          return `${key}=${val}`;
        })
        .join(', ');
      query += ` PARTITION (${partitionClauses})`;
    }

    return query + ';';
  }

  public static getPrimaryKeys(): string {
    return `
      SELECT
        column_name
      FROM
        information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kc
        ON kc.table_catalog = tc.table_catalog
        AND kc.table_schema = tc.table_schema
        AND kc.table_name = tc.table_name
        AND kc.constraint_name = tc.constraint_name
      WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = ?
      AND tc.table_name = ?
      ORDER BY kc.ordinal_position;`;
  }

  private static quoteIdentifier(tableName: string): string {
    return tableName
      .split('.')
      .map((part) => `"${part}"`)
      .join('.');
  }

  private static _prepareOptions(options: Record<string, unknown>): {
    keys: string[];
    values: string[];
    args: unknown[];
  } {
    const keys = Object.keys(options).map((key) => `"${key}"`);
    const values = keys.map(() => '?');
    const args = Object.values(options);
    return { keys, values, args };
  }

  private static buildColumnDefinition(colName: string, definition: ColumnDefinition): string {
    if ('properties' in definition) {
      // Handle OBJECT type
      const objectMode = definition.mode ? `(${definition.mode.toUpperCase()})` : '';
      const objectProps = Object.entries(definition.properties || {})
        .map(([key, prop]) => this.buildColumnDefinition(key, prop))
        .join(', ');
      return `"${colName}" OBJECT${objectMode} AS (${objectProps})`;
    }
    const baseDefinition = definition as BaseColumnDefinition;
    // Regular column definition
    let colDef = `"${colName}" ${definition.type.toUpperCase()}`;
    if (baseDefinition.notNull) colDef += ' NOT NULL';
    if (baseDefinition.defaultValue !== undefined) colDef += ` DEFAULT ${baseDefinition.defaultValue}`;
    if (baseDefinition.generatedAlways) colDef += ` GENERATED ALWAYS AS (${baseDefinition.generatedAlways})`;
    return colDef;
  }
}
