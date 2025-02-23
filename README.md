[![npm version](https://img.shields.io/npm/v/@proddata/node-cratedb.svg)](https://www.npmjs.com/package/@proddata/node-cratedb)
[![Build Status](https://github.com/proddata/node-cratedb/actions/workflows/test.yml/badge.svg)](https://github.com/proddata/node-cratedb/actions)

# CrateDB HTTP API Client for Node.js

This library is a lightweight Node.js client derived from `node-crate` for interacting with CrateDB via its **HTTP endpoint**. Unlike libraries such as `node-postgres`, which use the PostgreSQL wire protocol, this client communicates with CrateDB's native HTTP API.

> [!CAUTION]
> While it provides basic functionality to interact with CrateDB, it is not production-ready and lacks the robustness of established libraries.
> For production use, consider mature libraries like [`node-postgres`](https://node-postgres.com/) which leverage CrateDB's PostgreSQL compatibility.

## Installation

To install `node-cratedb` using npm:

```bash
npm install @proddata/node-cratedb
```

To use the `CrateDBClient`:

1. Import the `CrateDBClient` class.
2. Instantiate it with your configuration options.
3. Call any of the CRUD and DDL methods provided.

Import and instantiate CrateDBClient:

**ESM (ECMAScript Modules)**

```javascript
import { CrateDBClient } from '@proddata/node-cratedb';

const client = new CrateDBClient();
```

**CommonJS (CJS)**

```javascript
const { CrateDBClient } = require('@proddata/node-cratedb');

const client = new CrateDBClient();
```

For a remote CrateDB Instance:

```javascript
import { CrateDBClient } from '@proddata/node-cratedb';

const client = new CrateDBClient({
  user: 'database-user',
  password: 'secretpassword!!',
  host: 'my.database-server.com',
  port: 4200,
  ssl: true, // Use HTTPS
  keepAlive: true, // Enable persistent connections
  maxConnections: 20, // Limit to 10 concurrent sockets
  defaultSchema: 'my_schema', // Default schema for queries
});
```

You can also use JWT-based authentication. When a jwt is provided, it overrides the basic authentication credentials:

```javascript
import { CrateDBClient } from '@proddata/node-cratedb';

const client = new CrateDBClient({
  host: 'my.database-server.com',
  jwt: 'your.jwt.token.here', // Use JWT for Bearer authentication
  ssl: true,
});
```

### Configuration

The `CrateDBClient` can be configured with either environment variables or directly with an options object. Below are the configuration options, along with their default values.

#### Configuration Options

| Option     | Type               | Default Value                           | Description                          |
| ---------- | ------------------ | --------------------------------------- | ------------------------------------ |
| `user`     | `string`           | `'crate'` or `process.env.CRATEDB_USER` | Database user.                       |
| `password` | `string` or `null` | `''` or `process.env.CRATEDB_PASSWORD`  | Database password.                   |
| `jwt`      | `string \| null`   | `null`                                  | JWT token for Bearer authentication. |

| `host` | `string` | `'localhost'` or `process.env.CRATEDB_HOST` | Database host. |
| `port` | `number` | `4200` or `process.env.CRATEDB_PORT` | Database port. |
| `defaultSchema` | `string` | `null` or `process.env.CRATEDB_DEFAULT_SCHEMA` | Default schema for queries. |
| `connectionString` | `string` | `null` | Connection string, e.g., `https://user:password@host:port/`. |
| `ssl` | `object` or `null` | `null` | SSL configuration; |
| `keepAlive` | `boolean` | `true` | Enables HTTP keep-alive for persistent connections. |
| `maxConnections` | `number` | `20` | Limits the maximum number of concurrent connections. |
| `deserialization` | `DeserializationConfig` | `{ long: 'number', timestamp: 'date', date: 'date' }` | Controls deserialization behaviour |
| `rowMode` | `'array' \| 'object'` | `'array'` | Controls the format of returned rows. |

#### Environment Variables

Alternatively, you can set these variables in your environment:

```bash
export CRATEDB_USER=crate
export CRATEDB_PASSWORD=secretpassword
export CRATEDB_HOST=my.database-server.com
export CRATEDB_PORT=4200
export CRATEDB_DEFAULT_SCHEMA=doc
```

---

## Usage

### General Operations

#### execute(sql, args?, config?)

Execute a SQL query with optional parameters and configuration.

```typescript
// Basic query
await client.execute('SELECT * FROM my_table';);
// Parameterized query
await client.execute('SELECT FROM my_table WHERE id = ?', [123]);
// Query with row mode configuration
await client.execute('SELECT FROM my_table', undefined, { rowMode: 'object' });
```

The `rowMode` configuration determines how rows are returned:

- `'array'` (default): Returns rows as arrays of values
- `'object'`: Returns rows as objects with column names as keys

Example responses:

```typescript
// Basic query
const result = await client.execute('SELECT * FROM my_table');
console.log(result.rows); // [[1, 'Alice', 30], [2, 'Bob', 25]]

// Query with row mode configuration
const result = await client.execute('SELECT * FROM my_table', undefined, { rowMode: 'object' });
console.log(result.rows); // [{id: 1, name: 'Alice', age: 30}, {id: 2, name: 'Bob', age: 25}]
```

#### executeMany(sql, bulk_args)

Execute a raw bulk SQL query.

```js
await client.execute('INSERT INTO my_table VALUES(?);', [['Hello'], ['World']]);
```

#### streamQuery(sql, batchSize)

The `streamQuery` method in CrateDBClient wraps the Cursor functionality
for convenient query streaming. This method automatically manages the cursor's
lifecycle.

Streams query results row by row using an async generator. The `batchSize`
determines the number of rows fetched per request (default is `100`).

```js
for await (const row of client.streamQuery('SELECT * FROM my_table ORDER BY id', 5)) {
  console.log(row); // Process each row individually
}
```

### CRUD Operations

#### insert(tableName, obj, primaryKeys = null)

Insert a new row into a specified table with optional primary key conflict resolution.

- **`tableName`**: The name of the table to insert the row into.
- **`obj`**: An object representing the row to insert.
- **`primaryKeys`**: (Optional) An array of column names to use as primary keys for conflict resolution.

If `primaryKeys` are provided, the method will handle conflicts by updating the non-primary key fields of conflicting rows. If no `primaryKeys` are provided, conflicting rows will be skipped.

```javascript
// Insert a row with primary key conflict resolution
await client.insert('my_table', { id: 1, column1: 'value1', column2: 'value2' }, ['id']);

// Insert a row without conflict resolution
await client.insert('my_table', { id: 1, column1: 'value1', column2: 'value2' });
```

#### insertMany(tableName, jsonArray, primaryKeys = null)

Insert multiple rows into a table with optional primary key conflict resolution.

- **`tableName`**: The name of the table to insert rows into.
- **`jsonArray`**: An array of objects representing rows to insert.
- **`primaryKeys`**: (Optional) An array of column names to use as primary keys for conflict resolution.

If `primaryKeys` are provided, the method will handle conflicts by updating the non-primary key fields of conflicting rows. If no `primaryKeys` are provided, conflicting rows will be skipped.

```javascript
const bulkData = [
  { id: 1, name: 'Earth', kind: 'Planet', description: 'A beautiful place.' },
  { id: 2, name: 'Mars', kind: 'Planet', description: 'The red planet.' },
  { id: 1, name: 'Earth Updated', kind: 'Planet', description: 'Updated description.' }, // Conflict on id
];

await client.insertMany('my_table', bulkData, ['id']);
// Conflicting row with `id: 1` will be updated instead of skipped.

await client.insertMany('my_table', bulkData);
// Conflicting rows will be skipped as no `primaryKeys` are provided.
```

#### update(tableName, options, whereClause)

Update rows in a table that match a WHERE clause.

```js
await client.update('my_table', { column1: 'new_value' }, 'column2 = value2');
```

#### delete(tableName, whereClause)

Delete rows from a table that match a WHERE clause.

```js
await client.delete('my_table', 'column1 = value1');
```

#### drop(tableName)

Drop a specified table.

```js
await client.drop('my_table');
```

#### refresh(tableName)

Refresh a specified table.

```js
await client.refresh('my_table');
```

#### createTable(tableName, schema, options?)

Creates a new table with the specified name, schema, and options.

```typescript
// Basic table creation
await client.createTable('users', {
  id: { type: 'INTEGER', primaryKey: true },
  name: { type: 'TEXT', notNull: true },
  age: { type: 'INTEGER' },
  created_at: { type: 'TIMESTAMP', defaultValue: 'CURRENT_TIMESTAMP' },
  metadata: {
    type: 'object',
    mode: 'strict',
    properties: {
      email: { type: 'TEXT' },
      address: {
        type: 'object',
        mode: 'dynamic',
        properties: {
          city: { type: 'TEXT' },
        },
      },
    },
  },
});

// Advanced table creation with options
await client.createTable(
  'metrics',
  {
    timestamp: { type: 'TIMESTAMP', primaryKey: true },
    week: { type: 'TIMESTAMP', defaultValue: "date_trunc('week', timestamp)" },
    sensor_id: { type: 'TEXT' },
    value: { type: 'DOUBLE' },
    location: {
      type: 'object',
      mode: 'strict',
      properties: {
        lat: { type: 'DOUBLE' },
        lon: { type: 'DOUBLE' },
      },
    },
  },
  {
    numberOfShards: 6,
    numberOfReplicas: '2',
    clusteredBy: 'sensor_id',
    partitionedBy: ['week'],
  }
);
```

##### Parameters

- `tableName`: string - Name of the table to create
- `schema`: Record<string, ColumnDefinition> - Object defining the table columns
  - Regular columns:
    - `type`: string - SQL type of the column
    - `primaryKey?`: boolean - Whether this column is part of primary key
    - `notNull?`: boolean - Whether this column can contain NULL values
    - `defaultValue?`: unknown - Default value for the column
    - `generatedAlways?`: string - SQL expression for generated column
    - `stored?`: boolean - Whether generated column should be stored
  - Object columns:
    - `type`: 'object' - Specifies an object column
    - `mode?`: 'strict' | 'dynamic' | 'ignored' - Object mode (default: no mode)
    - `properties?`: Record<string, ColumnDefinition> - Optional nested column definitions
- `options?`: TableOptions - Additional table configuration
  - `clusteredBy?`: string - Column to use for clustering
  - `partitionedBy?`: string[] - Columns to use for partitioning
  - `numberOfShards?`: number - Number of shards (default: 6)
  - `numberOfReplicas?`: string | number - Number of replicas (default: '1')

### Cursor Operations

#### createCursor(sql)

Create a cursor to fetch large datasets efficiently.

```js
const cursor = client.createCursor('SELECT * FROM my_table ORDER BY id');
await cursor.open();

console.log(await cursor.fetchone()); // Fetch one record
console.log(await cursor.fetchmany(5)); // Fetch 5 records
console.log(await cursor.fetchall()); // Fetch all remaining records

await cursor.close(); // Close the cursor and commit the transaction
```

#### iterate(batchSize)

Creates an async generator that fetches query results in chunks of size
batchSize (default is 100).

```js
const cursor = client.createCursor('SELECT * FROM my_table ORDER BY id');
await cursor.open();

for await (const row of cursor.iterate(5)) {
  console.log(row); // Process each row individually
}

await cursor.close();
```

## Serialization and Deserialization

The client handles serialization and deserialization of various data types,
including `BigInt`, `Date`, and timestamps, ensuring precision and compatibility
between JavaScript and CrateDB.

- **Serialization**

  - Data types such as `BigInt`, `Date`, `Set`, and `Map` are serialized into
    compatible JSON representations without loss of precision.

- **Deserialization**
  - The client supports configurable deserialization for specific CrateDB data
    types, including `LONG`, `DATE`, `TIMESTAMP`, and `TIMESTAMPTZ`.
  - By default, when type information is available in the result set, values are
    automatically converted to the appropriate JavaScript types.
  - Integer values exceeding `Number.MAX_SAFE_INTEGER` are converted to
    `BigInt`, even when explicit type information is unavailable.

Deserialization behavior can be controlled through the client configuration:

```typescript
export type DeserializationConfig = {
  long: 'bigint' | 'number';
  timestamp: 'date' | 'number';
  date: 'date' | 'number';
};
```

## License

MIT License. Feel free to use and modify this library as per the terms of the license.
