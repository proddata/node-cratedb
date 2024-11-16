# CrateDB HTTP API Client for Node.js

This library is a lightweight Node.js client derived from `node-crate` for interacting with CrateDB via its **HTTP endpoint**. Unlike libraries such as `node-postgres`, which use the PostgreSQL wire protocol, this client communicates with CrateDB's native HTTP API.

> [!CAUTION]
> **This library is primarily a proof of concept.**  
> While it provides basic functionality to interact with CrateDB, it is not production-ready and lacks the robustness of established libraries.  
> 
> For production use, consider mature libraries like [`node-postgres`](https://node-postgres.com/) which leverage CrateDB's PostgreSQL compatibility. Use this client only for **testing, experimentation**, or if you know what you're doing. :wink:


## Configuration

The `CrateDBClient` can be configured with either environment variables or directly with an options object. Below are the configuration options, along with their default values.

### Configuration Options

| Option             | Type                | Default Value                                   | Description                                                     |
|--------------------|---------------------|-------------------------------------------------|-----------------------------------------------------------------|
| `user`             | `string`            | `'crate'` or `process.env.CRATEDB_USER`         | Database user.                                                  |
| `password`         | `string` or `null`  | `''` or `process.env.CRATEDB_PASSWORD`          | Database password.                                              |
| `host`             | `string`            | `'localhost'`  or `process.env.CRATEDB_HOST`    | Database host.                                                  |
| `port`             | `number`            | `4200` or `process.env.CRATEDB_PORT`            | Database port.                                                  |
| `defaultSchema`    | `string`            | `'doc'` or `process.env.CRATEDB_DEFAULT_SCHEMA` | Default schema for queries.                                     |
| `connectionString` | `string`            | `null`                                          | Connection string, e.g., `https://user:password@host:port/`.    |
| `ssl`              | `object` or `null`  | `null`                                          | SSL configuration;                                              |
| `keepAlive`        | `boolean`           | `true`                                          | Enables HTTP keep-alive for persistent connections.             |
| `maxSockets`       | `number`            | `Infinity`                                      | Limits the maximum number of concurrent connections.            |

### Environment Variables

Alternatively, you can set these variables in your environment:

```bash
export CRATEDB_USER=crate
export CRATEDB_PASSWORD=secretpassword
export CRATEDB_HOST=my.database-server.com
export CRATEDB_PORT=4200
export CRATEDB_DEFAULT_SCHEMA=doc
```


## Usage

To use the `CrateDBClient`:

1. Import the `CrateDBClient` class.
2. Instantiate it with your configuration options.
3. Call any of the CRUD and DDL methods provided.

```javascript
import { CrateDBClient } from './CrateDBClient.js';

const client = new CrateDBClient({
  user: 'database-user',
  password: 'secretpassword!!',
  host: 'my.database-server.com',
  port: 5334,
  defaultSchema: 'my_schema',
  keepAlive: true, // Enable persistent connections
  maxSockets: 10  // Limit to 10 concurrent sockets
});
```

### CRUD Operations

#### executeSql(sql, args)

Execute a raw SQL query.

```js
await client.executeSql('SELECT * FROM my_table';);
await client.executeSql('SELECT * FROM my_table', []);
```

#### insert(tableName, options)

Insert a new row into a specified table.

```js
await client.insert('my_table', { column1: 'value1', column2: 'value2' });
```

#### bulkInsert(tableName, jsonArray)

Insert multiple rows into a table using bulk operations.

```js
const bulkData = [
  { id: 1, name: 'Earth', kind: 'Planet', description: 'A beautiful place.' },
  { id: 2, name: 'Mars', kind: 'Planet', description: 'The red planet.' },
  { id: 3, name: 'Sun', kind: 'Star', description: 'A hot and fiery place.' },
];

const rowCounts = await client.bulkInsert('my_table', bulkData);
console.log('Row counts:', rowCounts);
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

```
await client.drop('my_table');
```

#### createTable(schema)

Create a new table based on a schema definition.

```js
await client.createTable({
  my_table: {
    id: 'INTEGER PRIMARY KEY',
    name: 'STRING',
    created_at: 'TIMESTAMP'
  }
});
```

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

### Connection Management

Configure keepAlive for persistent connections and maxSockets to limit concurrent connections.

```js
const client = new CrateDBClient({
  keepAlive: true,
  maxSockets: 5
});
```

## License

MIT License. Feel free to use and modify this library as per the terms of the license.

