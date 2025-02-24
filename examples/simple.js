import { CrateDBClient } from '@proddata/node-cratedb';

(async () => {
  const client = new CrateDBClient({
    user: 'crate',
    password: '',
    host: 'localhost',
    port: 4200,
    ssl: false,
  });

  try {
    // --- Example 1: Creating a Table ---
    console.log('Creating a table...');
    await client.createTable('locations', {
      id: { type: 'INTEGER', primaryKey: true },
      name: { type: 'TEXT' },
      kind: { type: 'TEXT' },
      description: { type: 'TEXT' },
    });
    console.log('Table "locations" created.');

    // --- Example 2: Inserting Data ---
    console.log('Inserting a record...');
    await client.insert('locations', {
      id: 1,
      name: 'Earth',
      kind: 'Planet',
      description: 'An awesome place to live.',
    });
    console.log('Record inserted into "locations".');

    // Refresh the table to make inserted data available for querying
    await client.refresh('locations');

    // --- Example 3: Querying Data ---
    console.log('Querying data...');
    const result = await client.execute('SELECT * FROM locations ORDER BY id');
    console.log('Query results:', result.rows);

    // --- Example 4: Bulk Insert ---
    console.log('Performing a bulk insert...');
    const bulkData = [
      { id: 2, name: 'Sun', kind: 'Star', description: 'Hot and fiery.' },
      { id: 3, name: 'Moon', kind: 'Satellite', description: 'Orbiting the Earth.' },
      { id: 4, kind: 'Asteroid', description: 'Rocky and small.' }, // Missing name
    ];

    const bulkInsertResult = await client.insertMany('locations', bulkData, ['id']);
    console.log('Bulk insert completed. Results:', bulkInsertResult);

    await client.refresh('locations');

    // --- Example 5: Streaming Query Results ---
    console.log('Streaming query results...');
    for await (const row of client.streamQuery('SELECT * FROM locations ORDER BY id')) {
      console.log('Record:', row);
    }

    // --- Example 6: Getting Primary Keys ---
    console.log('Getting primary keys...');
    const primaryKeys = await client.getPrimaryKeys('locations');
    console.log('Primary keys:', primaryKeys);

    // --- Example 7: Dropping the Table ---
    console.log('Dropping the table...');
    await client.drop('locations');
    console.log('Table "locations" dropped.');
  } catch (error) {
    console.error('Error during operations:', error);
  }
})();
