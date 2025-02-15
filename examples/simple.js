import { CrateDBClient } from '../dist/CrateDBClient.js';

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
    await client.createTable({
      locations: {
        id: 'INT PRIMARY KEY',
        name: 'TEXT',
        kind: 'TEXT',
        description: 'TEXT',
      },
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

    // --- Example 5: Querying with a Cursor ---
    console.log('Querying data using a cursor...');
    const cursor = client.createCursor('SELECT * FROM locations ORDER BY id');
    await cursor.open();

    console.log('First record:', await cursor.fetchone()); // Fetch one record
    console.log('Next two records:', await cursor.fetchmany(2)); // Fetch 2 records
    console.log('All remaining records:', await cursor.fetchall()); // Fetch all remaining records

    await cursor.close(); // Close the cursor and commit the transaction

    // --- Example 6: Updating Data ---
    console.log('Updating a record...');
    await client.update('locations', { description: 'Blue and beautiful.' }, 'id = 1');
    await client.refresh('locations');

    const updatedResult = await client.execute('SELECT * FROM locations WHERE id = 1');
    console.log('Updated record:', updatedResult.rows);

    // --- Example 7: Deleting Data ---
    console.log('Deleting a record...');
    await client.delete('locations', 'id = 4');
    await client.refresh('locations');

    const remainingResult = await client.execute('SELECT * FROM locations');
    console.log('Remaining records:', remainingResult.rows);

    // --- Example 8: Dropping the Table ---
    console.log('Dropping the table...');
    await client.drop('locations');
    console.log('Table "locations" dropped.');
  } catch (error) {
    console.error('Error during operations:', error);
  }
})();
