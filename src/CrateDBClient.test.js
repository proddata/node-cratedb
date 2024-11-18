import { describe, it, beforeAll, afterAll, expect } from "vitest";
import { GenericContainer } from "testcontainers";
import { CrateDBClient } from "./CrateDBClient.js";

describe("CrateDBClient", () => {
  let container;
  let client;
  let systemClient;

  const waitForDatabase = async (client, retries = 20, interval = 1000) => {
    for (let i = 0; i < retries; i++) {
      try {
        await client.execute("SELECT 1");
        return;
      } catch {
        await new Promise((resolve) => setTimeout(resolve, interval));
      }
    }
    throw new Error("Database did not start in time");
  };

  beforeAll(async () => {
    container = await new GenericContainer("crate").withExposedPorts(4200).start();
    const mappedPort = container.getMappedPort(4200);
    const host = container.getHost();

    client = new CrateDBClient({
      host: host,
      port: mappedPort,
    });

    systemClient = new CrateDBClient({
      host: host,
      port: mappedPort,
      defaultSchema: "sys",
      maxConnections: 1,
    });

    await waitForDatabase(client);
  }, 30000); // Explicitly set the timeout for this hook

  afterAll(async () => {
    try {
      if (container) {
        await container.stop();
      }
    } catch (error) {
      console.error("Error during container teardown:", error);
    }
  });

  it("should return error for invalid SQL", async () => {
    try {
      await client.execute("SELECT * FROM invalid_table");
    } catch (error) {
      expect(error.message).toContain("Table 'invalid_table' unknown");
    }
  });

  it("should create a table, insert data with upsert, and verify no update without conflict keys", async () => {
    // Define the table name as a variable
    const tableName = "my_schema.insert_test";
  
    // Step 1: Create the table
    await client.createTable({
      [tableName]: {
        id: "INT PRIMARY KEY",
        name: "TEXT",
      },
    });
  
    // Step 2: Insert a row using upsert (with primaryKeys)
    await client.insert(tableName, { id: 1, name: "test" }, ["id"]);
  
    // Step 3: Insert another row with the same primary key using upsert
    await client.insert(tableName, { id: 1, name: "updated_test" }, ["id"]);

    await client.refresh(tableName);
  
    // Step 4: Insert another row without specifying primaryKeys (should do nothing on conflict)
    await client.insert(tableName, { id: 1, name: "should_not_update" },null);
  
    // Step 5: Refresh the table to make the data queryable
    await client.refresh(tableName);
  
    // Step 6: Query the table and verify the result
    const result = await client.execute(`SELECT * FROM ${tableName}`);
    expect(result.rows).toEqual([[1, "updated_test"]]);
  
    // Step 7: Clean up by dropping the table
    await client.drop(tableName);
  });

  it("should create a table, insert bulk data, and query it", async () => {
    const tableName = "my_schema.bulk_insert_tets";
    await client.createTable({
      [tableName]: {
        id: "INT PRIMARY KEY",
        name: "TEXT",
      },
    });

    const insert_statement = `INSERT INTO ${tableName}  (id, name) VALUES (?, ?)`;
    const insert_bulk_args = [
      [1, "test"], 
      [2,  "test"]
    ];

    const insertManyResult = await client.executeMany(insert_statement, insert_bulk_args);
    expect(insertManyResult.results.length).toEqual(insert_bulk_args.length);

    // Refresh the table to make the data queryable
    await client.refresh(tableName);

    // Query the data and verify
    const result = await client.execute(`SELECT * FROM ${tableName}`);
    expect(result.rows).toEqual(insert_bulk_args);

    await client.drop(tableName);
  });

  it("should handle primary key conflicts in insertMany", async () => {
    const tableName = "my_schema.primary_key_test";
  
    await client.createTable({
      [tableName]: {
        id: "INT PRIMARY KEY",
        name: "TEXT",
        description: "TEXT",
      },
    });
  
    const initialData = [
      { id: 1, name: "Alice", description: "Original description" },
      { id: 2, name: "Bob", description: "Another description" },
    ];
  
    const conflictingData = [
      { id: 1, name: "Alice Updated", description: "Updated description" },
      { id: 3, name: "Charlie", description: "New description" },
    ];
  
    // Insert initial data
    await client.insertMany(tableName, initialData, ["id"]);
    await client.refresh(tableName);
  
    // Insert conflicting data with primary key conflict handling
    await client.insertMany(tableName, conflictingData, ["id"]);
    await client.refresh(tableName);
  
    // Query the table and validate the results
    const result = await client.execute(`SELECT * FROM ${tableName} ORDER BY id`);
    expect(result.rows).toEqual([
      [1, "Alice Updated", "Updated description"], // Updated row
      [2, "Bob", "Another description"],          // Unchanged row
      [3, "Charlie", "New description"],          // Newly inserted row
    ]);
  
    // Clean up
    await client.drop(tableName);
  });

  it("should not update rows when primaryKeys is not provided in insertMany", async () => {
    const tableName = "my_schema.no_primary_key_test";
  
    await client.createTable({
      [tableName]: {
        id: "INT PRIMARY KEY",
        name: "TEXT",
      },
    });
  
    const initialData = [
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ];
  
    const conflictingData = [
      { id: 1, name: "Alice Updated" },
      { id: 3, name: "Charlie" },
    ];
  
    // Insert initial data
    await client.insertMany(tableName, initialData, ["id"]);
    await client.refresh(tableName);
  
    // Insert conflicting data without primaryKeys
    await client.insertMany(tableName, conflictingData);
    await client.refresh(tableName);
  
    // Query the table and validate the results
    const result = await client.execute(`SELECT * FROM ${tableName} ORDER BY id`);
    expect(result.rows).toEqual([
      [1, "Alice"],         // Unchanged row
      [2, "Bob"],           // Unchanged row
      [3, "Charlie"],       // New row
    ]);
  
    // Clean up
    await client.drop(tableName);
  });

  it("should validate sys.summits table with a separate connection", async () => {
    const result = await systemClient.execute("SELECT COUNT(*) FROM sys.summits");
    expect(result.rows[0][0]).toBeGreaterThanOrEqual(0);
  });

  it("should perform a bulk insert and validate data", async () => {
    const tableName = "my_schema.bulk_table";

    await client.createTable({
      [tableName]: {
        id: "INT PRIMARY KEY",
        name: "TEXT",
        kind: "TEXT",
        description: "TEXT",
      },
    });

    const jsonArray = [
      { id: 1, name: "Earth", kind: "Planet", description: "An awesome place to live." },
      { id: 2, name: "Sun", kind: "Star" }, // Missing description
      { id: 3, kind: "Moon", description: "Titan, where it rains fossil fuels." }, // Missing name
    ];

    const {results} = await client.insertMany(tableName, jsonArray);

    // Validate bulk insert row counts
    expect(results).toEqual([{"rowcount":1}, {"rowcount":1}, {"rowcount":1}]);

    await client.refresh(tableName);

    // Query inserted data and validate
    const result = await client.execute(`SELECT * FROM ${tableName} ORDER BY id`);
    expect(result.rows).toEqual([
      [1, "Earth", "Planet", "An awesome place to live."],
      [2, "Sun", "Star", null],
      [3, null, "Moon", "Titan, where it rains fossil fuels."],
    ]);

    // Clean up
    await client.execute(`DROP TABLE ${tableName}`);
  });

  it("should stream results using streamQuery()", async () => {
    const tableName = "stream_test_table";

    // Create the table and insert test data
    await client.createTable({
      [tableName]: {
        id: "INT PRIMARY KEY",
        name: "TEXT",
        value: "TEXT",
      },
    });

    const testData = [
      { id: 1, name: "Alice", value: "Test 1" },
      { id: 2, name: "Bob", value: "Test 2" },
      { id: 3, name: "Charlie", value: "Test 3" },
      { id: 4, name: "Diana", value: "Test 4" },
      { id: 5, name: "Eve", value: "Test 5" },
      { id: 6, name: "Frank", value: "Test 6" },
    ];

    await client.insertMany(tableName, testData);
    await client.refresh(tableName);

    // Stream query results
    const results = [];
    for await (const row of client.streamQuery(`SELECT * FROM ${tableName} ORDER BY id`, 2)) {
      results.push(row);
    }

    // Validate the streamed results
    expect(results).toEqual(testData);

    // Clean up
    await client.drop(tableName);
  });
});