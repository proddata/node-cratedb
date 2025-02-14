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

  // New test to validate JWT configuration behavior
  it("should prioritize JWT authentication over basic auth", async () => {
    const jwt = "dummy.jwt.token";
    const jwtClient = new CrateDBClient({
      host: container.getHost(),
      port: container.getMappedPort(4200),
      user: "ignoredUser",
      password: "ignoredPass",
      jwt: jwt,
    });

    // Verify that the Authorization header is set to the JWT value
    expect(jwtClient.httpOptions.headers.Authorization).toBe(`Bearer ${jwt}`);
    // And that basic auth is not used
    expect(jwtClient.httpOptions.auth).toBeUndefined();

    // Optionally, attempt a query. This may fail if the server doesn't accept JWT,
    // but the test mainly verifies configuration.
    try {
      await jwtClient.execute("SELECT 1");
    } catch (error) {
      // Depending on the server behavior, an auth error may be thrown.
      expect(error.message).toContain("The input is not a valid base 64 encoded string.");
    }
  });

  it("should execute a basic SELECT query and include durations", async () => {
    // Execute a simple query
    const response = await client.execute("SELECT 1");
  
    // Validate the response structure
    expect(response.rows).toEqual([[1]]);
    expect(response.cols).toEqual(["1"]);
  
    // Check if durations exist
    expect(response.durations).toBeDefined();
    expect(response.durations.request).toBeDefined();
    expect(response.durations.cratedb).toBeDefined();

    // Check if sizes exist
    expect(response.sizes).toBeDefined();
    expect(response.sizes.request).toBeGreaterThan(0);
    expect(response.sizes.response).toBeGreaterThan(0);
  });

  it("should return an error for invalid SQL queries", async () => {
    try {
      await client.execute("SELECT * FROM invalid_table");
    } catch (error) {
      expect(error.message).toContain("Table 'invalid_table' unknown");
      if (error.response && error.response.durations) {
        // Check if durations object exists in the error response
        expect(error.response.durations).toBeDefined();
        expect(error.response.durations.request).toBeDefined();
        expect(error.response.durations.cratedb).toBeDefined();
      }
    }
  });

  it("should handle upsert conflicts correctly when inserting data", async () => {
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

  it("should insert and query bulk data", async () => {
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

      // Check if the durations object exists
    expect(insertManyResult.durations).toBeDefined();
    expect(insertManyResult.durations.request).toBeDefined();
    expect(insertManyResult.durations.cratedb).toBeDefined();

    // Validate the number of results matches the bulk args
    expect(insertManyResult.results.length).toEqual(insert_bulk_args.length);

    // Refresh the table to make the data queryable
    await client.refresh(tableName);

    // Query the data and verify
    const result = await client.execute(`SELECT * FROM ${tableName}`);
    expect(result.rows).toEqual(insert_bulk_args);

    await client.drop(tableName);
  });

  it("should handle primary key conflicts during bulk insert", async () => {
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

  it("should ignore updates when primary keys are not provided in bulk insert", async () => {
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

  it("should validate the sys.summits table with a separate connection", async () => {
    const result = await systemClient.execute("SELECT COUNT(*) FROM sys.summits");
    expect(result.rows[0][0]).toBeGreaterThanOrEqual(0);
  });

  it("should perform and validate a bulk insert", async () => {
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

  it("should stream query results and validate them", async () => {
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