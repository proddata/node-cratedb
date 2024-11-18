import { describe, it, beforeAll, afterAll, expect } from "vitest";
import { GenericContainer } from "testcontainers";
import { CrateDBClient } from "./CrateDBClient.js";

describe("CrateDBCursor", () => {
  let container;
  let client;

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

  it("should create a cursor, fetch rows as objects, and close it", async () => {
    const tableName = `my_table_${Date.now()}`;

    await client.createTable({
      [tableName]: {
        id: "INT PRIMARY KEY",
        name: "TEXT",
      },
    });

    await client.insert(tableName, { id: 1, name: "Alice" });
    await client.insert(tableName, { id: 2, name: "Bob" });
    await client.insert(tableName, { id: 3, name: "Charlie" });
    await client.insert(tableName, { id: 4, name: "David" });

    await client.refresh(tableName);

    const cursor = client.createCursor(`SELECT * FROM ${tableName} ORDER BY id`);

    try {
      await cursor.open();

      const firstRow = await cursor.fetchone();
      expect(firstRow).toEqual({ id: 1, name: "Alice" });

      const fetchTwoRows = await cursor.fetchmany(2);
      expect(fetchTwoRows).toEqual([{ id: 2, name: "Bob" },{ id: 3, name: "Charlie" }]);

      const fetchNoRows = await cursor.fetchmany(0);
      expect(fetchNoRows).toEqual([]);

      const remainingRows = await cursor.fetchall();
      expect(remainingRows).toEqual([{ id: 4, name: "David" }]);

      const noMoreRows = await cursor.fetchone();
      expect(noMoreRows).toBeNull();

      const noMoreRowsMany = await cursor.fetchmany(2);
      expect(noMoreRowsMany).toBeNull();

      const noMoreRowsAll = await cursor.fetchall();
      expect(noMoreRowsAll).toBeNull();
    } finally {
      await cursor.close();
      await client.execute(`DROP TABLE ${tableName}`);
    }
  });

  it("should iterate over rows using cursor.iterate()", async () => {
    const tableName = "test_table";

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

    // Use cursor to iterate over rows
    const cursor = client.createCursor(`SELECT * FROM ${tableName} ORDER BY id`);
    const results = [];

    try {
      await cursor.open();

      for await (const row of cursor.iterate(5)) {
        results.push(row);
      }

    } finally {
      await cursor.close();
    }

    // Validate the fetched results
    expect(results).toEqual(testData);

    // Clean up
    await client.drop(tableName);
  });
});