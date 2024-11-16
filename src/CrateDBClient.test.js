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
        await client.executeSql("SELECT 1");
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

  it("should create a table, insert data, and query it", async () => {
    await client.createTable({
      "my_schema.my_table": {
        id: "INT PRIMARY KEY",
        name: "TEXT",
      },
    });
    
    await client.insert("my_schema.my_table", { id: 1, name: "test" });

    await client.refresh("my_schema.my_table");
    
    const result = await client.executeSql("SELECT * FROM my_schema.my_table");
    expect(result.rows).toEqual([[1, "test"]]);
  });

  it("should validate sys.summits table with a separate connection", async () => {
    const result = await systemClient.executeSql("SELECT COUNT(*) FROM sys.summits");
    //expect(result.rows).toBeDefined();
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

    const rowCounts = await client.bulkInsert(tableName, jsonArray);

    // Validate bulk insert row counts
    expect(rowCounts).toEqual([1, 1, 1]);

    await client.refresh(tableName);

    // Query inserted data and validate
    const result = await client.executeSql(`SELECT * FROM ${tableName} ORDER BY id`);
    expect(result.rows).toEqual([
      [1, "Earth", "Planet", "An awesome place to live."],
      [2, "Sun", "Star", null],
      [3, null, "Moon", "Titan, where it rains fossil fuels."],
    ]);

    // Clean up
    await client.executeSql(`DROP TABLE ${tableName}`);
  });
});