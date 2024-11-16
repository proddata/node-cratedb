import { describe, it, beforeAll, afterAll, expect } from "vitest";
import { GenericContainer } from "testcontainers";
import { CrateDBClient } from "./CrateDBClient.js";

describe("CrateDBCursor", () => {
  let container;
  let client;

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

    await client.refresh(tableName);

    const cursor = client.createCursor(`SELECT * FROM ${tableName} ORDER BY id`);

    try {
      await cursor.open();

      const firstRow = await cursor.fetchone();
      expect(firstRow).toEqual({ id: 1, name: "Alice" });

      const secondRow = await cursor.fetchone();
      expect(secondRow).toEqual({ id: 2, name: "Bob" });

      const remainingRows = await cursor.fetchmany(2);
      expect(remainingRows).toEqual([{ id: 3, name: "Charlie" }]);

      const noMoreRows = await cursor.fetchone();
      expect(noMoreRows).toBeNull();
    } finally {
      await cursor.close();
      await client.executeSql(`DROP TABLE ${tableName}`);
    }
  });
});