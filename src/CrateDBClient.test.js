import { describe, it, beforeAll, afterAll, expect } from "vitest";
import { GenericContainer } from "testcontainers";
import { CrateDBClient } from "./CrateDBClient.js";

describe("CrateDBClient", () => {
  let container;
  let client;

  beforeAll(async () => {
    try {
      container = await new GenericContainer("crate").withExposedPorts(4200).start();
      const mappedPort = container.getMappedPort(4200);
      const host = container.getHost();

      client = new CrateDBClient({
        host: host,
        port: mappedPort,
      });

      // Ensure CrateDB has enough time to initialize
      await new Promise((resolve) => setTimeout(resolve, 10000));
    } catch (error) {
      console.error("Error during container setup:", error);
      throw error;
    }
  }, 30000); // Explicitly set the timeout for this hook

  afterAll(async () => {
    if (container) {
      await container.stop();
    }
  });

  it("should create a table, insert data, and query it", async () => {
    await client.createTable({
      my_table: {
        id: "INT PRIMARY KEY",
        name: "TEXT",
      },
    });
    
    await client.insert("my_table", { id: 1, name: "test" });

    await client.executeSql("REFRESH TABLE my_table");
    
    const result = await client.executeSql("SELECT * FROM my_table");
    expect(result.rows).toEqual([[1, "test"]]);
  });
});