import { CrateDBClient } from '../dist/esm/CrateDBClient.js';
let client = new CrateDBClient();

client
  .execute('SELECT 1;')
  .then((res) => console.log(res))
  .catch((err) => console.error(err));
