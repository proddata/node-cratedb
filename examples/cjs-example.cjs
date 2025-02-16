let CrateDBClient = require('../dist/cjs/CrateDBClient.js');
let client = new CrateDBClient.CrateDBClient();

client
  .execute('SELECT 1;')
  .then((res) => console.log(res))
  .catch((err) => console.error(err));
