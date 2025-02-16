import { CrateDBClient } from '../dist/esm/CrateDBClient.js';
let client = new CrateDBClient();

const stmt = `
SELECT 
    1 AS int_value,
    256::SHORT AS short_value,
    '9223372036854775807'::LONG AS long_value,
    '9223372036854775807.1'::NUMERIC AS numeric_value,
    3.141::FLOAT AS float_value, 
    3.1415926535::DOUBLE AS double_value, 
    'Hello, CrateDB!' AS string_value,
    'a'::CHAR AS char_value,
    'ab'::CHARACTER(2) AS character_value,
    true AS boolean_value,
    [['a'], ['b'], ['c']] AS string_array, 
    [[1], [2], [3]]::ARRAY(ARRAY(LONG)) AS long_array_array, 
    [1, 2, 3] AS integer_array, 
    { "key" = 'value1', "key2" = 42 } AS object_value,
    [8.6821, 50.1109]::GEO_POINT AS geo_point_value,
    { "type" = 'Point', "coordinates" = [8.6821, 50.1109] }::OBJECT::GEO_SHAPE AS geo_shape_value,
    [0.123, 0.999]::FLOAT_VECTOR(2) AS float_vector_value,
    '2025-01-01'::DATE AS date_value,
    '1 month 5 minutes'::INTERVAL AS interval_value,
    '2025-01-01T16:27:52.497Z'::TIMESTAMP AS timestamp_value,
    '2025-01-01T16:27:52.497+01'::TIMESTAMPTZ AS timestamptz_value,
    '127.0.0.1'::IP AS ipv4_value,
    '0:0:0:0:0:ffff:c0a8:64'::IP AS ipv6_value,
    B'00010010' AS bit limit 100;
`;

async function run() {
  try {
    const res = await client.execute(stmt);
    console.log(res.rows[0][11]);
    //console.log(JSON.stringify(res, replacer, 2));
  } catch (err) {
    console.error(err);
  }
}

run();

function replacer(_, value) {
  if (typeof value === 'bigint') {
    return JSON.rawJSON(value.toString());
  }
  if (value instanceof Map) {
    return Object.fromEntries(value);
  }
  if (value instanceof Set) {
    return Array.from(value);
  }
  return value;
}
