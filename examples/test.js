import http from 'http';
import zlib from 'zlib';

// Configurable number of requests (default to 100)
const numRequests = process.argv[2] ? parseInt(process.argv[2], 10) : 100;

const requestData = JSON.stringify({
  stmt: 'SELECT * FROM sys.summits',
});

// Pre-compress the request data using deflate once (since it's the same for every request)
const compressedData = zlib.deflateSync(requestData);

/**
 * Formats a number of bytes into a human-readable string.
 * @param {number} bytes - The number of bytes.
 * @returns {string} The formatted string.
 */
function formatBytes(bytes) {
  if (bytes < 1024) return bytes + ' B';
  const kb = bytes / 1024;
  if (kb < 1024) return kb.toFixed(2) + ' KB';
  const mb = kb / 1024;
  if (mb < 1024) return mb.toFixed(2) + ' MB';
  const gb = mb / 1024;
  return gb.toFixed(2) + ' GB';
}

/**
 * Performs a single HTTP request using the given Accept-Encoding header.
 * @param {string|undefined} acceptEncoding - The Accept-Encoding header value (or undefined for no header).
 * @returns {Promise<number>} A promise that resolves with the response size in bytes.
 */
function performRequest(acceptEncoding) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 4200,
      path: '/_sql',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Encoding': 'deflate', // The request body is compressed with deflate
        Accept: 'application/json',
      },
    };

    if (acceptEncoding) {
      options.headers['Accept-Encoding'] = acceptEncoding;
    }

    const req = http.request(options, (res) => {
      let responseSize = 0;
      res.on('data', (chunk) => {
        responseSize += chunk.length;
      });
      res.on('end', () => {
        resolve(responseSize);
      });
    });

    req.on('error', (error) => {
      reject(error);
    });

    req.write(compressedData);
    req.end();
  });
}

/**
 * Runs a batch of requests.
 * @param {string|undefined} acceptEncoding - The Accept-Encoding header value for the batch.
 * @param {number} count - Number of requests to perform.
 * @returns {Promise<{elapsedTime: number, totalResponseSize: number}>}
 */
async function runBatch(acceptEncoding, count) {
  const startTime = Date.now();
  let totalResponseSize = 0;
  for (let i = 0; i < count; i++) {
    try {
      const size = await performRequest(acceptEncoding);
      totalResponseSize += size;
    } catch (error) {
      console.error('Request error:', error);
    }
  }
  const elapsedTime = Date.now() - startTime;
  return { elapsedTime, totalResponseSize };
}

/**
 * Runs all test batches and logs the aggregated metrics.
 */
async function runTests() {
  console.log(`Running ${numRequests} requests with Accept-Encoding: gzip`);
  const gzipResults = await runBatch('gzip', numRequests);
  console.log(
    `GZIP - Total time: ${gzipResults.elapsedTime} ms, ` +
      `Average time: ${(gzipResults.elapsedTime / numRequests).toFixed(2)} ms, ` +
      `Total response size: ${formatBytes(gzipResults.totalResponseSize)}`
  );

  console.log(`Running ${numRequests} requests with Accept-Encoding: deflate`);
  const deflateResults = await runBatch('deflate', numRequests);
  console.log(
    `DEFLATE - Total time: ${deflateResults.elapsedTime} ms, ` +
      `Average time: ${(deflateResults.elapsedTime / numRequests).toFixed(2)} ms, ` +
      `Total response size: ${formatBytes(deflateResults.totalResponseSize)}`
  );

  console.log(`Running ${numRequests} requests without Accept-Encoding header`);
  const noHeaderResults = await runBatch(undefined, numRequests);
  console.log(
    `No Accept-Encoding - Total time: ${noHeaderResults.elapsedTime} ms, ` +
      `Average time: ${(noHeaderResults.elapsedTime / numRequests).toFixed(2)} ms, ` +
      `Total response size: ${formatBytes(noHeaderResults.totalResponseSize)}`
  );
}

runTests().catch((error) => {
  console.error('Error during tests:', error);
});
