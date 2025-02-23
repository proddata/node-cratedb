import http from 'http';
import zlib from 'zlib';

const requestData = JSON.stringify({
  stmt: 'SELECT * FROM sys.summits',
});

// Compress request data using gzip
zlib.deflate(requestData, (err, compressedData) => {
  if (err) {
    console.error('Compression failed:', err);
    return;
  }

  const options = {
    hostname: 'localhost',
    port: 4200,
    path: '/_sql',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Encoding': 'deflate', // Tell CrateDB the request is compressed
      Accept: 'application/json',
      'Accept-Encoding': 'gzip',
    },
  };

  const req = http.request(options, (res) => {
    let responseData = '';

    res.on('data', (chunk) => {
      responseData += chunk;
    });

    res.on('end', () => {
      console.log('Response:', responseData);
    });
  });

  req.on('error', (error) => {
    console.error('Request error:', error);
  });

  req.write(compressedData); // Send gzipped request body
  req.end();
});
