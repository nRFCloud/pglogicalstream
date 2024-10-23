import pg from 'pg';

// Concurrent worker code
async function doWork(iterator, mapper, resultOutput) {
  for (const [index, item] of iterator) {
    try {
      resultOutput[index] = { ok: true, value: await mapper(item) };
    } catch (error) {
      resultOutput[index] = { ok: false, error };
    }
  }
}

async function concurrentMap(items, fn, concurrency = 10) {
  const iterator = items.entries();
  const results = Array.from({ length: items.length });
  const workers = [];
  for (let i = 0; i < Math.min(concurrency, items.length); i++) {
    workers.push(doWork(iterator, fn, results));
  }
  await Promise.all(workers);
  return results;
}

// Main script
const poolSize = 10; // Configurable pool size
const numQueries = 1000; // Number of queries to run

const pool = new pg.Pool({
  user: 'john',
  host: 'default-best-cluster.brumby-dragon.ts.net',
  database: 'postgres',
  password: 'eGqpHPBXAPkNBrTovdMWwXghhht4g0SfrqdJmzmzmotOGCAFEAZqi7NA4Fv4Txhl',
  port: 5432,
  max: poolSize,
});

const query = `
  INSERT INTO device (tenant_id, device_id, name, data) 
  VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), '{"key": "value"}');
`;

async function runQuery() {
  const client = await pool.connect();
  try {
    await client.query(query);
  } finally {
    client.release();
  }
}

async function main() {
  const startTime = Date.now();

  const queries = Array(numQueries).fill(null);
  const results = await concurrentMap(queries, runQuery, poolSize);

  const endTime = Date.now();
  const duration = (endTime - startTime) / 1000;

  const successCount = results.filter(result => result.ok).length;
  const errorCount = results.filter(result => !result.ok).length;

  console.log(`Completed ${numQueries} queries in ${duration} seconds`);
  console.log(`Successful queries: ${successCount}`);
  console.log(`Failed queries: ${errorCount}`);

  await pool.end();
}

await main();