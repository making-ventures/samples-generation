import { TrinoDataGenerator } from '../src/generator/index.js';

const generator = new TrinoDataGenerator({
  host: 'localhost',
  port: 8080,
  user: 'trino',
  catalog: 'iceberg',
  schema: 'warehouse',
});

await generator.connect();
console.log('\n=== employees (10 rows) ===');
const rows = await generator.queryRows('employees', 10);
console.table(rows);
await generator.disconnect();
