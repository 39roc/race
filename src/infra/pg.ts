import { Pool } from 'pg';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

export function makePool(url: string): Pool {
  return new Pool({ connectionString: url, max: 50 });
}

export async function migrate(pool: Pool): Promise<void> {
  const sql = readFileSync(
    resolve(__dirname, '../../db/migrations/001_init.sql'),
    'utf8',
  );
  await pool.query(sql);
}

export async function seedEvent(
  pool: Pool,
  id: string,
  stock: number,
): Promise<void> {
  await pool.query(
    `INSERT INTO events (id, name, stock, version) VALUES ($1, $1, $2, 0)
     ON CONFLICT (id) DO UPDATE SET stock = $2, version = 0`,
    [id, stock],
  );
}
