'use strict';
const pg = require('pg');
const path = require('path');
const fs = require('fs');

try { process.loadEnvFile(path.join(__dirname, '.env')); } catch { /* no .env file */ }

// Ensure BIGINT (e.g. COUNT(*)) is parsed as integer number
pg.types.setTypeParser(20, (val) => (val === null ? null : parseInt(val, 10)));

const RES = {
  patients:      { cols: { name: 's!', latin_name: 's', age: 'i', age_unit: 's', dob: 's', sex: 's', phone: 's', addr: 's', type: 's', room: 's', bed: 's', unpaid: 'b', unpaid_remark: 'x', khmer: 'b', nssf: 'b', pet: 'b', follow_up: 'b', diagnosis: 's', nssf_id: 's', national_id: 's', complaints: 'x', history: 'x', reg_date: 's', photo: 's' }, search: ['name', 'latin_name', 'phone', 'addr', 'nssf_id', 'national_id', 'diagnosis'], order: 'id DESC' },
  appointments:  { cols: { name: 's!', svc: 's', date: 's', time: 's', doc: 's', phone: 's', st: 's', patient_id: 'i', type: 's', reminder_date: 's', reminder_status: 's', telesales: 's', pancake: 's', description: 'x', notes: 'x' }, search: ['name', 'svc', 'doc', 'phone', 'description', 'notes'], order: 'date DESC, id DESC' },
  ipd:           { cols: { name: 's!', code: 's', room: 's', bed: 's', paid: 'b', out: 'b' }, search: ['name', 'code', 'room', 'bed'], order: 'id DESC' },
  doctors:       { cols: { name: 's!', role: 's', code: 's!', phone: 's', av: 's', workdays: 's', photo: 's', signature: 'j', department: 's', specialist: 's', epf_no: 's', basic_salary_cents: 'i', contract_type: 's', work_location: 's', date_joined: 's' }, search: ['name', 'code', 'role'], order: 'id' },
  payslips:      { cols: { doctor_id: 'i!', period_month: 'i!', period_year: 'i!', date_start: 's!', date_end: 's!', basic_cents: 'i!', position_allowance_cents: 'i!', overtime_cents: 'i!', benefits_cents: 'i!', deduction_cents: 'i!', note: 'x' }, search: ['note'], order: 'period_year DESC, period_month DESC, id DESC' },
  leaves:        { cols: { employee_name: 's!', leave_type: 's!', start_date: 's!', end_date: 's!', reason: 'x', status: 's!' }, search: ['employee_name', 'leave_type', 'reason', 'status'], order: 'start_date DESC, id DESC' },
  expenses:      { cols: { name: 's!', category: 's', payee: 's', date: 's!', unit_price: 'i', quantity: 'i', amount: 'i!', notes: 'x' }, search: ['name', 'category', 'payee', 'notes'], order: 'date DESC, id DESC' },
  medicines:     { cols: { name: 's!', composition: 's', price: 'i', stock: 'i', photo: 's', units: 'x', default_morning: 's', default_noon: 's', default_evening: 's', default_night: 's', default_duration: 'i', default_quantity: 'i', expiration_date: 's', notification_date: 's' }, search: ['name', 'composition'], order: 'LOWER(name)' },
  services:      { cols: { name: 's!', price: 'i', description: 'x' }, search: ['name', 'description'], order: 'LOWER(name)' },
  labs:          { cols: { name: 's!', price: 'i', cat: 's', fields: 'x' }, search: ['name', 'cat'], order: 'LOWER(name)' },
  labcategories: { cols: { name: 's!' }, search: ['name'], order: 'LOWER(name)' },
  echo:          { cols: { name: 's!', price: 'i', font: 's', sections: 'x', fields: 'x' }, search: ['name'], order: 'LOWER(name)' },
  diagnoses:     { cols: { name: 's!' }, search: ['name'], order: 'LOWER(name)' },
  prescriptions: { cols: { name: 's!', meds: 'x' }, search: ['name', 'meds'], order: 'LOWER(name)' },
  symptoms:      { cols: { name: 's!' }, search: ['name'], order: 'id' },
  addresses:     { cols: { name: 's!' }, search: ['name'], order: 'LOWER(name)' },
  files:         { cols: { name: 's!', content: 'x' }, search: ['name', 'content'], order: 'id DESC' },
  records:       { cols: { patient_id: 'i', kind: 's!', title: 's', date: 's', color: 's', notes: 'x', image: 's', data: 'j', created_by: 'i' }, search: ['title', 'notes'], order: 'date DESC, id DESC' },
  chat:          { cols: { msg: 's!', user_id: 'i' }, search: ['msg'], order: 'id' },
};

const CLINIC = { cols: { name: 's', specialty: 's', address: 's', phone: 's', currency: 's', rate: 'i', logo: 's', qr: 's', banner: 's', footer_banner: 's', footer_banner_text: 'x', footer_banner_mode: 's', show_name: 'b', show_address: 'b', show_phone: 'b', print_header: 'x', print_footer: 'x', print_template: 'j', cnf_text: 'j' } };

const SQLT = { s: 'TEXT', x: 'TEXT', j: 'TEXT', i: 'INTEGER', b: 'INTEGER' };
const dflt = (k) => (k === 'i' || k === 'b' ? '0' : k === 'j' ? "'{}'" : "''");

let pool = null;
let initPromise = null;
let memoryFallbackUsed = false;
const dataDir = path.join(__dirname, 'data');
fs.mkdirSync(dataDir, { recursive: true });

function useMemoryDb() {
  memoryFallbackUsed = true;
  const { newDb, DataType } = require('pg-mem');
  const mem = newDb();
  mem.public.registerFunction({
    name: 'to_char',
    args: [DataType.timestamptz, DataType.text],
    returns: DataType.text,
    implementation: (d, f) => (d ? new Date(d).toISOString().slice(0, 10) : null),
  });
  mem.public.registerFunction({
    name: 'to_char',
    args: [DataType.timestamp, DataType.text],
    returns: DataType.text,
    implementation: (d, f) => (d ? new Date(d).toISOString().slice(0, 10) : null),
  });
  const pgAdapter = mem.adapters.createPg();
  pool = new pgAdapter.Pool();
  return pool;
}

function getPool() {
  if (!pool) {
    const explicitPgConfig = process.env.DATABASE_URL || process.env.PGHOST || process.env.PGPORT || process.env.PGUSER || process.env.PGPASSWORD !== undefined || process.env.PGDATABASE;
    if (process.env.USE_MEM_DB === 'true' || (!explicitPgConfig && process.env.USE_MEM_DB !== 'false') || (!process.env.DATABASE_URL && process.env.NODE_ENV === 'test')) {
      return useMemoryDb();
    }
    if (process.env.DATABASE_URL) {
      pool = new pg.Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: process.env.PGSSL === 'true' ? { rejectUnauthorized: false } : undefined,
      });
    } else {
      pool = new pg.Pool({
        host: process.env.PGHOST || '127.0.0.1',
        port: Number(process.env.PGPORT) || 5432,
        user: process.env.PGUSER || 'postgres',
        password: process.env.PGPASSWORD !== undefined ? process.env.PGPASSWORD : '',
        database: process.env.PGDATABASE || 'sovanvitou',
        ssl: process.env.PGSSL === 'true' ? { rejectUnauthorized: false } : undefined,
      });
    }
  }
  return pool;
}

function setPool(p, fallback = false) {
  pool = p;
  initPromise = null;
  memoryFallbackUsed = !!fallback;
}

function shouldUseMemoryFallback(error) {
  if (process.env.USE_MEM_DB === 'false') return false;
  if (!error || typeof error !== 'object') return false;
  const code = String(error.code || '').toLowerCase();
  const message = String(error.message || '').toLowerCase();
  const hints = ['connection refused', 'timeout exceeded', 'connect econnrefused', 'ecconnrefused', 'econnrefused', 'enotfound', 'econnreset', 'econnabort', 'password authentication failed', 'database does not exist', 'could not connect to server'];
  return hints.some((hint) => message.includes(hint) || code.includes(hint));
}

async function ensureDbExists() {
  if (process.env.USE_MEM_DB === 'true' || process.env.NODE_ENV === 'test') return;
  const targetDb = process.env.PGDATABASE || 'sovanvitou';
  if (targetDb === 'postgres') return;
  const config = process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL.replace(/\/[^/?]+(\?|$)/, '/postgres$1') }
    : {
        host: process.env.PGHOST || '127.0.0.1',
        port: Number(process.env.PGPORT) || 5432,
        user: process.env.PGUSER || 'postgres',
        password: process.env.PGPASSWORD !== undefined ? process.env.PGPASSWORD : '',
        ssl: process.env.PGSSL === 'true' ? { rejectUnauthorized: false } : undefined,
        database: 'postgres',
      };
  const client = new pg.Client(config);
  try {
    await client.connect();
    const res = await client.query('SELECT 1 FROM pg_database WHERE datname = $1', [targetDb]);
    if (res.rows.length === 0) {
      await client.query(`CREATE DATABASE "${targetDb.replace(/"/g, '""')}"`);
      console.log(`Database "${targetDb}" created successfully.`);
    }
    await client.end();
  } catch {
    try { await client.end(); } catch {}
  }
}

async function initializeSchema(p) {
  // 1. Create RES tables
  for (const [t, { cols }] of Object.entries(RES)) {
    const defs = Object.entries(cols).map(([c, ty]) =>
      `"${c}" ${SQLT[ty[0]]} NOT NULL DEFAULT ${dflt(ty[0])}`).join(',');
    await p.query(`CREATE TABLE IF NOT EXISTS "${t}" (
      id SERIAL PRIMARY KEY,
      ${defs},
      created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )`);
    for (const [c, ty] of Object.entries(cols)) {
      await p.query(`ALTER TABLE "${t}" ADD COLUMN IF NOT EXISTS "${c}" ${SQLT[ty[0]]} NOT NULL DEFAULT ${dflt(ty[0])}`);
    }
  }
  await p.query(`CREATE TABLE IF NOT EXISTS patient_portal_tokens (
    id SERIAL PRIMARY KEY,
    patient_id INTEGER NOT NULL REFERENCES patients(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`);
  await p.query('CREATE INDEX IF NOT EXISTS patient_portal_tokens_patient_idx ON patient_portal_tokens(patient_id)');
  await p.query('CREATE UNIQUE INDEX IF NOT EXISTS payslips_doctor_period_unique ON payslips(doctor_id, period_year, period_month)');

  // 2. Create clinic table
  await p.query(`CREATE TABLE IF NOT EXISTS clinic (
    id INTEGER PRIMARY KEY CHECK(id=1),
    name TEXT NOT NULL DEFAULT '',
    specialty TEXT NOT NULL DEFAULT '',
    address TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    currency TEXT NOT NULL DEFAULT 'KHR',
    rate INTEGER NOT NULL DEFAULT 4000,
    logo TEXT NOT NULL DEFAULT '',
    qr TEXT NOT NULL DEFAULT '',
    banner TEXT NOT NULL DEFAULT '',
    footer_banner TEXT NOT NULL DEFAULT '',
    footer_banner_text TEXT NOT NULL DEFAULT '',
    footer_banner_mode TEXT NOT NULL DEFAULT 'image',
    show_name INTEGER NOT NULL DEFAULT 1,
    show_address INTEGER NOT NULL DEFAULT 1,
    show_phone INTEGER NOT NULL DEFAULT 1,
    print_header TEXT NOT NULL DEFAULT '',
    print_footer TEXT NOT NULL DEFAULT '',
    print_template TEXT NOT NULL DEFAULT '{}',
    cnf_text TEXT NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
  )`);
  await p.query(`ALTER TABLE clinic ADD COLUMN IF NOT EXISTS print_header TEXT NOT NULL DEFAULT ''`);
  await p.query(`ALTER TABLE clinic ADD COLUMN IF NOT EXISTS print_footer TEXT NOT NULL DEFAULT ''`);
  await p.query(`ALTER TABLE clinic ADD COLUMN IF NOT EXISTS footer_banner TEXT NOT NULL DEFAULT ''`);
  await p.query(`ALTER TABLE clinic ADD COLUMN IF NOT EXISTS footer_banner_text TEXT NOT NULL DEFAULT ''`);
  await p.query(`ALTER TABLE clinic ADD COLUMN IF NOT EXISTS footer_banner_mode TEXT NOT NULL DEFAULT 'image'`);
  await p.query(`ALTER TABLE clinic ADD COLUMN IF NOT EXISTS print_template TEXT NOT NULL DEFAULT '{}'`);
  await p.query(`ALTER TABLE clinic ADD COLUMN IF NOT EXISTS cnf_text TEXT NOT NULL DEFAULT '{}'`);
  await p.query(`INSERT INTO clinic(id) VALUES(1) ON CONFLICT (id) DO NOTHING`);

  // 3. Create users, settings, sales, sale_items
  await p.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      email TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL DEFAULT '',
      role TEXT NOT NULL DEFAULT 'staff',
      permissions TEXT NOT NULL DEFAULT '{}',
      pass TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    ALTER TABLE users ADD COLUMN IF NOT EXISTS permissions TEXT NOT NULL DEFAULT '{}';
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sales (
      id SERIAL PRIMARY KEY,
      total INTEGER NOT NULL,
      method TEXT NOT NULL,
      user_id INTEGER,
      created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS sale_items (
      id SERIAL PRIMARY KEY,
      sale_id INTEGER NOT NULL REFERENCES sales(id) ON DELETE CASCADE,
      medicine_id INTEGER,
      name TEXT NOT NULL,
      qty INTEGER NOT NULL,
      price INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_sales_created ON sales(created_at);
    CREATE INDEX IF NOT EXISTS idx_appt_date ON appointments(date);
    CREATE INDEX IF NOT EXISTS idx_patients_name ON patients(name);
    CREATE INDEX IF NOT EXISTS idx_records_patient ON records(patient_id, kind);
  `);
}

async function initDb() {
  if (initPromise) return initPromise;
  initPromise = (async () => {
    try {
      await ensureDbExists();
      const p = getPool();
      await initializeSchema(p);
    } catch (error) {
      if (!memoryFallbackUsed && shouldUseMemoryFallback(error)) {
        console.warn('PostgreSQL unavailable; switching to the in-memory database fallback.');
        const fallbackPool = useMemoryDb();
        setPool(fallbackPool, true);
        await initializeSchema(fallbackPool);
        return;
      }
      throw error;
    }
  })();
  return initPromise;
}

async function query(text, params) {
  await initDb();
  const poolRef = getPool();
  try {
    return await poolRef.query(text, params);
  } catch (error) {
    if (!memoryFallbackUsed && shouldUseMemoryFallback(error)) {
      console.warn('PostgreSQL unavailable; switching to the in-memory database fallback.');
      setPool(useMemoryDb(), true);
      await initDb();
      return pool.query(text, params);
    }
    throw error;
  }
}

async function queryOne(text, params) {
  const res = await query(text, params);
  return res.rows[0] || null;
}

async function queryAll(text, params) {
  const res = await query(text, params);
  return res.rows;
}

async function tx(fn) {
  await initDb();
  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    const r = await fn(client);
    await client.query('COMMIT');
    return r;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

async function close() {
  if (pool) {
    await pool.end();
    pool = null;
    initPromise = null;
    memoryFallbackUsed = false;
  }
}

module.exports = {
  getPool,
  setPool,
  useMemoryDb,
  query,
  queryOne,
  queryAll,
  tx,
  initDb,
  close,
  RES,
  CLINIC,
  dataDir,
};
