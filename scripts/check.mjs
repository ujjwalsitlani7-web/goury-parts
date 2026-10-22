// Syntax-checks every script in public/js. Run with: npm run check
import { readdirSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

const dir = 'public/js';
const tmp = mkdtempSync(join(tmpdir(), 'goury-'));
let failed = 0;
for (const f of readdirSync(dir).sort()) {
  // Classic scripts are checked as CommonJS, modules as ESM.
  const target = f.endsWith('.mjs') ? join(dir, f) : join(tmp, f.replace(/\.js$/, '.cjs'));
  if (!f.endsWith('.mjs')) copyFileSync(join(dir, f), target);
  try { execFileSync(process.execPath, ['--check', target], { stdio: 'pipe' }); console.log('ok   ', f); }
  catch (e) { failed++; console.error('FAIL ', f, '\n' + e.stderr.toString()); }
}
process.exit(failed ? 1 : 0);
