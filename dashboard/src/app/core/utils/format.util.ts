/**
 * Pure formatting helpers shared across the dashboard (currency, dates, CSV).
 * Kept stateless so any component/service can use them without DI.
 */

/** INR with grouping, no decimals (e.g. 13,52,10,684). */
export function formatInr(n: number): string {
  return new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0, minimumFractionDigits: 0 }).format(n);
}

/** USD with up to 6 decimals, no trailing zeros. */
export function formatUsd(n: number): string {
  return new Intl.NumberFormat('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 6 }).format(n);
}

/** USD rounded to exactly 1 decimal (used for average summaries). */
export function formatUsd1(n: number): string {
  return new Intl.NumberFormat('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(n);
}

/** USD->INR spot rate: 2-4 decimals so small feed moves are visible. */
export function formatUsdInrRate(n: number): string {
  return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 }).format(n);
}

/** RSU label normalization (DB stores NSU). */
export function typeLabel(type: string): string {
  return type === 'NSU' ? 'RSU' : type;
}

/** Display date dd/mm/yyyy (en-IN). Returns input unchanged if unparseable. */
export function formatDate(isoDate: string): string {
  if (!isoDate) return '';
  const d = new Date(isoDate);
  return isNaN(d.getTime()) ? isoDate : d.toLocaleDateString('en-IN', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

/** ISO date (YYYY-MM-DD) for CSV export so spreadsheets parse it as a real date. */
export function formatDateForExport(value: string): string {
  const s = String(value ?? '').trim();
  if (!s) return '';
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  const y = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, '0');
  const da = String(d.getDate()).padStart(2, '0');
  return `${y}-${mo}-${da}`;
}

/** Quote a CSV cell when it contains delimiters/quotes/newlines. */
export function escapeCsvCell(s: string): string {
  const str = String(s ?? '');
  if (/[",\r\n]/.test(str)) return '"' + str.replace(/"/g, '""') + '"';
  return str;
}

/** Plain number for CSV export: no commas, no currency symbols. */
export function formatForExport(value: number, decimals = 2): string {
  const n = Number(value);
  return isNaN(n) ? '' : n.toFixed(decimals);
}
