import type { Chart, ChartConfiguration } from 'chart.js';

import type { DailyPriceHistoryResponse } from '../../../core/models/dashboard.types';
import {
  AXIS_DAYS, GRID_COLOUR, YearSeries, ZERO_LINE_COLOUR, calendarXAxis, dayLabel, pctTick, signedFixed,
} from '../year-overlay-chart/year-overlay';

export type SeasonalUnit = 'week' | 'month';

/** One week of the year (ISO, 1–52) or one month, across the past years and the current one. */
export interface PeriodStats {
  period: number;
  label: string;
  /** Median of the past years' returns for this period, in percent. */
  median: number | null;
  mean: number | null;
  /** Past years in which the period closed higher than the one before it. */
  up: number;
  n: number;
  /** Each past year's return for the period, newest first. */
  byYear: { year: number; pct: number }[];
  /** The current year's return, once the period has started. */
  current: number | null;
  currentIsPartial: boolean;
}

export interface SeasonalAnalysis {
  unit: SeasonalUnit;
  currentYear: number;
  /** Newest first. */
  pastYears: number[];
  periods: PeriodStats[];
  /** Correlation of the current year's finished periods with the past years' medians. */
  currentMatch: number | null;
  /** Each past year against the median of the other past years, newest first. */
  pastMatches: { year: number; r: number | null }[];
  pastMatchAvg: number | null;
}

/** The past years' change since the previous year-end on each calendar day, against the current year's. */
export interface TypicalPath {
  currentYear: number;
  /** Newest first. */
  pastYears: number[];
  p25: number[];
  median: number[];
  p75: number[];
  /** Up to the current year's latest close. */
  current: number[];
  /** Past years the current year is ahead of at its latest close. */
  aheadOf: number;
}

export interface SeasonalSummary {
  level: string;
  pattern: string;
  consistent: string;
}

type Point = { x: number; y: number };
export type TypicalPathChart = Chart<'line', Point[]>;
export type PeriodPatternChart = Chart<'bar' | 'line', (number | null)[], string>;

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const CURRENT_COLOUR = '#58a6ff';
const UP_RGB = '63, 185, 80';
const DOWN_RGB = '248, 81, 73';
/** Share of past years that must agree on a period's direction for it to count as consistent. */
const CONSISTENT_SHARE = 0.75;
const DAY_MS = 86400000;

const pct = (v: number) => `${signedFixed(v, 1)}%`;
const mean = (xs: number[]) => xs.reduce((a, b) => a + b, 0) / xs.length;

/** Linear interpolation between order statistics, so the median of an even count is the midpoint. */
function quantile(sorted: number[], q: number): number {
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  return sorted[lo] + (sorted[Math.ceil(pos)] - sorted[lo]) * (pos - lo);
}

const median = (xs: number[]) => quantile([...xs].sort((a, b) => a - b), 0.5);

/** Chance that n independent years, each up with probability p, agree on direction in at least `share` of them. */
function oneSidedChance(n: number, p: number, share: number): number {
  const need = Math.ceil(share * n);
  let total = 0;
  let ways = 1;
  for (let k = 0; k <= n; k++) {
    if (k > 0) ways = (ways * (n - k + 1)) / k;
    if (k >= need || n - k >= need) total += ways * p ** k * (1 - p) ** (n - k);
  }
  return total;
}

function pearson(xs: number[], ys: number[]): number | null {
  if (xs.length < 3) return null;
  const mx = mean(xs);
  const my = mean(ys);
  let sxy = 0;
  let sxx = 0;
  let syy = 0;
  xs.forEach((x, i) => {
    sxy += (x - mx) * (ys[i] - my);
    sxx += (x - mx) ** 2;
    syy += (ys[i] - my) ** 2;
  });
  return sxx > 0 && syy > 0 ? sxy / Math.sqrt(sxx * syy) : null;
}

/** ISO 8601 week-numbering year and week: weeks run Monday–Sunday, and week 1 holds the year's first Thursday. */
function isoWeek(y: number, m: number, d: number): [number, number] {
  const thursday = new Date(Date.UTC(y, m - 1, d));
  thursday.setUTCDate(thursday.getUTCDate() + 4 - (thursday.getUTCDay() || 7));
  const weekYear = thursday.getUTCFullYear();
  return [weekYear, Math.ceil(((thursday.getTime() - Date.UTC(weekYear, 0, 1)) / DAY_MS + 1) / 7)];
}

/** Monday of an ISO week, at UTC midnight. */
function isoWeekMonday(weekYear: number, week: number): Date {
  const day = new Date(Date.UTC(weekYear, 0, 4));
  day.setUTCDate(day.getUTCDate() - ((day.getUTCDay() || 7) - 1) + (week - 1) * 7);
  return day;
}

const addDays = (t: Date, days: number) => new Date(t.getTime() + days * DAY_MS);
const utcDayLabel = (t: Date) => t.toLocaleDateString('en-IN', { day: 'numeric', month: 'short', timeZone: 'UTC' });

/** Month (0–11) that holds most of an ISO week. */
const weekMonth = (weekYear: number, week: number) => addDays(isoWeekMonday(weekYear, week), 3).getUTCMonth();

/** Last weekday (YYYY-MM-DD) of a week or month: after it, that period's close is final. */
function lastWeekday(unit: SeasonalUnit, year: number, period: number): string {
  let end = unit === 'week' ? addDays(isoWeekMonday(year, period), 4) : new Date(Date.UTC(year, period, 0));
  const dow = end.getUTCDay();
  if (dow === 0 || dow === 6) end = addDays(end, dow === 0 ? -2 : -1);
  return end.toISOString().slice(0, 10);
}

/** Today in New York, where the trading day is defined. */
const etToday = () => new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });

/**
 * Return of every week (or month) of every year, and how each period behaved across the
 * past years. The current year is the one holding the latest close; its period still in
 * progress is shown but left out of the match, since its return is not final.
 */
export function seasonalAnalysis(history: DailyPriceHistoryResponse, unit: SeasonalUnit, today = etToday()): SeasonalAnalysis | null {
  // Close at the end of each week or month, in date order.
  const ends: { year: number; period: number; close: number }[] = [];
  history.dates.forEach((date, i) => {
    const [y, m, d] = date.split('-').map(Number);
    const [year, period] = unit === 'week' ? isoWeek(y, m, d) : [y, m];
    const last = ends[ends.length - 1];
    if (last && last.year === year && last.period === period) last.close = history.closes[i];
    else ends.push({ year, period, close: history.closes[i] });
  });
  if (ends.length < 2) return null;
  const returns = new Map<string, number>();
  for (let k = 1; k < ends.length; k++) {
    returns.set(`${ends[k].year}-${ends[k].period}`, (ends[k].close / ends[k - 1].close - 1) * 100);
  }
  const latest = ends[ends.length - 1];
  const currentYear = latest.year;
  const pastYears: number[] = [];
  for (let y = currentYear - 1; y >= history.firstYear; y--) pastYears.push(y);
  if (!pastYears.length) return null;
  const latestIsPartial = today <= lastWeekday(unit, latest.year, latest.period);

  // ISO week 53 only exists in some years, too few to say anything about.
  const periods: PeriodStats[] = [];
  for (let period = 1; period <= (unit === 'week' ? 52 : 12); period++) {
    const byYear = pastYears.flatMap((year) => {
      const r = returns.get(`${year}-${period}`);
      return r == null ? [] : [{ year, pct: r }];
    });
    const pcts = byYear.map((v) => v.pct);
    periods.push({
      period,
      label: unit === 'week' ? `W${period}` : MONTHS[period - 1],
      median: pcts.length ? median(pcts) : null,
      mean: pcts.length ? mean(pcts) : null,
      up: pcts.filter((v) => v > 0).length,
      n: pcts.length,
      byYear,
      current: returns.get(`${currentYear}-${period}`) ?? null,
      currentIsPartial: period === latest.period && latestIsPartial,
    });
  }

  const xs: number[] = [];
  const ys: number[] = [];
  for (const p of periods) {
    if (p.current != null && !p.currentIsPartial && p.median != null) {
      xs.push(p.current);
      ys.push(p.median);
    }
  }
  const pastMatches = pastYears.map((year) => {
    const own: number[] = [];
    const others: number[] = [];
    for (const p of periods) {
      const mine = p.byYear.find((v) => v.year === year);
      const rest = p.byYear.filter((v) => v.year !== year).map((v) => v.pct);
      if (mine && rest.length) {
        own.push(mine.pct);
        others.push(median(rest));
      }
    }
    return { year, r: pearson(own, others) };
  });
  const rs = pastMatches.flatMap((m) => (m.r == null ? [] : [m.r]));
  return { unit, currentYear, pastYears, periods, currentMatch: pearson(xs, ys), pastMatches, pastMatchAvg: rs.length ? mean(rs) : null };
}

/** `series` is newest first, as buildYearSeries returns it; the first one is the year compared. */
export function typicalPath(series: YearSeries[]): TypicalPath | null {
  const [current, ...past] = series;
  if (!current || past.length < 2) return null;
  const p25: number[] = [];
  const med: number[] = [];
  const p75: number[] = [];
  AXIS_DAYS.forEach((_, i) => {
    const vals = past.map((s) => s.pcts[i]).filter(Number.isFinite).sort((a, b) => a - b);
    p25.push(quantile(vals, 0.25));
    med.push(quantile(vals, 0.5));
    p75.push(quantile(vals, 0.75));
  });
  const last = current.pcts.length - 1;
  return {
    currentYear: current.year,
    pastYears: past.map((s) => s.year),
    p25,
    median: med,
    p75,
    current: current.pcts,
    aheadOf: past.filter((s) => s.pcts[last] < current.pcts[last]).length,
  };
}

const yearSpan = (years: number[]) => `${years[years.length - 1]}–${years[0]}`;

function periodName(a: SeasonalAnalysis, p: PeriodStats): string {
  return a.unit === 'week' ? `W${p.period} (${MONTHS[weekMonth(a.currentYear, p.period)]})` : p.label;
}

export function seasonalSummary(tp: TypicalPath, a: SeasonalAnalysis): SeasonalSummary {
  const span = yearSpan(tp.pastYears);
  const i = tp.current.length - 1;
  const level =
    `By ${dayLabel(new Date(AXIS_DAYS[i].x), false)}, ${tp.currentYear} is ${pct(tp.current[i])} for the year. ` +
    `The typical (median) year in ${span} was ${pct(tp.median[i])} by then, with the middle half between ` +
    `${pct(tp.p25[i])} and ${pct(tp.p75[i])}; ${tp.currentYear} is ahead of ${tp.aheadOf} of those ${tp.pastYears.length} years.`;

  const unit = a.unit;
  let pattern: string;
  if (a.currentMatch == null || a.pastMatchAvg == null) {
    pattern = `Not enough finished ${unit}s in ${a.currentYear} yet to compare with past years.`;
  } else {
    const strength = a.pastMatchAvg >= 0.5 ? 'strong' : a.pastMatchAvg >= 0.2 ? 'loose' : 'weak';
    const follows = a.currentMatch >= 0.3 ? 'is following it' : a.currentMatch > -0.3 ? 'is not following it' : 'is moving against it';
    pattern =
      `${unit === 'week' ? 'Week-to-week' : 'Month-to-month'} pattern match: ${signedFixed(a.currentMatch, 2)} for ${a.currentYear}, ` +
      `against ${signedFixed(a.pastMatchAvg, 2)} on average when each past year is compared with the others ` +
      `(1 = moves in step, 0 = unrelated). NVDA's ${unit}ly rhythm has been ${strength}, and ${a.currentYear} ${follows}.`;
  }

  const agreement = (p: PeriodStats) => Math.max(p.up, p.n - p.up) / p.n;
  const counted = a.periods.filter((p) => p.n > 0);
  const oneSided = counted.filter((p) => agreement(p) >= CONSISTENT_SHARE);
  const steady = [...oneSided]
    .sort((x, y) => agreement(y) - agreement(x) || Math.abs(y.median ?? 0) - Math.abs(x.median ?? 0))
    .slice(0, 4)
    .map((p) => `${periodName(a, p)} ${p.up * 2 >= p.n ? 'up' : 'down'} in ${Math.max(p.up, p.n - p.up)} of ${p.n}`);
  // With an upward drift, plenty of periods look one-sided by luck; this says how many to expect.
  const upRate = counted.reduce((s, p) => s + p.up, 0) / counted.reduce((s, p) => s + p.n, 0);
  const byLuck = counted.reduce((s, p) => s + oneSidedChance(p.n, upRate, CONSISTENT_SHARE), 0);
  const beatsLuck = oneSided.length > 1.5 * byLuck;
  const consistent = steady.length
    ? `Most consistent ${unit}s: ${steady.join(' · ')}. ${oneSided.length} of ${counted.length} ${unit}s were this one-sided, ` +
      (beatsLuck ? `more than the ${Math.round(byLuck)} or so that luck alone would give` : `and about ${Math.round(byLuck)} would be by luck alone`) +
      ` (NVDA rose in ${Math.round(upRate * 100)}% of ${unit}s overall)${beatsLuck ? '.' : ', so treat these as hints rather than rules.'}`
    : `No ${unit} moved the same way in at least 3 of 4 past years.`;
  return { level, pattern, consistent };
}

const linePoints = (ys: number[]): Point[] => ys.map((y, i) => ({ x: AXIS_DAYS[i].x, y }));

export function typicalPathConfig(tp: TypicalPath): ChartConfiguration<'line', Point[]> {
  const span = yearSpan(tp.pastYears);
  const band = { borderColor: 'transparent', borderWidth: 0, pointRadius: 0, pointHoverRadius: 0, tension: 0 };
  return {
    type: 'line',
    data: {
      datasets: [
        {
          label: String(tp.currentYear),
          data: linePoints(tp.current),
          borderColor: CURRENT_COLOUR,
          backgroundColor: CURRENT_COLOUR,
          borderWidth: 2.5,
          pointRadius: 0,
          pointHoverRadius: 3,
          tension: 0,
        },
        {
          label: `Typical year (median of ${span})`,
          data: linePoints(tp.median),
          borderColor: '#c9d1d9',
          backgroundColor: '#c9d1d9',
          borderWidth: 1.5,
          borderDash: [5, 4],
          pointRadius: 0,
          pointHoverRadius: 3,
          tension: 0,
        },
        // The band is the area between these two: the upper edge fills down to the next dataset.
        { ...band, label: `Middle half of ${span}`, data: linePoints(tp.p75), backgroundColor: 'rgba(139, 148, 158, 0.2)', fill: '+1' },
        { ...band, label: '', data: linePoints(tp.p25), fill: false },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 300 },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#c9d1d9', boxWidth: 14, boxHeight: 8, padding: 12, filter: (item) => item.datasetIndex !== 3 } },
        datalabels: { display: false },
        tooltip: {
          filter: (item) => item.datasetIndex !== 3,
          callbacks: {
            title: (items) => (items.length ? dayLabel(new Date(AXIS_DAYS[items[0].dataIndex].x), false) : ''),
            label: (item) => {
              const i = item.dataIndex;
              if (item.datasetIndex === 0) return `${tp.currentYear}: ${pct(tp.current[i])}`;
              if (item.datasetIndex === 1) return `Typical: ${pct(tp.median[i])}`;
              return `Middle half: ${pct(tp.p25[i])} to ${pct(tp.p75[i])}`;
            },
          },
        },
      },
      scales: {
        x: calendarXAxis(),
        y: {
          grace: '5%',
          ticks: { color: '#8b949e', callback: pctTick },
          grid: { color: (ctx) => (ctx.tick?.value === 0 ? ZERO_LINE_COLOUR : GRID_COLOUR) },
        },
      },
    },
  };
}

export function periodPatternConfig(a: SeasonalAnalysis): ChartConfiguration<'bar' | 'line', (number | null)[], string> {
  const weekly = a.unit === 'week';
  const typicalLabel = `Typical ${yearSpan(a.pastYears)} (median)`;
  const agreement = (p: PeriodStats) => (p.n ? Math.max(p.up, p.n - p.up) / p.n : 0);
  const monthOf = a.periods.map((p) => (weekly ? weekMonth(a.currentYear, p.period) : p.period - 1));
  const startsMonth = (i: number) => i === 0 || monthOf[i] !== monthOf[i - 1];
  return {
    type: 'bar',
    data: {
      labels: a.periods.map((p) => p.label),
      datasets: [
        {
          type: 'line',
          label: String(a.currentYear),
          data: a.periods.map((p) => p.current),
          showLine: false,
          borderColor: CURRENT_COLOUR,
          backgroundColor: CURRENT_COLOUR,
          pointBackgroundColor: a.periods.map((p) => (p.currentIsPartial ? 'transparent' : CURRENT_COLOUR)),
          pointBorderWidth: 1.5,
          pointRadius: weekly ? 3 : 4.5,
          pointHoverRadius: weekly ? 5 : 6,
          datalabels: { display: false },
        },
        {
          type: 'bar',
          label: typicalLabel,
          data: a.periods.map((p) => p.median),
          backgroundColor: a.periods.map(
            (p) => `rgba(${(p.median ?? 0) >= 0 ? UP_RGB : DOWN_RGB}, ${agreement(p) >= CONSISTENT_SHARE ? 0.85 : 0.35})`,
          ),
          borderRadius: 2,
          datalabels: weekly
            ? { display: false }
            : {
                anchor: 'end',
                align: 'end',
                offset: 2,
                color: '#8b949e',
                font: { size: 10 },
                formatter: (_v, ctx) => `${a.periods[ctx.dataIndex].up}/${a.periods[ctx.dataIndex].n}`,
              },
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 300 },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          labels: {
            color: '#c9d1d9',
            usePointStyle: true,
            boxHeight: 7,
            padding: 12,
            // The bars have one colour each, which leaves the default swatch blank.
            generateLabels: (chart) => [
              { text: String(a.currentYear), fillStyle: CURRENT_COLOUR, strokeStyle: CURRENT_COLOUR, pointStyle: 'circle', fontColor: '#c9d1d9', datasetIndex: 0, hidden: !chart.isDatasetVisible(0) },
              { text: typicalLabel, fillStyle: `rgba(${UP_RGB}, 0.85)`, strokeStyle: `rgba(${UP_RGB}, 0.85)`, pointStyle: 'rect', fontColor: '#c9d1d9', datasetIndex: 1, hidden: !chart.isDatasetVisible(1) },
            ],
          },
        },
        tooltip: {
          callbacks: {
            title: (items) => {
              const p = a.periods[items[0]?.dataIndex ?? 0];
              if (!weekly) return new Date(Date.UTC(2000, p.period - 1, 1)).toLocaleDateString('en-IN', { month: 'long', timeZone: 'UTC' });
              const monday = isoWeekMonday(a.currentYear, p.period);
              return `Week ${p.period} (${utcDayLabel(monday)} – ${utcDayLabel(addDays(monday, 4))} in ${a.currentYear})`;
            },
            label: (item) => {
              const p = a.periods[item.dataIndex];
              if (item.datasetIndex === 0) return `${a.currentYear}: ${pct(p.current ?? 0)}${p.currentIsPartial ? ' so far' : ''}`;
              return `Typical: ${pct(p.median ?? 0)} (mean ${pct(p.mean ?? 0)}), up in ${p.up} of ${p.n} years`;
            },
            afterBody: (items) => {
              const cells = a.periods[items[0]?.dataIndex ?? 0].byYear.map((v) => `${v.year} ${pct(v.pct)}`);
              const lines: string[] = [];
              for (let k = 0; k < cells.length; k += 3) lines.push(cells.slice(k, k + 3).join('   '));
              return lines.length ? ['', ...lines] : [];
            },
          },
        },
      },
      scales: {
        x: {
          ticks: {
            color: '#8b949e',
            autoSkip: false,
            maxRotation: 0,
            // A narrow weekly chart only has room to name every other month.
            callback(_v, i) {
              if (!startsMonth(i)) return '';
              return weekly && this.width < 480 && monthOf[i] % 2 ? '' : MONTHS[monthOf[i]];
            },
          },
          grid: { color: (ctx) => (startsMonth(ctx.index) ? GRID_COLOUR : 'transparent') },
        },
        y: {
          grace: '8%',
          ticks: { color: '#8b949e', callback: pctTick },
          grid: { color: (ctx) => (ctx.tick?.value === 0 ? ZERO_LINE_COLOUR : GRID_COLOUR) },
        },
      },
    },
  };
}
