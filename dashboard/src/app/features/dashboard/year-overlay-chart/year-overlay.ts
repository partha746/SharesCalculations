import type { Chart, ChartConfiguration } from 'chart.js';

import type { DailyPriceHistoryResponse } from '../../../core/models/dashboard.types';

export type YearOverlayMode = 'pct' | 'price';

/** One calendar year on the shared Jan–Dec axis: index i is the same calendar day in every year. */
export interface YearSeries {
  year: number;
  /** Close in effect that day, i.e. the latest close on or before it. */
  closes: number[];
  /** Each close against the previous year's last close, in percent. */
  pcts: number[];
  /** Trading day each close comes from (YYYY-MM-DD). */
  asOf: string[];
  /** Change over the whole year, or so far for the current one. */
  changePct: number;
  isCurrentYear: boolean;
}

/** What the chart's callbacks read, so switching mode only has to swap the point arrays. */
export interface YearOverlayView {
  /** Newest year first: it is drawn on top and leads the legend and tooltip. */
  series: YearSeries[];
  mode: YearOverlayMode;
}

type Point = { x: number; y: number };
export type YearOverlayChart = Chart<'line', Point[]>;

/** Both Playground charts ask for this much history, so they share the backend's cached copy. */
export const PRICE_HISTORY_YEARS = 10;

/** Leap year every series is drawn in, so that 29 February has a slot of its own. */
const AXIS_YEAR = 2000;
const pad2 = (n: number) => String(n).padStart(2, '0');

/** Every calendar day on the axis: its x position (local midnight) and MM-DD. */
export const AXIS_DAYS: { x: number; mmdd: string }[] = [];
for (const d = new Date(AXIS_YEAR, 0, 1); d.getFullYear() === AXIS_YEAR; d.setDate(d.getDate() + 1)) {
  AXIS_DAYS.push({ x: d.getTime(), mmdd: `${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}` });
}

/** The latest year takes the accent blue the rest of the dashboard uses. */
const YEAR_COLOURS = ['#58a6ff', '#3fb950', '#d29922', '#bc8cff', '#f778ba', '#39c5cf', '#ff7b72', '#e3b341', '#a5d6ff', '#7ee787'];
export const GRID_COLOUR = '#21262d';
export const ZERO_LINE_COLOUR = '#6e7681';

/** y-axis tick for a percentage scale. */
export const pctTick = (v: number | string) => `${Number(v) > 0 ? '+' : ''}${v}%`;

export function signedFixed(v: number, digits: number): string {
  const s = v.toFixed(digits);
  return Number(s) > 0 ? `+${s}` : Number(s) === 0 ? (0).toFixed(digits) : s;
}

export function dayLabel(date: Date, withYear: boolean): string {
  return date.toLocaleDateString('en-IN', { day: 'numeric', month: 'short', ...(withYear ? { year: 'numeric' } : {}) });
}

/** The Jan–Dec x axis, one tick per month. */
export function calendarXAxis() {
  return {
    type: 'time' as const,
    min: AXIS_DAYS[0].x,
    max: AXIS_DAYS[AXIS_DAYS.length - 1].x,
    time: { unit: 'month' as const, displayFormats: { month: 'MMM' } },
    ticks: { color: '#8b949e', maxRotation: 0 },
    grid: { color: GRID_COLOUR },
  };
}

/**
 * Splits daily closes into calendar years laid over one Jan–Dec axis. A weekend or holiday
 * carries the previous close, so every year has a value on every calendar day and the
 * tooltip can line the years up. The current year stops at its latest close.
 * `years` keeps only that many of the most recent calendar years.
 */
export function buildYearSeries(history: DailyPriceHistoryResponse, years?: number, today = new Date()): YearSeries[] {
  const { dates, closes } = history;
  const n = dates.length;
  if (n === 0) return [];
  const lastDate = dates[n - 1];
  const firstYear = years ? Math.max(history.firstYear, Number(lastDate.slice(0, 4)) - years + 1) : history.firstYear;
  let j = dates.findIndex((d) => Number(d.slice(0, 4)) >= firstYear);
  if (j < 0) return [];
  // The close in effect as a year starts. When the data begins inside the first year,
  // that year is measured from its own first close instead.
  let asOf = dates[Math.max(0, j - 1)];
  let close = closes[Math.max(0, j - 1)];
  const series: YearSeries[] = [];
  for (let year = firstYear; year <= Number(lastDate.slice(0, 4)); year++) {
    const base = close;
    const s: YearSeries = { year, closes: [], pcts: [], asOf: [], changePct: 0, isCurrentYear: year === today.getFullYear() };
    for (const day of AXIS_DAYS) {
      // Plain string order works for 29 Feb in a non-leap year too: it carries 28 Feb.
      const key = `${year}-${day.mmdd}`;
      if (key > lastDate) break;
      for (; j < n && dates[j] <= key; j++) {
        asOf = dates[j];
        close = closes[j];
      }
      s.closes.push(close);
      s.pcts.push((close / base - 1) * 100);
      s.asOf.push(asOf);
    }
    s.changePct = s.pcts[s.pcts.length - 1];
    series.push(s);
  }
  return series.reverse();
}

export function yearOverlayPoints(s: YearSeries, mode: YearOverlayMode): Point[] {
  return (mode === 'pct' ? s.pcts : s.closes).map((y, i) => ({ x: AXIS_DAYS[i].x, y }));
}

export function yearOverlayConfig(view: YearOverlayView): ChartConfiguration<'line', Point[]> {
  return {
    type: 'line',
    data: {
      datasets: view.series.map((s, k) => ({
        label: `${s.year}${s.isCurrentYear ? ' YTD' : ''} ${signedFixed(s.changePct, 1)}%`,
        data: yearOverlayPoints(s, view.mode),
        borderColor: YEAR_COLOURS[k % YEAR_COLOURS.length],
        backgroundColor: YEAR_COLOURS[k % YEAR_COLOURS.length],
        borderWidth: k === 0 ? 2.5 : 1.5,
        pointRadius: 0,
        pointHoverRadius: 3,
        tension: 0,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 300 },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#c9d1d9', boxWidth: 14, boxHeight: 3, padding: 12 } },
        datalabels: { display: false },
        tooltip: {
          callbacks: {
            title: (items) => (items.length ? dayLabel(new Date(AXIS_DAYS[items[0].dataIndex].x), false) : ''),
            label: (item) => {
              const s = view.series[item.datasetIndex];
              const i = item.dataIndex;
              const asOf = s.asOf[i];
              const [y, m, d] = asOf.split('-').map(Number);
              // A weekend or holiday shows the close it carried, and says which day that was.
              const from = asOf.slice(5) === AXIS_DAYS[i].mmdd ? '' : ` · ${dayLabel(new Date(y, m - 1, d), y !== s.year)}`;
              return `${s.year}: $${s.closes[i].toFixed(2)} (${signedFixed(s.pcts[i], 1)}%)${from}`;
            },
          },
        },
      },
      scales: {
        x: calendarXAxis(),
        y: {
          grace: '5%',
          ticks: {
            color: '#8b949e',
            callback: (v) => (view.mode === 'pct' ? pctTick(v) : `$${v}`),
          },
          grid: { color: (ctx) => (view.mode === 'pct' && ctx.tick?.value === 0 ? ZERO_LINE_COLOUR : GRID_COLOUR) },
        },
      },
    },
  };
}

/** Swaps every line between % change and price in place, keeping years hidden from the legend hidden. */
export function setYearOverlayMode(chart: YearOverlayChart, view: YearOverlayView, mode: YearOverlayMode): void {
  view.mode = mode;
  chart.data.datasets.forEach((ds, k) => {
    ds.data = yearOverlayPoints(view.series[k], mode);
  });
  chart.update();
}
