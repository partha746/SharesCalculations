import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { Chart } from 'chart.js/auto';
import 'chartjs-adapter-date-fns';

import { DailyPriceHistoryResponse } from '../../../core/models/dashboard.types';
import { DashboardService } from '../../../core/services/dashboard.service';
import { PRICE_HISTORY_YEARS, buildYearSeries, signedFixed } from '../year-overlay-chart/year-overlay';
import {
  PeriodPatternChart, SeasonalAnalysis, SeasonalSummary, SeasonalUnit, TypicalPath, TypicalPathChart,
  periodPatternConfig, seasonalAnalysis, seasonalSummary, typicalPath, typicalPathConfig,
} from './seasonality';

/** Playground card: whether NVDA's year so far follows the shape of its past years. */
@Component({
  selector: 'app-seasonality-chart',
  imports: [],
  templateUrl: './seasonality-chart.component.html',
  styleUrl: './seasonality-chart.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SeasonalityChartComponent implements OnInit, OnDestroy {
  unit: SeasonalUnit = 'week';
  history: DailyPriceHistoryResponse | null = null;
  path: TypicalPath | null = null;
  analysis: SeasonalAnalysis | null = null;
  summary: SeasonalSummary | null = null;
  loading = false;
  error: string | null = null;

  private pathChart: TypicalPathChart | null = null;
  private patternChart: PeriodPatternChart | null = null;
  private pathCanvasEl: HTMLCanvasElement | null = null;
  private patternCanvasEl: HTMLCanvasElement | null = null;

  /** The canvases only exist once there is something to draw, so each chart is built when its canvas appears. */
  @ViewChild('pathCanvas') set pathCanvas(ref: ElementRef<HTMLCanvasElement> | undefined) {
    const el = ref?.nativeElement ?? null;
    if (el === this.pathCanvasEl) return;
    this.pathCanvasEl = el;
    this.renderPath();
  }

  @ViewChild('patternCanvas') set patternCanvas(ref: ElementRef<HTMLCanvasElement> | undefined) {
    const el = ref?.nativeElement ?? null;
    if (el === this.patternCanvasEl) return;
    this.patternCanvasEl = el;
    this.renderPattern();
  }

  constructor(private dashboardService: DashboardService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.load();
  }

  ngOnDestroy(): void {
    this.pathChart?.destroy();
    this.patternChart?.destroy();
    this.pathChart = null;
    this.patternChart = null;
  }

  get span(): string {
    const years = this.path?.pastYears ?? [];
    return years.length ? `${years[years.length - 1]}–${years[0]}` : '';
  }

  get patternDesc(): string {
    const year = this.analysis?.currentYear;
    return this.unit === 'week'
      ? `Bars are each ISO week's median return in ${this.span}, solid where at least 3 in 4 of those years moved the same way. ` +
          `Dots are ${year}, hollow while the week is still running. Tap a week for every year's figure.`
      : `Bars are each month's median return in ${this.span}, solid where at least 3 in 4 of those years moved the same way; ` +
          `the figure above is how many of them rose. Dots are ${year}, hollow while the month is still running.`;
  }

  get lastCloseLabel(): string {
    const last = this.history?.dates[this.history.dates.length - 1];
    if (!last) return '';
    const [y, m, d] = last.split('-').map(Number);
    return new Date(y, m - 1, d).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', year: 'numeric' });
  }

  get fetchedAtLabel(): string {
    return this.history ? new Date(this.history.fetchedAt).toLocaleString('en-IN', { day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit' }) : '';
  }

  formatMatch(r: number | null): string {
    return r == null ? 'n/a' : signedFixed(r, 2);
  }

  load(): void {
    this.loading = true;
    this.error = null;
    this.cdr.markForCheck();
    this.dashboardService.getDailyPriceHistory(PRICE_HISTORY_YEARS).subscribe({
      next: (res) => {
        this.history = res;
        this.path = typicalPath(buildYearSeries(res));
        this.analyse();
        if (!this.summary) this.error = 'Not enough NVDA history came back to compare years.';
        this.loading = false;
        this.renderPath();
        this.renderPattern();
        this.cdr.markForCheck();
      },
      error: (err) => {
        this.error = err?.error?.error || err?.message || 'Could not load NVDA price history';
        this.loading = false;
        this.cdr.markForCheck();
      },
    });
  }

  setUnit(unit: SeasonalUnit): void {
    if (unit === this.unit) return;
    this.unit = unit;
    this.analyse();
    this.renderPattern();
  }

  private analyse(): void {
    this.analysis = this.history ? seasonalAnalysis(this.history, this.unit) : null;
    this.summary = this.path && this.analysis ? seasonalSummary(this.path, this.analysis) : null;
  }

  private renderPath(): void {
    this.pathChart?.destroy();
    this.pathChart = this.pathCanvasEl && this.path ? new Chart(this.pathCanvasEl, typicalPathConfig(this.path)) : null;
  }

  private renderPattern(): void {
    this.patternChart?.destroy();
    this.patternChart = this.patternCanvasEl && this.analysis ? new Chart(this.patternCanvasEl, periodPatternConfig(this.analysis)) : null;
  }
}
