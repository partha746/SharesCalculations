import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { Chart } from 'chart.js/auto';
import 'chartjs-adapter-date-fns';

import { DailyPriceHistoryResponse } from '../../../core/models/dashboard.types';
import { DashboardService } from '../../../core/services/dashboard.service';
import {
  PRICE_HISTORY_YEARS, YearOverlayChart, YearOverlayMode, YearOverlayView, buildYearSeries, setYearOverlayMode, yearOverlayConfig,
} from './year-overlay';

/** Playground card: NVDA's last five calendar years over one Jan–Dec axis, a line per year. */
@Component({
  selector: 'app-year-overlay-chart',
  imports: [],
  templateUrl: './year-overlay-chart.component.html',
  styleUrl: './year-overlay-chart.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class YearOverlayChartComponent implements OnInit, OnDestroy {
  readonly years = 5;
  history: DailyPriceHistoryResponse | null = null;
  loading = false;
  error: string | null = null;

  private readonly view: YearOverlayView = { series: [], mode: 'pct' };
  private chart: YearOverlayChart | null = null;
  private canvasEl: HTMLCanvasElement | null = null;

  /** The canvas only exists once there are lines to draw, so the chart is built when it appears. */
  @ViewChild('canvas') set canvas(ref: ElementRef<HTMLCanvasElement> | undefined) {
    const el = ref?.nativeElement ?? null;
    if (el === this.canvasEl) return;
    this.canvasEl = el;
    this.renderChart();
  }

  constructor(private dashboardService: DashboardService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.load();
  }

  ngOnDestroy(): void {
    this.chart?.destroy();
    this.chart = null;
  }

  get mode(): YearOverlayMode {
    return this.view.mode;
  }

  get hasLines(): boolean {
    return this.view.series.length > 0;
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

  load(): void {
    this.loading = true;
    this.error = null;
    this.cdr.markForCheck();
    this.dashboardService.getDailyPriceHistory(PRICE_HISTORY_YEARS).subscribe({
      next: (res) => {
        this.history = res;
        this.view.series = buildYearSeries(res, this.years);
        if (!this.hasLines) this.error = 'No NVDA closes came back for this period.';
        this.loading = false;
        this.renderChart();
        this.cdr.markForCheck();
      },
      error: (err) => {
        this.error = err?.error?.error || err?.message || 'Could not load NVDA price history';
        this.loading = false;
        this.cdr.markForCheck();
      },
    });
  }

  setMode(mode: YearOverlayMode): void {
    if (mode === this.view.mode) return;
    if (this.chart) setYearOverlayMode(this.chart, this.view, mode);
    else this.view.mode = mode;
  }

  private renderChart(): void {
    this.chart?.destroy();
    this.chart = this.canvasEl && this.hasLines ? new Chart(this.canvasEl, yearOverlayConfig(this.view)) : null;
  }
}
