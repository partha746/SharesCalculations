import { AfterViewInit, ChangeDetectorRef, Component, ElementRef, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Chart } from 'chart.js/auto';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import { DashboardService } from '../../core/services/dashboard.service';

Chart.register(ChartDataLabels);
import { DashboardResponse, HoldingRow, SoldRow } from '../../core/models/dashboard.types';

/** Recommendation row: lot with tax at simulation target price and qty to sell (may be partial). */
export interface PlayTaxRecommendationRow extends HoldingRow {
  /** Tax to pay (INR) for the recommended qty at simulation target price */
  taxToPayAtTargetInr: number;
  /** Number of shares to sell from this lot (≤ row.qty when capped by simulation) */
  qtyToSell: number;
}

const SOLD_COLS: (keyof SoldRow)[] = ['sellDate', 'buyDate', 'type', 'qtySold', 'priceBoughtUsd', 'priceSellUsd', 'buyValueInr', 'sellValueInr', 'gainBeforeTaxInr', 'taxPaidInr', 'profitPercent', 'taxPercent'];
const SOLD_DATE_COLS: (keyof SoldRow)[] = ['sellDate', 'buyDate'];
const SOLD_COL_LABELS: Record<keyof SoldRow, string> = {
  sellDate: 'Sell date',
  buyDate: 'Buy date',
  type: 'Type',
  qtySold: 'Qty sold',
  priceBoughtUsd: 'Price bought (USD)',
  priceSellUsd: 'Price sell (USD)',
  buyValueInr: 'Buy value (INR)',
  sellValueInr: 'Sell value (INR)',
  gainBeforeTaxInr: 'Gain before tax (INR)',
  taxPaidInr: 'Tax paid (INR)',
  profitPercent: 'Profit %',
  taxPercent: 'Tax %',
};
import { StatCardComponent } from './stat-card/stat-card.component';

const HOLDING_COLS = ['buyPriceUsd', 'type', 'totalPurchaseInr', 'netIfSellTodayInr', 'buyDate', 'qty', 'profitPercent', 'taxToPayInr', 'taxPercent'] as const;
const DATE_COLS = ['buyDate'];
const HOLDING_COL_DEFS: { key: (typeof HOLDING_COLS)[number]; label: string }[] = [
  { key: 'buyPriceUsd', label: 'Buy price (USD)' },
  { key: 'type', label: 'Type' },
  { key: 'totalPurchaseInr', label: 'Total purchase (INR)' },
  { key: 'netIfSellTodayInr', label: 'If you sell this lot today (INR)' },
  { key: 'buyDate', label: 'Date bought' },
  { key: 'qty', label: 'Qty' },
  { key: 'profitPercent', label: 'Profit %' },
  { key: 'taxToPayInr', label: 'Tax to pay (INR)' },
  { key: 'taxPercent', label: 'Tax %' },
];
const HOLDINGS_COLUMN_ORDER_STORAGE_KEY = 'dashboard.holdingsColumnOrder';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule, FormsModule, StatCardComponent],
  templateUrl: './dashboard.component.html',
  styleUrl: './dashboard.component.scss',
})
export class DashboardComponent implements OnInit, AfterViewInit, OnDestroy {
  data: DashboardResponse | null = null;
  /** When dashboard data was last fetched (for display in Live price block) */
  lastRefreshedAt: Date | null = null;
  /** Snapshot from previous fetch; used to show movement vs current */
  lastRefreshHoldings: { totalShares: number; totalValueUsd: number; totalValueInr: number; livePriceUsd: number } | null = null;
  /** Expose Math for template (e.g. Math.abs in diff display). */
  readonly Math = Math;
  /** When current diff is 0, show this last non-zero diff instead of 0. */
  private lastShownLivePriceMovement: { diffUsd: number; diffPct: number } | null = null;
  private lastShownTotalValueUsdMovement: { diffUsd: number; diffPct: number } | null = null;
  private lastShownTotalValueInrMovement: { diffInr: number; diffPct: number } | null = null;
  private lastShownTotalSharesMovement: { diff: number; diffPct: number } | null = null;
  holdings: HoldingRow[] = [];
  loading = true;
  error: string | null = null;
  /** True while refreshing live data (click on live price card). */
  refreshLiveInProgress = false;
  /** Default: date bought descending (newest first). */
  holdingsSortKey: keyof HoldingRow | '' = 'buyDate';
  holdingsSortDir: 1 | -1 = -1;
  holdingsFilter: Record<string, string> = {};
  /** Column order for holdings table (persisted in localStorage). */
  holdingsColumnOrder: (typeof HOLDING_COLS)[number][] = [...HOLDING_COLS];
  private draggedColIndex: number | null = null;
  /** Selected row keys for sell-simulation (key = getRowKey(row)). */
  selectedRowKeys = new Set<string>();
  /** Sell qty per row key (qty to sell from that lot; used for simulation). */
  rowSellQty: Record<string, number> = {};
  /** Last row clicked (without shift) for shift-click range selection. */
  private lastClickedRowKey: string | null = null;
  /** Tab: Holdings vs Sold Shares vs Playground */
  activeTab: 'holdings' | 'sold' | 'playground' = 'holdings';
  soldRows: SoldRow[] = [];
  soldLoading = false;
  private soldLoaded = false;
  soldSortKey: keyof SoldRow | '' = '';
  soldSortDir: 1 | -1 = 1;
  soldFilter: Record<string, string> = {};
  readonly soldCols = SOLD_COLS;
  readonly soldColLabels = SOLD_COL_LABELS;
  /** Selected sold row keys for summary card (key = getSoldRowKey(row)). */
  selectedSoldKeys = new Set<string>();

  @ViewChild('holdingsChartCanvas') holdingsChartCanvas?: ElementRef<HTMLCanvasElement>;
  @ViewChild('soldChartCanvas') soldChartCanvas?: ElementRef<HTMLCanvasElement>;
  @ViewChild('livePriceChartCanvas') livePriceChartCanvas?: ElementRef<HTMLCanvasElement>;
  private holdingsChart: InstanceType<typeof Chart> | null = null;
  private soldChart: InstanceType<typeof Chart> | null = null;
  private livePriceChart: InstanceType<typeof Chart> | null = null;

  /** Chart consolidation: by date, or by month/quarter/year. */
  holdingsChartGroupBy: 'date' | 'monthly' | 'quarterly' | 'yearly' = 'date';
  readonly holdingsChartGroupByOptions: { value: 'date' | 'monthly' | 'quarterly' | 'yearly'; label: string }[] = [
    { value: 'date', label: 'By date' },
    { value: 'monthly', label: 'Monthly' },
    { value: 'quarterly', label: 'Quarterly' },
    { value: 'yearly', label: 'Yearly' },
  ];
  soldChartGroupBy: 'date' | 'monthly' | 'quarterly' | 'yearly' = 'date';
  readonly soldChartGroupByOptions: { value: 'date' | 'monthly' | 'quarterly' | 'yearly'; label: string }[] = [
    { value: 'date', label: 'By date' },
    { value: 'monthly', label: 'Monthly' },
    { value: 'quarterly', label: 'Quarterly' },
    { value: 'yearly', label: 'Yearly' },
  ];

  /** Mark as sold modal */
  showMarkSoldModal = false;
  markSoldSellDate = '';
  markSoldPriceUsd: number | null = null;
  markSoldInProgress = false;
  markSoldError: string | null = null;
  /** Undo last mark-as-sold */
  undoMarkSoldInProgress = false;
  undoMarkSoldError: string | null = null;
  /** Whether there is a mark-as-sold to undo (from API). */
  canUndoMarkSold = false;

  /** Playground: Start with money – target amount in bank (INR) after tax */
  playReqAmountInr: number | null = null;
  /** Playground: Override current price (USD) for table 1; null = use live price */
  playCurrentPriceOverride1: number | null = null;
  /** Playground: Target price (USD) for table 1 */
  playTargetPrice1: number | null = null;
  /** Playground: Approx. % of gross proceeds you keep after tax (e.g. 85) */
  playKeepPct = 87.5;
  /** Playground: Start with quantity – share count */
  playQty: number | null = null;
  /** Playground: Override current price (USD) for table 2; null = use live price */
  playCurrentPriceOverride: number | null = null;
  /** Playground: Target price (USD) for table 2 */
  playTargetPrice2: number | null = null;
  /** Playground: which simulation drives the sell recommendation (shares + target price) */
  playRecommendationSource: 'money' | 'quantity' = 'money';
  /** Playground: recommend only long-term, only short-term, or mixed (all) lots */
  playRecommendationTermFilter: 'long' | 'short' | 'mixed' = 'mixed';

  /** Live price polling: history for chart (max 7 days, max 5000 points) */
  livePriceHistory: { timestamp: number; livePriceUsd: number; usdToInrRate: number }[] = [];
  /** Whether US market is open (from /api/market-status). */
  marketOpen = false;
  private livePricePollingInterval: ReturnType<typeof setInterval> | null = null;
  private static readonly LIVE_PRICE_POLL_MS = 14000;
  private static readonly LIVE_PRICE_HISTORY_DAYS_MS = 7 * 24 * 60 * 60 * 1000;
  private static readonly LIVE_PRICE_HISTORY_MAX = 5000;

  constructor(
    private dashboardService: DashboardService,
    private cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.loadHoldingsColumnOrder();
    this.load();
    // Fetch undo availability immediately so Undo button state is correct
    this.updateCanUndoMarkSold();
  }

  private loadHoldingsColumnOrder(): void {
    try {
      const raw = localStorage.getItem(HOLDINGS_COLUMN_ORDER_STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw) as unknown;
        if (Array.isArray(parsed)) {
          // Migrate old column key to new one
          const migrated = (parsed as string[]).map((k) => (k === 'priceTodayInr' ? 'netIfSellTodayInr' : k));
          if (migrated.length === HOLDING_COLS.length) {
            const allKeys = new Set(HOLDING_COLS);
            const hasAll = migrated.every((k) => allKeys.has(k as (typeof HOLDING_COLS)[number]));
            if (hasAll) this.holdingsColumnOrder = migrated as (typeof HOLDING_COLS)[number][];
          }
        }
      }
    } catch {
      // ignore invalid stored order
    }
  }

  private saveHoldingsColumnOrder(): void {
    try {
      localStorage.setItem(HOLDINGS_COLUMN_ORDER_STORAGE_KEY, JSON.stringify(this.holdingsColumnOrder));
    } catch {
      // ignore
    }
  }

  load(refresh = false): void {
    this.loading = true;
    this.error = null;
    this.dashboardService.getDashboardData(refresh).subscribe({
      next: (res: DashboardResponse) => {
        if (this.data) {
          this.lastRefreshHoldings = {
            totalShares: this.data.totalShares,
            totalValueUsd: this.data.totalValueUsd,
            totalValueInr: this.data.totalValueInr,
            livePriceUsd: this.data.livePriceUsd,
          };
        }
        this.data = res;
        // Baseline for diff: after first load we have something to compare against on first poll.
        if (!this.lastRefreshHoldings) {
          this.lastRefreshHoldings = {
            totalShares: res.totalShares,
            totalValueUsd: res.totalValueUsd,
            totalValueInr: res.totalValueInr,
            livePriceUsd: res.livePriceUsd,
          };
        }
        this.lastRefreshedAt = new Date();
        this.loading = false;
        this.loadHoldings();
        this.updateCanUndoMarkSold();
        this.loadLivePriceHistoryFromDb();
        this.startLivePricePolling();
      },
      error: (err: { error?: { error?: string }; message?: string }) => {
        this.error = err?.error?.error || err?.message || 'Failed to load dashboard';
        this.loading = false;
      },
    });
  }

  /** Load stored live price history from DB, merge current data point, trim; restore diff baseline from previous point. */
  private loadLivePriceHistoryFromDb(): void {
    if (!this.data) return;
    const now = Date.now();
    const currentPoint = {
      timestamp: now,
      livePriceUsd: this.data.livePriceUsd,
      usdToInrRate: this.data.usdToInrRate,
    };
    this.dashboardService.getLivePriceHistory(7).subscribe({
      next: (stored) => {
        const combined = stored.length ? [...stored] : [];
        combined.push(currentPoint);
        combined.sort((a, b) => a.timestamp - b.timestamp);
        const cutoff = now - DashboardComponent.LIVE_PRICE_HISTORY_DAYS_MS;
        this.livePriceHistory = combined
          .filter((p) => p.timestamp >= cutoff)
          .slice(-DashboardComponent.LIVE_PRICE_HISTORY_MAX);
        if (this.livePriceHistory.length >= 2) {
          const prev = this.livePriceHistory[this.livePriceHistory.length - 2];
          const totalShares = this.data!.totalShares ?? 0;
          const prevTotalUsd = totalShares * prev.livePriceUsd;
          const prevTotalInr = totalShares * prev.livePriceUsd * prev.usdToInrRate;
          this.lastRefreshHoldings = {
            totalShares,
            totalValueUsd: Math.round(prevTotalUsd * 100) / 100,
            totalValueInr: Math.round(prevTotalInr * 100) / 100,
            livePriceUsd: prev.livePriceUsd,
          };
          const diffUsd = this.data!.livePriceUsd - prev.livePriceUsd;
          if (prev.livePriceUsd !== 0 && Math.abs(diffUsd) >= 0.01) {
            this.lastShownLivePriceMovement = { diffUsd, diffPct: (diffUsd / prev.livePriceUsd) * 100 };
          }
          if (prevTotalUsd !== 0 && Math.abs(this.data!.totalValueUsd - prevTotalUsd) >= 0.01) {
            const du = this.data!.totalValueUsd - prevTotalUsd;
            this.lastShownTotalValueUsdMovement = { diffUsd: du, diffPct: (du / prevTotalUsd) * 100 };
          }
          if (prevTotalInr !== 0 && Math.abs(this.data!.totalValueInr - prevTotalInr) >= 1) {
            const di = this.data!.totalValueInr - prevTotalInr;
            this.lastShownTotalValueInrMovement = { diffInr: di, diffPct: (di / prevTotalInr) * 100 };
          }
        }
        this.initOrUpdateLivePriceChart();
        this.cdr.detectChanges();
      },
      error: () => {
        this.livePriceHistory = [currentPoint];
        this.initOrUpdateLivePriceChart();
        this.cdr.detectChanges();
      },
    });
  }

  private startLivePricePolling(): void {
    this.stopLivePricePolling();
    const poll = (): void => {
      this.dashboardService.getMarketStatus().subscribe({
        next: (status) => {
          this.marketOpen = status.marketOpen;
          this.cdr.detectChanges();
          if (status.marketOpen) this.fetchAndPushLivePrice();
        },
        error: () => {
          this.marketOpen = false;
          this.cdr.detectChanges();
        },
      });
    };
    poll();
    this.livePricePollingInterval = setInterval(poll, DashboardComponent.LIVE_PRICE_POLL_MS);
  }

  private stopLivePricePolling(): void {
    if (this.livePricePollingInterval != null) {
      clearInterval(this.livePricePollingInterval);
      this.livePricePollingInterval = null;
    }
  }

  private fetchAndPushLivePrice(): void {
    this.dashboardService.getLivePrice().subscribe({
      next: (res) => {
        if (!this.data) return;
        // Snapshot current values so movement getters show diff vs previous (before this poll).
        this.lastRefreshHoldings = {
          totalShares: this.data.totalShares,
          totalValueUsd: this.data.totalValueUsd,
          totalValueInr: this.data.totalValueInr,
          livePriceUsd: this.data.livePriceUsd,
        };
        const totalShares = this.data.totalShares ?? 0;
        const totalValueUsd = totalShares * res.livePriceUsd;
        const totalValueInr = totalShares * res.livePriceUsd * res.usdToInrRate;
        this.data = {
          ...this.data,
          livePriceUsd: res.livePriceUsd,
          usdToInrRate: res.usdToInrRate,
          totalValueUsd: Math.round(totalValueUsd * 100) / 100,
          totalValueInr: Math.round(totalValueInr * 100) / 100,
        };
        this.lastRefreshedAt = new Date(res.lastUpdated);
        const now = Date.now();
        const point = { timestamp: now, livePriceUsd: res.livePriceUsd, usdToInrRate: res.usdToInrRate };
        this.livePriceHistory.push(point);
        const cutoff = now - DashboardComponent.LIVE_PRICE_HISTORY_DAYS_MS;
        this.livePriceHistory = this.livePriceHistory.filter((p) => p.timestamp >= cutoff);
        if (this.livePriceHistory.length > DashboardComponent.LIVE_PRICE_HISTORY_MAX) {
          this.livePriceHistory = this.livePriceHistory.slice(-DashboardComponent.LIVE_PRICE_HISTORY_MAX);
        }
        this.dashboardService.appendLivePriceHistory(point).subscribe({ error: () => { /* persist best-effort */ } });
        this.initOrUpdateLivePriceChart();
        this.updateLastShownMovements();
        this.cdr.detectChanges();
      },
      error: () => { /* keep last values */ },
    });
  }

  /** After a poll, update last-shown movements when current diff is non-zero so we keep showing it when next poll is unchanged. */
  private updateLastShownMovements(): void {
    if (!this.data || !this.lastRefreshHoldings) return;
    const last = this.lastRefreshHoldings;
    const cur = this.data;
    if (last.livePriceUsd !== 0) {
      const diffUsd = cur.livePriceUsd - last.livePriceUsd;
      if (Math.abs(diffUsd) >= 0.01) {
        this.lastShownLivePriceMovement = { diffUsd, diffPct: (diffUsd / last.livePriceUsd) * 100 };
      }
    }
    if (last.totalValueUsd !== 0) {
      const diffUsd = cur.totalValueUsd - last.totalValueUsd;
      if (Math.abs(diffUsd) >= 0.01) {
        this.lastShownTotalValueUsdMovement = { diffUsd, diffPct: (diffUsd / last.totalValueUsd) * 100 };
      }
    }
    if (last.totalValueInr !== 0) {
      const diffInr = cur.totalValueInr - last.totalValueInr;
      if (Math.abs(diffInr) >= 1) {
        this.lastShownTotalValueInrMovement = { diffInr, diffPct: (diffInr / last.totalValueInr) * 100 };
      }
    }
    if (last.totalShares !== 0) {
      const diff = cur.totalShares - last.totalShares;
      if (diff !== 0) {
        this.lastShownTotalSharesMovement = { diff, diffPct: (diff / last.totalShares) * 100 };
      }
    }
  }

  private updateCanUndoMarkSold(): void {
    this.dashboardService.getMarkSoldCanUndo().subscribe({
      next: (res) => {
        this.canUndoMarkSold = res.canUndo;
        this.cdr.detectChanges();
      },
      error: () => {
        this.canUndoMarkSold = false;
        this.cdr.detectChanges();
      },
    });
  }

  loadHoldings(): void {
    this.dashboardService.getHoldings().subscribe({
      next: (rows) => {
        this.holdings = rows;
        this.cdr.detectChanges();
        if (this.activeTab === 'holdings') setTimeout(() => this.initOrUpdateHoldingsChart(), 0);
      },
      error: () => { this.holdings = []; },
    });
  }

  ngAfterViewInit(): void {
    if (this.activeTab === 'holdings' && this.holdings.length > 0) setTimeout(() => this.initOrUpdateHoldingsChart(), 0);
  }

  ngOnDestroy(): void {
    this.stopLivePricePolling();
    if (this.holdingsChart) {
      this.holdingsChart.destroy();
      this.holdingsChart = null;
    }
    if (this.soldChart) {
      this.soldChart.destroy();
      this.soldChart = null;
    }
    if (this.livePriceChart) {
      this.livePriceChart.destroy();
      this.livePriceChart = null;
    }
  }

  private initOrUpdateLivePriceChart(): void {
    if (this.livePriceHistory.length < 2) return;
    setTimeout(() => {
      this.cdr.detectChanges();
      if (this.livePriceChart) {
        this.updateLivePriceChartData();
      } else if (this.livePriceChartCanvas?.nativeElement) {
        this.createLivePriceChart();
      }
    }, 100);
  }

  private updateLivePriceChartData(): void {
    if (!this.livePriceChart || this.livePriceHistory.length < 2) return;
    const h = this.livePriceHistory;
    this.livePriceChart.data.labels = h.map((p) => {
      const d = new Date(p.timestamp);
      return d.toLocaleString('en-IN', { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });
    });
    const nvdaPrices = h.map((p) => p.livePriceUsd);
    const avg = nvdaPrices.reduce((a, b) => a + b, 0) / nvdaPrices.length;
    (this.livePriceChart.data.datasets[0] as { data: number[] }).data = nvdaPrices;
    (this.livePriceChart.data.datasets[1] as { data: number[] }).data = nvdaPrices.map(() => avg);
    this.livePriceChart.update();
  }

  private createLivePriceChart(): void {
    const canvas = this.livePriceChartCanvas?.nativeElement;
    if (!canvas || this.livePriceHistory.length < 2 || this.livePriceChart) return;
    const h = this.livePriceHistory;
    const labels = h.map((p) => {
      const d = new Date(p.timestamp);
      return d.toLocaleString('en-IN', { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });
    });
    const nvdaPrices = h.map((p) => p.livePriceUsd);
    const avg = nvdaPrices.reduce((a, b) => a + b, 0) / nvdaPrices.length;
    this.livePriceChart = new Chart(canvas, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'NVDA (USD)',
            data: nvdaPrices,
            borderColor: '#388bfd',
            backgroundColor: 'rgba(56, 139, 253, 0.1)',
            fill: false,
            yAxisID: 'y',
            tension: 0.2,
          },
          {
            label: 'Average',
            data: nvdaPrices.map(() => avg),
            borderColor: '#7ee787',
            backgroundColor: 'transparent',
            fill: false,
            yAxisID: 'y',
            borderDash: [6, 4],
            tension: 0,
            datalabels: { display: false },
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { position: 'top' },
          tooltip: { enabled: true },
        },
        scales: {
          x: {
            ticks: { color: '#8b949e', maxRotation: 45, maxTicksLimit: 12 },
            title: { display: true, text: 'Time', color: '#8b949e' },
          },
          y: {
            position: 'left',
            title: { display: true, text: 'NVDA ($)', color: '#8b949e' },
            ticks: { color: '#8b949e' },
            grid: { drawOnChartArea: true },
          },
        },
      },
    });
  }

  /** Build stacked bar data by buy date (RSU + ESPP). Group by date, month, quarter, or year. */
  private getHoldingsChartData(): { labels: string[]; rsuQty: number[]; esppQty: number[] } {
    const groupBy = this.holdingsChartGroupBy;
    const bucket: Record<string, { rsu: number; espp: number }> = {};

    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    for (const row of this.holdings) {
      const dateStr = String(row.buyDate ?? '').split('T')[0].trim();
      if (!dateStr || dateStr === 'Unknown') continue;
      const [y, m, day] = dateStr.split('-');
      const year = parseInt(y, 10);
      const month = parseInt(m, 10) || 1;

      let key: string;
      if (groupBy === 'date') {
        key = dateStr;
      } else if (groupBy === 'monthly') {
        key = `${y}-${m}`;
      } else if (groupBy === 'quarterly') {
        const q = Math.ceil(month / 3);
        key = `${y}-${q}`;
      } else {
        key = y;
      }

      if (!bucket[key]) bucket[key] = { rsu: 0, espp: 0 };
      if (row.type === 'ESPP') bucket[key].espp += row.qty;
      else bucket[key].rsu += row.qty;
    }

    const sortedKeys = Object.keys(bucket).sort((a, b) => {
      if (groupBy === 'date') return a.localeCompare(b);
      if (groupBy === 'yearly') return a.localeCompare(b);
      if (groupBy === 'monthly') return a.localeCompare(b);
      if (groupBy === 'quarterly') {
        const [ya, qa] = a.split('-').map((x) => parseInt(x, 10));
        const [yb, qb] = b.split('-').map((x) => parseInt(x, 10));
        return ya !== yb ? ya - yb : qa - qb;
      }
      return 0;
    });

    const labels = sortedKeys.map((key) => {
      if (groupBy === 'date') {
        const [yy, mm, dd] = key.split('-');
        return `${mm}/${dd}/${yy}`;
      }
      if (groupBy === 'monthly') {
        const [yy, mm] = key.split('-');
        const m = parseInt(mm, 10) || 1;
        return `${monthNames[m - 1]} ${yy}`;
      }
      if (groupBy === 'quarterly') {
        const [yy, q] = key.split('-');
        return `Q${q} ${yy}`;
      }
      return key; // yearly
    });

    const rsuQty = sortedKeys.map((k) => bucket[k].rsu);
    const esppQty = sortedKeys.map((k) => bucket[k].espp);
    return { labels, rsuQty, esppQty };
  }

  setHoldingsChartGroupBy(mode: 'date' | 'monthly' | 'quarterly' | 'yearly'): void {
    this.holdingsChartGroupBy = mode;
    this.initOrUpdateHoldingsChart();
  }

  setSoldChartGroupBy(mode: 'date' | 'monthly' | 'quarterly' | 'yearly'): void {
    this.soldChartGroupBy = mode;
    this.initOrUpdateSoldChart();
  }

  /** Build stacked bar data for sold by sell date (RSU + ESPP qty sold). */
  private getSoldChartData(): { labels: string[]; rsuQty: number[]; esppQty: number[] } {
    const groupBy = this.soldChartGroupBy;
    const bucket: Record<string, { rsu: number; espp: number }> = {};
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    for (const row of this.soldRows) {
      const dateStr = String(row.sellDate ?? '').split('T')[0].trim();
      if (!dateStr) continue;
      const [y, m] = dateStr.split('-');
      const year = parseInt(y, 10);
      const month = parseInt(m, 10) || 1;

      let key: string;
      if (groupBy === 'date') key = dateStr;
      else if (groupBy === 'monthly') key = `${y}-${m}`;
      else if (groupBy === 'quarterly') key = `${y}-${Math.ceil(month / 3)}`;
      else key = y;

      if (!bucket[key]) bucket[key] = { rsu: 0, espp: 0 };
      if (row.type === 'ESPP') bucket[key].espp += row.qtySold;
      else bucket[key].rsu += row.qtySold;
    }

    const sortedKeys = Object.keys(bucket).sort((a, b) => {
      if (groupBy === 'date' || groupBy === 'yearly') return a.localeCompare(b);
      if (groupBy === 'monthly') return a.localeCompare(b);
      if (groupBy === 'quarterly') {
        const [ya, qa] = a.split('-').map((x) => parseInt(x, 10));
        const [yb, qb] = b.split('-').map((x) => parseInt(x, 10));
        return ya !== yb ? ya - yb : qa - qb;
      }
      return 0;
    });

    const labels = sortedKeys.map((key) => {
      if (groupBy === 'date') {
        const [yy, mm, dd] = key.split('-');
        return `${mm}/${dd}/${yy}`;
      }
      if (groupBy === 'monthly') {
        const [, mm] = key.split('-');
        const m = parseInt(mm, 10) || 1;
        return `${monthNames[m - 1]} ${key.split('-')[0]}`;
      }
      if (groupBy === 'quarterly') return `Q${key.split('-')[1]} ${key.split('-')[0]}`;
      return key;
    });

    return {
      labels,
      rsuQty: sortedKeys.map((k) => bucket[k].rsu),
      esppQty: sortedKeys.map((k) => bucket[k].espp),
    };
  }

  private initOrUpdateHoldingsChart(): void {
    const canvas = this.holdingsChartCanvas?.nativeElement;
    if (!canvas || this.holdings.length === 0) return;
    const { labels, rsuQty, esppQty } = this.getHoldingsChartData();
    if (labels.length === 0) return;

    if (this.holdingsChart) {
      this.holdingsChart.data.labels = labels;
      (this.holdingsChart.data.datasets[0] as { data: number[] }).data = rsuQty;
      (this.holdingsChart.data.datasets[1] as { data: number[] }).data = esppQty;
      const scales = this.holdingsChart.options.scales;
      const xScale = scales && (scales as Record<string, { title?: { text?: string } }>)['x'];
      if (xScale?.title) {
        xScale.title.text = this.holdingsChartGroupBy === 'date' ? 'Date bought' : 'Period';
      }
      this.holdingsChart.update();
      return;
    }

    this.holdingsChart = new Chart(canvas, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            label: 'RSU',
            data: rsuQty,
            backgroundColor: '#388bfd',
            stack: 'qty',
            datalabels: {
              anchor: 'center',
              align: 'center',
              color: '#e6edf3',
              formatter: ((value: number, ctx: { dataIndex: number; dataset: { data: unknown[] } }) => {
                if (value === 0) return '';
                const prev = ctx.dataIndex > 0 ? ctx.dataset.data[ctx.dataIndex - 1] : undefined;
                return prev === undefined || value !== prev ? String(value) : '';
              }) as (value: unknown, context: unknown) => unknown,
              font: { size: 11 },
            },
          },
          {
            label: 'ESPP',
            data: esppQty,
            backgroundColor: '#7ee787',
            stack: 'qty',
            datalabels: {
              anchor: 'end',
              align: 'top',
              offset: 4,
              color: '#e6edf3',
              formatter: ((value: number, ctx: { dataIndex: number; dataset: { data: unknown[] } }) => {
                if (value === 0) return '';
                const prev = ctx.dataIndex > 0 ? ctx.dataset.data[ctx.dataIndex - 1] : undefined;
                return prev === undefined || value !== prev ? String(value) : '';
              }) as (value: unknown, context: unknown) => unknown,
              font: { size: 11 },
            },
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top' },
          tooltip: { mode: 'index', intersect: false },
        },
        scales: {
          x: {
            stacked: true,
            title: { display: true, text: this.holdingsChartGroupBy === 'date' ? 'Date bought' : 'Period', color: '#8b949e' },
            ticks: { color: '#8b949e', maxRotation: 45 },
          },
          y: {
            stacked: true,
            beginAtZero: true,
            title: { display: true, text: 'Total qty', color: '#8b949e' },
            ticks: { color: '#8b949e' },
          },
        },
      },
    });
  }

  private initOrUpdateSoldChart(): void {
    const canvas = this.soldChartCanvas?.nativeElement;
    if (!canvas || this.soldRows.length === 0) return;
    const { labels, rsuQty, esppQty } = this.getSoldChartData();
    if (labels.length === 0) return;

    if (this.soldChart) {
      this.soldChart.data.labels = labels;
      (this.soldChart.data.datasets[0] as { data: number[] }).data = rsuQty;
      (this.soldChart.data.datasets[1] as { data: number[] }).data = esppQty;
      const scales = this.soldChart.options.scales;
      const xScale = scales && (scales as Record<string, { title?: { text?: string } }>)['x'];
      if (xScale?.title) {
        xScale.title.text = this.soldChartGroupBy === 'date' ? 'Sell date' : 'Period';
      }
      this.soldChart.update();
      return;
    }

    this.soldChart = new Chart(canvas, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            label: 'RSU',
            data: rsuQty,
            backgroundColor: '#388bfd',
            stack: 'qty',
            datalabels: {
              anchor: 'center',
              align: 'center',
              color: '#e6edf3',
              formatter: ((value: number, ctx: { dataIndex: number; dataset: { data: unknown[] } }) => {
                if (value === 0) return '';
                const prev = ctx.dataIndex > 0 ? ctx.dataset.data[ctx.dataIndex - 1] : undefined;
                return prev === undefined || value !== prev ? String(value) : '';
              }) as (value: unknown, context: unknown) => unknown,
              font: { size: 11 },
            },
          },
          {
            label: 'ESPP',
            data: esppQty,
            backgroundColor: '#7ee787',
            stack: 'qty',
            datalabels: {
              anchor: 'end',
              align: 'top',
              offset: 4,
              color: '#e6edf3',
              formatter: ((value: number, ctx: { dataIndex: number; dataset: { data: unknown[] } }) => {
                if (value === 0) return '';
                const prev = ctx.dataIndex > 0 ? ctx.dataset.data[ctx.dataIndex - 1] : undefined;
                return prev === undefined || value !== prev ? String(value) : '';
              }) as (value: unknown, context: unknown) => unknown,
              font: { size: 11 },
            },
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top' },
          tooltip: { mode: 'index', intersect: false },
        },
        scales: {
          x: {
            stacked: true,
            title: { display: true, text: this.soldChartGroupBy === 'date' ? 'Sell date' : 'Period', color: '#8b949e' },
            ticks: { color: '#8b949e', maxRotation: 45 },
          },
          y: {
            stacked: true,
            beginAtZero: true,
            title: { display: true, text: 'Qty sold', color: '#8b949e' },
            ticks: { color: '#8b949e' },
          },
        },
      },
    });
  }

  /** Refresh live price and dashboard data (skips backend cache). Click on live price card. */
  refreshLiveData(): void {
    if (!this.data || this.refreshLiveInProgress) return;
    this.refreshLiveInProgress = true;
    this.dashboardService.getDashboardData(true).subscribe({
      next: (res) => {
        if (this.data) {
          this.lastRefreshHoldings = {
            totalShares: this.data.totalShares,
            totalValueUsd: this.data.totalValueUsd,
            totalValueInr: this.data.totalValueInr,
            livePriceUsd: this.data.livePriceUsd,
          };
        }
        this.data = res;
        this.lastRefreshedAt = new Date();
        this.refreshLiveInProgress = false;
        this.loadHoldings();
      },
      error: () => {
        this.refreshLiveInProgress = false;
      },
    });
  }

  loadSold(): void {
    this.soldLoading = true;
    this.dashboardService.getSold().subscribe({
      next: (rows) => {
        this.soldRows = rows;
        this.soldLoaded = true;
        this.soldLoading = false;
        this.cdr.detectChanges();
        setTimeout(() => this.initOrUpdateSoldChart(), 0);
      },
      error: () => { this.soldRows = []; this.soldLoading = false; },
    });
  }

  setActiveTab(tab: 'holdings' | 'sold' | 'playground'): void {
    this.activeTab = tab;
    if (tab === 'sold') {
      if (this.holdingsChart) {
        this.holdingsChart.destroy();
        this.holdingsChart = null;
      }
      if (!this.soldLoaded && !this.soldLoading) this.loadSold();
      else if (this.soldRows.length > 0) setTimeout(() => this.initOrUpdateSoldChart(), 0);
    } else if (tab === 'playground') {
      if (this.holdingsChart) {
        this.holdingsChart.destroy();
        this.holdingsChart = null;
      }
      if (this.soldChart) {
        this.soldChart.destroy();
        this.soldChart = null;
      }
    } else {
      if (this.soldChart) {
        this.soldChart.destroy();
        this.soldChart = null;
      }
      if (this.holdings.length > 0) setTimeout(() => this.initOrUpdateHoldingsChart(), 0);
    }
  }

  /** Movement vs last refresh: total value INR; when unchanged show last non-zero diff. */
  get totalValueInrMovement(): { diffInr: number; diffPct: number } | null {
    if (!this.data || !this.lastRefreshHoldings) return null;
    const cur = this.data.totalValueInr;
    const last = this.lastRefreshHoldings.totalValueInr;
    if (last === 0) return this.lastShownTotalValueInrMovement;
    const diffInr = cur - last;
    if (Math.abs(diffInr) >= 1) return { diffInr, diffPct: (diffInr / last) * 100 };
    return this.lastShownTotalValueInrMovement;
  }
  /** Movement vs last refresh: total value USD; when unchanged show last non-zero diff. */
  get totalValueUsdMovement(): { diffUsd: number; diffPct: number } | null {
    if (!this.data || !this.lastRefreshHoldings) return null;
    const cur = this.data.totalValueUsd;
    const last = this.lastRefreshHoldings.totalValueUsd;
    if (last === 0) return this.lastShownTotalValueUsdMovement;
    const diffUsd = cur - last;
    if (Math.abs(diffUsd) >= 0.01) return { diffUsd, diffPct: (diffUsd / last) * 100 };
    return this.lastShownTotalValueUsdMovement;
  }
  /** Movement vs last refresh: shares; when unchanged show last non-zero diff. */
  get totalSharesMovement(): { diff: number; diffPct: number } | null {
    if (!this.data || !this.lastRefreshHoldings) return null;
    const cur = this.data.totalShares;
    const last = this.lastRefreshHoldings.totalShares;
    if (last === 0) return this.lastShownTotalSharesMovement;
    const diff = cur - last;
    if (diff !== 0) return { diff, diffPct: (diff / last) * 100 };
    return this.lastShownTotalSharesMovement;
  }
  /** Movement vs last refresh: live NVDA price; when unchanged show last non-zero diff. */
  get livePriceMovement(): { diffUsd: number; diffPct: number } | null {
    if (!this.data || !this.lastRefreshHoldings) return null;
    const cur = this.data.livePriceUsd;
    const last = this.lastRefreshHoldings.livePriceUsd;
    if (last === 0) return this.lastShownLivePriceMovement;
    const diffUsd = cur - last;
    if (Math.abs(diffUsd) >= 0.01) return { diffUsd, diffPct: (diffUsd / last) * 100 };
    return this.lastShownLivePriceMovement;
  }

  /** Playground table 1: current price (USD) from dashboard */
  get playCurrentPrice(): number {
    return this.data?.livePriceUsd ?? 0;
  }
  /** Playground: USD to INR rate */
  get playUsdToInr(): number {
    return this.data?.usdToInrRate ?? 0;
  }
  /** Playground table 1: required gross proceeds (INR) to get req amount after tax */
  get playReqBeforeTaxInr(): number | null {
    const amt = this.playReqAmountInr;
    const pct = this.playKeepPct;
    if (amt == null || amt <= 0 || pct <= 0 || pct > 100) return null;
    return amt / (pct / 100);
  }
  /** Playground table 1: current price (override or live) */
  get playCurrentPriceForMoney(): number {
    return this.playCurrentPriceOverride1 ?? this.playCurrentPrice;
  }
  /** Playground table 1: shares to sell at current price to achieve req amount */
  get playSharesAtCurrent(): number | null {
    const before = this.playReqBeforeTaxInr;
    const price = this.playCurrentPriceForMoney;
    const rate = this.playUsdToInr;
    if (before == null || before <= 0 || price <= 0 || rate <= 0) return null;
    return before / (price * rate);
  }
  /** Playground table 1: shares to sell at target price */
  get playSharesAtTarget(): number | null {
    const before = this.playReqBeforeTaxInr;
    const target = this.playTargetPrice1;
    const rate = this.playUsdToInr;
    if (before == null || before <= 0 || target == null || target <= 0 || rate <= 0) return null;
    return before / (target * rate);
  }
  /** Playground table 1: difference in shares (target − current); negative = fewer shares at target */
  get playSharesDiff(): number | null {
    const cur = this.playSharesAtCurrent;
    const tgt = this.playSharesAtTarget;
    if (cur == null || tgt == null) return null;
    return Math.round(tgt - cur);
  }
  /** Playground table 1: monetary difference (INR) between selling at target vs current for same shares */
  get playValueDiffInr(): number | null {
    const cur = this.playSharesAtCurrent;
    const target = this.playTargetPrice1;
    const rate = this.playUsdToInr;
    const price = this.playCurrentPriceForMoney;
    if (cur == null || target == null || rate <= 0 || price <= 0) return null;
    const atTarget = cur * target * rate;
    const atCurrent = cur * price * rate;
    return atTarget - atCurrent;
  }
  /** Playground table 2: current price (override or live) */
  get playCurrentPriceForQty(): number {
    return this.playCurrentPriceOverride ?? this.playCurrentPrice;
  }
  /** Playground table 2: current total value (INR) */
  get playCurrentValueInr(): number | null {
    const qty = this.playQty;
    const price = this.playCurrentPriceForQty;
    const rate = this.playUsdToInr;
    if (qty == null || qty <= 0 || price <= 0 || rate <= 0) return null;
    return qty * price * rate;
  }
  /** Playground table 2: target total value (INR) */
  get playTargetValueInr(): number | null {
    const qty = this.playQty;
    const target = this.playTargetPrice2;
    const rate = this.playUsdToInr;
    if (qty == null || qty <= 0 || target == null || target <= 0 || rate <= 0) return null;
    return qty * target * rate;
  }
  /** Playground table 2: difference (target − current) in INR */
  get playValueDiff2Inr(): number | null {
    const cur = this.playCurrentValueInr;
    const tgt = this.playTargetValueInr;
    if (cur == null || tgt == null) return null;
    return tgt - cur;
  }
  /** Playground: target price (USD) for recommendation; from selected simulation */
  get playTargetPriceForRecommendation(): number | null {
    if (this.playRecommendationSource === 'money') return this.playTargetPrice1 ?? null;
    return this.playTargetPrice2 ?? null;
  }
  /** Playground: max shares to recommend; from selected simulation; null = no cap */
  get playSharesNeededForRecommendation(): number | null {
    let raw: number | null;
    if (this.playRecommendationSource === 'money') {
      raw = this.playSharesAtCurrent ?? this.playSharesAtTarget ?? null;
    } else {
      raw = this.playQty;
    }
    if (raw == null || raw <= 0 || !Number.isFinite(raw)) return null;
    return Math.ceil(raw);
  }
  /** Playground: recommendations sorted by tax per share at target (lowest first) to minimize total tax, capped to simulation shares */
  get playTaxRecommendations(): PlayTaxRecommendationRow[] {
    const rate = this.playUsdToInr;
    const targetPrice = this.playTargetPriceForRecommendation;
    const maxShares = this.playSharesNeededForRecommendation;
    const termFilter = this.playRecommendationTermFilter;
    // Long-term typically 12.5% (>=2 yrs); short-term 30% (<2 yrs). Use threshold 15 to separate.
    const isLongTerm = (row: HoldingRow) => (row.taxPercent ?? 0) <= 15;
    let holdingsFiltered = this.holdings;
    if (termFilter === 'long') holdingsFiltered = this.holdings.filter(isLongTerm);
    else if (termFilter === 'short') holdingsFiltered = this.holdings.filter((r) => !isLongTerm(r));
    const list = holdingsFiltered.map((row): HoldingRow & { taxToPayAtTargetInr: number } => {
      const taxPct = (row.taxPercent ?? 0) / 100;
      let taxToPayAtTargetInr: number;
      if (targetPrice != null && targetPrice > 0 && rate > 0) {
        const gainPerShareUsd = targetPrice - row.buyPriceUsd;
        const gainInr = gainPerShareUsd * row.qty * rate;
        taxToPayAtTargetInr = Math.max(0, gainInr) * taxPct;
      } else {
        taxToPayAtTargetInr = row.taxToPayInr ?? 0;
      }
      return { ...row, taxToPayAtTargetInr };
    });
    // Sort by tax per share at target (lowest first) so total tax is minimized, not just tax rate
    list.sort((a, b) => {
      const perShareA = a.qty > 0 ? (a as { taxToPayAtTargetInr: number }).taxToPayAtTargetInr / a.qty : 0;
      const perShareB = b.qty > 0 ? (b as { taxToPayAtTargetInr: number }).taxToPayAtTargetInr / b.qty : 0;
      return perShareA - perShareB;
    });

    const out: PlayTaxRecommendationRow[] = [];
    let remaining = maxShares ?? Infinity;
    for (const row of list) {
      if (remaining <= 0) break;
      const qtyToSell = Math.min(row.qty, remaining);
      remaining -= qtyToSell;
      const ratio = row.qty > 0 ? qtyToSell / row.qty : 0;
      out.push({
        ...row,
        taxToPayAtTargetInr: Math.round((row as { taxToPayAtTargetInr: number }).taxToPayAtTargetInr * ratio * 100) / 100,
        qtyToSell,
      });
    }
    return out;
  }
  /** Playground: total tax (INR) for recommended lots at simulation target price */
  get playTaxRecommendationTotalTaxInr(): number | null {
    const rec = this.playTaxRecommendations;
    if (rec.length === 0) return null;
    return Math.round(rec.reduce((sum, r) => sum + r.taxToPayAtTargetInr, 0) * 100) / 100;
  }
  /** Playground: total shares in the recommendation (sum of qtyToSell) */
  get playTaxRecommendationTotalShares(): number {
    return this.playTaxRecommendations.reduce((sum, r) => sum + r.qtyToSell, 0);
  }
  /** Playground: when simulation has a target and filtered lots don't cover it, shares short; null otherwise */
  get playTaxRecommendationShortfall(): number | null {
    const needed = this.playSharesNeededForRecommendation;
    if (needed == null || needed <= 0) return null;
    const total = this.playTaxRecommendationTotalShares;
    if (total >= needed) return null;
    return needed - total;
  }

  formatPlayNum(n: number | null, decimals = 0): string {
    if (n == null || !Number.isFinite(n)) return '–';
    return decimals > 0 ? n.toFixed(decimals) : String(Math.round(n));
  }

  /** Display text for a sold row cell (used for contains filter). */
  private soldCellDisplay(row: SoldRow, col: keyof SoldRow): string {
    const v = (row as unknown as Record<string, unknown>)[col];
    if (col === 'sellDate' || col === 'buyDate') return this.formatDate(String(v ?? ''));
    if (col === 'type') return this.typeLabel(String(v ?? ''));
    if (col === 'qtySold') return String(v ?? '');
    if (col === 'priceBoughtUsd' || col === 'priceSellUsd') return this.formatUsd(Number(v));
    if (['buyValueInr', 'sellValueInr', 'gainBeforeTaxInr', 'taxPaidInr'].includes(col)) return '₹ ' + this.formatInr(Number(v));
    if (col === 'profitPercent' || col === 'taxPercent') return (Number(v) ?? 0).toFixed(1) + '%';
    return String(v ?? '');
  }

  get soldFilteredSorted(): SoldRow[] {
    let list = this.soldRows.slice();
    for (const col of SOLD_COLS) {
      const f = (this.soldFilter[col] ?? '').trim().toLowerCase();
      if (!f) continue;
      list = list.filter((r) => {
        const cell = this.soldCellDisplay(r, col);
        return cell.toLowerCase().includes(f);
      });
    }
    if (!this.soldSortKey) return list;
    const key = this.soldSortKey;
    const isDate = SOLD_DATE_COLS.includes(key);
    list.sort((a, b) => {
      const va = (a as unknown as Record<string, unknown>)[key];
      const vb = (b as unknown as Record<string, unknown>)[key];
      let cmp = 0;
      if (isDate) {
        cmp = this.parseDate(String(va)) - this.parseDate(String(vb));
      } else if (typeof va === 'number' && typeof vb === 'number') {
        cmp = va - vb;
      } else {
        cmp = String(va ?? '').localeCompare(String(vb ?? ''));
      }
      return this.soldSortDir * cmp;
    });
    return list;
  }

  setSoldSort(key: keyof SoldRow): void {
    if (this.soldSortKey === key) this.soldSortDir *= -1;
    else { this.soldSortKey = key; this.soldSortDir = 1; }
  }

  getSoldRowKey(row: SoldRow): string {
    return `${row.sellDate}|${row.buyDate}|${row.type}|${row.qtySold}|${row.sellValueInr}`;
  }

  isSoldRowSelected(row: SoldRow): boolean {
    return this.selectedSoldKeys.has(this.getSoldRowKey(row));
  }

  toggleSoldRowSelection(row: SoldRow): void {
    const key = this.getSoldRowKey(row);
    if (this.selectedSoldKeys.has(key)) {
      this.selectedSoldKeys.delete(key);
      this.selectedSoldKeys = new Set(this.selectedSoldKeys);
    } else {
      this.selectedSoldKeys = new Set(this.selectedSoldKeys).add(key);
    }
    this.cdr.detectChanges();
  }

  /** Select only the currently visible (filtered) sold rows; replaces selection. */
  selectAllSoldVisible(): void {
    const rows = this.soldFilteredSorted;
    const next = new Set<string>();
    for (const r of rows) next.add(this.getSoldRowKey(r));
    this.selectedSoldKeys = next;
    this.cdr.detectChanges();
  }

  /** Deselect only the currently visible (filtered) sold rows. */
  deselectAllSoldVisible(): void {
    const rows = this.soldFilteredSorted;
    const next = new Set(this.selectedSoldKeys);
    for (const r of rows) next.delete(this.getSoldRowKey(r));
    this.selectedSoldKeys = next;
    this.cdr.detectChanges();
  }

  /** Totals for sold: when rows selected, use selected only; else use all filtered. */
  get soldSummaryTotals(): { totalQtySold: number; totalSellValueInr: number; totalTaxPaidInr: number; selectedCount: number } {
    const rows = this.soldFilteredSorted;
    const useSelected = this.selectedSoldKeys.size > 0;
    let totalQtySold = 0;
    let totalSellValueInr = 0;
    let totalTaxPaidInr = 0;
    let selectedCount = 0;
    for (const r of rows) {
      if (useSelected && !this.selectedSoldKeys.has(this.getSoldRowKey(r))) continue;
      selectedCount++;
      totalQtySold += r.qtySold;
      totalSellValueInr += r.sellValueInr;
      totalTaxPaidInr += r.taxPaidInr;
    }
    return { totalQtySold, totalSellValueInr, totalTaxPaidInr, selectedCount };
  }

  /** RSU share of total sold value (0–100) for pie chart. When no breakdown (total 0), returns 0 so pie shows no RSU slice. */
  get soldRsuPercent(): number {
    if (!this.data) return 0;
    const rsu = this.data.soldValueRsuInr ?? 0;
    const espp = this.data.soldValueEsppInr ?? 0;
    const total = rsu + espp;
    return total === 0 ? 0 : (rsu / total) * 100;
  }

  /** ESPP share of total sold value (0–100) for pie chart. When no breakdown (total 0), returns 100 so pie shows full ESPP. */
  get soldEsppPercent(): number {
    const total = (this.data?.soldValueRsuInr ?? 0) + (this.data?.soldValueEsppInr ?? 0);
    return total === 0 ? 100 : 100 - this.soldRsuPercent;
  }

  /** True when we have a valid RSU+ESPP breakdown for the realised profit pie (so we don't show misleading 50/50). */
  get hasSoldBreakdown(): boolean {
    const total = (this.data?.soldValueRsuInr ?? 0) + (this.data?.soldValueEsppInr ?? 0);
    return total > 0;
  }

  /** CSS conic-gradient for RSU vs ESPP pie (RSU from 12 o'clock). */
  get soldPieConicGradient(): string {
    const rsu = this.soldRsuPercent;
    return `conic-gradient(#388bfd 0% ${rsu}%, #7ee787 ${rsu}% 100%)`;
  }

  /** RSU (nsu) share of total profit after tax (0–100) for breakdown pie; when both nsu and espp exist. */
  get breakdownNsuPercent(): number {
    if (!this.data?.nsu || !this.data?.espp) return 50;
    const nsu = this.data.nsu.profitAfterTax ?? 0;
    const espp = this.data.espp.profitAfterTax ?? 0;
    const total = nsu + espp;
    return total === 0 ? 50 : (nsu / total) * 100;
  }

  /** ESPP share of total profit after tax (0–100) for breakdown pie. */
  get breakdownEsppPercent(): number {
    if (!this.data?.nsu || !this.data?.espp) return 50;
    const total = (this.data.nsu?.profitAfterTax ?? 0) + (this.data.espp?.profitAfterTax ?? 0);
    return total === 0 ? 50 : 100 - this.breakdownNsuPercent;
  }

  /** CSS conic-gradient for RSU vs ESPP breakdown pie (profit after tax). */
  get breakdownPieConicGradient(): string {
    const nsu = this.breakdownNsuPercent;
    return `conic-gradient(#388bfd 0% ${nsu}%, #7ee787 ${nsu}% 100%)`;
  }

  formatInr(n: number): string {
    return new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0, minimumFractionDigits: 0 }).format(n);
  }

  formatUsd(n: number): string {
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 6 }).format(n);
  }

  liveInrPerShare(): number {
    return this.data ? this.data.livePriceUsd * this.data.usdToInrRate : 0;
  }

  typeLabel(type: string): string {
    return type === 'NSU' ? 'RSU' : type;
  }

  formatDate(isoDate: string): string {
    if (!isoDate) return '';
    const d = new Date(isoDate);
    return isNaN(d.getTime()) ? isoDate : d.toLocaleDateString('en-IN', { day: '2-digit', month: '2-digit', year: 'numeric' });
  }

  /** Date and time of last dashboard refresh for display in Live price block */
  formatLastRefreshed(d: Date | null): string {
    if (!d) return '';
    return d.toLocaleString('en-IN', { dateStyle: 'medium', timeStyle: 'short' });
  }

  parseDate(s: string): number {
    if (!s) return 0;
    const d = new Date(s);
    return isNaN(d.getTime()) ? 0 : d.getTime();
  }

  get holdingsFilteredSorted(): HoldingRow[] {
    let list = this.holdings.slice();
    for (const col of HOLDING_COLS) {
      const f = (this.holdingsFilter[col] || '').trim().toLowerCase();
      if (!f) continue;
      list = list.filter((r) => {
        const cell = this.formatCell(r, col);
        return cell.toLowerCase().includes(f);
      });
    }
    if (!this.holdingsSortKey) return list;
    const key = this.holdingsSortKey;
    const isDate = DATE_COLS.includes(key);
    list.sort((a, b) => {
      const va = (a as unknown as Record<string, unknown>)[key];
      const vb = (b as unknown as Record<string, unknown>)[key];
      let cmp = 0;
      if (isDate) {
        cmp = this.parseDate(String(va)) - this.parseDate(String(vb));
      } else if (typeof va === 'number' && typeof vb === 'number') {
        cmp = va - vb;
      } else {
        cmp = String(va ?? '').localeCompare(String(vb ?? ''));
      }
      return this.holdingsSortDir * cmp;
    });
    return list;
  }

  setHoldingsSort(key: keyof HoldingRow): void {
    if (this.holdingsSortKey === key) this.holdingsSortDir *= -1;
    else { this.holdingsSortKey = key; this.holdingsSortDir = 1; }
  }

  /** Stable key for a row (for selection and sell qty). Normalized so template and array always match. */
  getRowKey(row: HoldingRow): string {
    const date = String(row.buyDate ?? '').split('T')[0];
    const price = Number(row.buyPriceUsd);
    const total = Math.round(Number(row.totalPurchaseInr));
    return `${date}|${row.type}|${price}|${row.qty}|${total}`;
  }

  isRowSelected(row: HoldingRow): boolean {
    return this.selectedRowKeys.has(this.getRowKey(row));
  }

  /** Select only the currently visible (filtered) rows; replaces selection. */
  selectAllVisible(): void {
    const rows = this.holdingsFilteredSorted;
    const next = new Set<string>();
    const qty: Record<string, number> = {};
    for (const r of rows) {
      const key = this.getRowKey(r);
      next.add(key);
      qty[key] = r.qty;
    }
    this.selectedRowKeys = next;
    this.rowSellQty = qty;
  }

  /** Deselect only the currently visible (filtered) rows. */
  deselectAllVisible(): void {
    const rows = this.holdingsFilteredSorted;
    const next = new Set(this.selectedRowKeys);
    const qty = { ...this.rowSellQty };
    for (const r of rows) {
      const key = this.getRowKey(r);
      next.delete(key);
      qty[key] = 0;
    }
    this.selectedRowKeys = next;
    this.rowSellQty = qty;
  }

  /** Handle select control click: normal click toggles row; shift+click selects range. All state is ours, so tick always matches. */
  onRowSelectClick(row: HoldingRow, event: MouseEvent): void {
    const rows = this.holdingsFilteredSorted;
    const currentKey = this.getRowKey(row);
    const currentIndex = rows.findIndex((r) => this.getRowKey(r) === currentKey);
    if (event.shiftKey && this.lastClickedRowKey != null) {
      const anchorIndex = rows.findIndex((r) => this.getRowKey(r) === this.lastClickedRowKey);
      if (anchorIndex >= 0 && currentIndex >= 0) {
        const from = Math.min(anchorIndex, currentIndex);
        const to = Math.max(anchorIndex, currentIndex);
        const next = new Set<string>();
        const qty: Record<string, number> = {};
        for (let i = from; i <= to; i++) {
          const r = rows[i];
          const key = this.getRowKey(r);
          next.add(key);
          qty[key] = r.qty;
        }
        this.selectedRowKeys = next;
        this.rowSellQty = { ...this.rowSellQty, ...qty };
      } else {
        this.toggleRowSelection(row);
      }
    } else {
      this.toggleRowSelection(row);
    }
    this.lastClickedRowKey = currentKey;
  }

  toggleRowSelection(row: HoldingRow): void {
    const key = this.getRowKey(row);
    if (this.selectedRowKeys.has(key)) {
      this.selectedRowKeys.delete(key);
      this.selectedRowKeys = new Set(this.selectedRowKeys);
      this.rowSellQty = { ...this.rowSellQty, [key]: 0 };
    } else {
      this.selectedRowKeys = new Set(this.selectedRowKeys).add(key);
      this.rowSellQty = { ...this.rowSellQty, [key]: row.qty };
    }
  }

  getSellQty(row: HoldingRow): number {
    const v = this.rowSellQty[this.getRowKey(row)];
    const stored = typeof v === 'number' && !isNaN(v) ? v : 0;
    return Math.min(Math.max(0, stored), row.qty);
  }

  setSellQty(row: HoldingRow, value: string | number): void {
    const key = this.getRowKey(row);
    const n = typeof value === 'number' ? value : parseInt(String(value || '0'), 10);
    const val = !isNaN(n) && n >= 0 ? Math.min(Math.max(0, n), row.qty) : 0;
    this.rowSellQty = { ...this.rowSellQty, [key]: val };
    // Auto-select row when sell qty > 0, auto-deselect when 0
    const next = new Set(this.selectedRowKeys);
    if (val > 0) next.add(key);
    else next.delete(key);
    this.selectedRowKeys = next;
  }

  onSellQtyFocus(event: Event): void {
    const input = event.target as HTMLInputElement | null;
    if (!input) return;
    setTimeout(() => input.select(), 0);
  }

  /** Handle sell qty input: clamp and immediately update the input's displayed value so user cannot see values > row.qty. */
  onSellQtyInput(row: HoldingRow, event: Event): void {
    const input = event.target as HTMLInputElement;
    const raw = input.value;
    const n = parseInt(raw, 10);
    const val = raw === '' || isNaN(n) ? 0 : Math.min(Math.max(0, n), row.qty);
    this.setSellQty(row, val);
    input.value = String(val);
  }

  /** Total qty set to sell across all visible (filtered) rows. */
  get totalSellQty(): number {
    return this.holdingsFilteredSorted.reduce((sum, r) => sum + this.getSellQty(r), 0);
  }

  /** Rows that have sell qty > 0 (for mark-as-sold payload). */
  get markSoldRows(): HoldingRow[] {
    return this.holdingsFilteredSorted.filter((r) => this.getSellQty(r) > 0);
  }

  openMarkSoldModal(): void {
    if (this.totalSellQty <= 0 || this.markSoldRows.length === 0) return;
    this.markSoldError = null;
    const today = new Date();
    this.markSoldSellDate = today.getFullYear() + '-' + String(today.getMonth() + 1).padStart(2, '0') + '-' + String(today.getDate()).padStart(2, '0');
    this.markSoldPriceUsd = this.data?.livePriceUsd ?? null;
    this.showMarkSoldModal = true;
  }

  closeMarkSoldModal(): void {
    this.showMarkSoldModal = false;
    this.markSoldError = null;
  }

  submitMarkSold(): void {
    const sellDate = (this.markSoldSellDate || '').trim().slice(0, 10);
    const price = this.markSoldPriceUsd != null ? Number(this.markSoldPriceUsd) : NaN;
    if (!sellDate || !/^\d{4}-\d{2}-\d{2}$/.test(sellDate)) {
      this.markSoldError = 'Please enter a valid sell date (YYYY-MM-DD).';
      return;
    }
    if (!Number.isFinite(price) || price <= 0) {
      this.markSoldError = 'Please enter a valid sell price (USD).';
      return;
    }
    const items = this.markSoldRows.map((row) => ({
      type: row.type,
      buyDate: String(row.buyDate ?? '').split('T')[0],
      buyPriceUsd: row.buyPriceUsd,
      ...(row.type === 'ESPP' && row.priceBoughtUsd != null ? { priceBoughtUsd: row.priceBoughtUsd } : {}),
      qtyToSell: this.getSellQty(row),
    }));
    this.markSoldInProgress = true;
    this.markSoldError = null;
    this.dashboardService.markSold({ sellDate, priceSellUsd: price, items }).subscribe({
      next: () => {
        this.markSoldInProgress = false;
        this.showMarkSoldModal = false;
        this.selectedRowKeys = new Set();
        this.rowSellQty = {};
        this.canUndoMarkSold = true;
        this.loadHoldings();
        if (this.data) this.load(true);
        if (this.soldLoaded) this.loadSold();
        this.cdr.detectChanges();
      },
      error: (err: { error?: { error?: string }; message?: string }) => {
        this.markSoldInProgress = false;
        this.markSoldError = err?.error?.error || err?.message || 'Failed to mark as sold.';
        this.cdr.detectChanges();
      },
    });
  }

  undoLastMarkSold(): void {
    this.undoMarkSoldError = null;
    this.undoMarkSoldInProgress = true;
    this.dashboardService.undoMarkSold().subscribe({
      next: () => {
        this.undoMarkSoldInProgress = false;
        this.canUndoMarkSold = false;
        this.loadHoldings();
        if (this.data) this.load(true);
        if (this.soldLoaded) this.loadSold();
        this.cdr.detectChanges();
      },
      error: (err: { status?: number; error?: { error?: string }; message?: string }) => {
        this.undoMarkSoldInProgress = false;
        this.undoMarkSoldError = err?.status === 404 ? 'Nothing to undo.' : (err?.error?.error || err?.message || 'Undo failed.');
        if (err?.status === 404) this.canUndoMarkSold = false;
        this.cdr.detectChanges();
      },
    });
  }

  /** Totals for selected rows only, scaled by sell qty per row (for sell-strategy simulation). */
  get holdingsSelectedTotals(): { totalValueTodayInr: number; totalTaxToPayInr: number; totalNetInAccountInr: number; selectedCount: number } {
    const rows = this.holdingsFilteredSorted;
    let totalValueTodayInr = 0;
    let totalTaxToPayInr = 0;
    let totalNetInAccountInr = 0;
    let selectedCount = 0;
    for (const r of rows) {
      if (!this.selectedRowKeys.has(this.getRowKey(r))) continue;
      selectedCount++;
      const sellQty = this.getSellQty(r);
      const ratio = r.qty > 0 ? Math.min(sellQty, r.qty) / r.qty : 0;
      const valueTodayRow = r.netIfSellTodayInr + r.taxToPayInr;
      totalValueTodayInr += valueTodayRow * ratio;
      totalTaxToPayInr += r.taxToPayInr * ratio;
      totalNetInAccountInr += r.netIfSellTodayInr * ratio;
    }
    return { totalValueTodayInr, totalTaxToPayInr, totalNetInAccountInr, selectedCount };
  }

  onColDragStart(event: DragEvent, index: number): void {
    this.draggedColIndex = index;
    if (event.dataTransfer) {
      event.dataTransfer.effectAllowed = 'move';
      event.dataTransfer.setData('text/plain', String(index));
    }
  }

  onColDragEnd(): void {
    this.draggedColIndex = null;
  }

  onColDragOver(event: DragEvent): void {
    event.preventDefault();
    if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
  }

  onColDrop(event: DragEvent, targetIndex: number): void {
    event.preventDefault();
    if (this.draggedColIndex == null || this.draggedColIndex === targetIndex) return;
    const order = this.holdingsColumnOrder.slice();
    const [moved] = order.splice(this.draggedColIndex, 1);
    order.splice(targetIndex, 0, moved);
    this.holdingsColumnOrder = order;
    this.saveHoldingsColumnOrder();
    this.draggedColIndex = null;
  }

  getColLabel(key: (typeof HOLDING_COLS)[number]): string {
    return HOLDING_COL_DEFS.find((c) => c.key === key)?.label ?? key;
  }

  /** Display value for a cell (formatted). */
  formatCell(row: HoldingRow, key: keyof HoldingRow): string {
    const v = row[key];
    if (key === 'buyPriceUsd') {
      const tdsOrBuy = this.formatUsd(Number(v));
      if (row.type === 'ESPP' && row.priceBoughtUsd != null) {
        return tdsOrBuy + '(' + this.formatUsd(row.priceBoughtUsd) + ')';
      }
      return tdsOrBuy;
    }
    if (key === 'type') return this.typeLabel(String(v));
    if (key === 'totalPurchaseInr' || key === 'netIfSellTodayInr' || key === 'taxToPayInr') return '₹ ' + this.formatInr(Number(v));
    if (key === 'buyDate') return this.formatDate(String(v));
    if (key === 'qty') return String(v);
    if (key === 'profitPercent' || key === 'taxPercent') return (Number(v) ?? 0).toFixed(1) + '%';
    return String(v ?? '');
  }

  private escapeCsvCell(s: string): string {
    const str = String(s ?? '');
    if (/[",\r\n]/.test(str)) return '"' + str.replace(/"/g, '""') + '"';
    return str;
  }

  /** Plain number for CSV export: no commas, no currency symbols. */
  private formatForExport(value: number, decimals = 2): string {
    const n = Number(value);
    return isNaN(n) ? '' : n.toFixed(decimals);
  }

  private downloadCsv(content: string, filename: string): void {
    const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  /** Export currently visible (filtered/sorted) holdings to CSV. No commas, no rupee/currency symbols. */
  exportHoldingsCsv(): void {
    const rows = this.holdingsFilteredSorted;
    const cols = this.holdingsColumnOrder;
    const header = cols.map((k) => this.escapeCsvCell(this.getColLabel(k))).join(',');
    const lines = [header];
    for (const row of rows) {
      const cells = cols.map((k) => {
        const v = row[k];
        if (k === 'buyPriceUsd' || k === 'totalPurchaseInr' || k === 'netIfSellTodayInr' || k === 'taxToPayInr') return this.escapeCsvCell(this.formatForExport(Number(v)));
        if (k === 'type') return this.escapeCsvCell(this.typeLabel(String(v)));
        if (k === 'buyDate') return this.escapeCsvCell(this.formatDate(String(v)));
        if (k === 'qty') return this.escapeCsvCell(String(v));
        if (k === 'profitPercent' || k === 'taxPercent') return this.escapeCsvCell(this.formatForExport(Number(v), 1));
        return this.escapeCsvCell(String(v ?? ''));
      });
      lines.push(cells.join(','));
    }
    const csv = lines.join('\r\n');
    const filename = `holdings-${new Date().toISOString().slice(0, 10)}.csv`;
    this.downloadCsv(csv, filename);
  }

  /** Export currently visible (filtered/sorted) sold shares to CSV. No commas, no rupee/currency symbols. */
  exportSoldCsv(): void {
    const rows = this.soldFilteredSorted;
    const cols = this.soldCols;
    const header = cols.map((k) => this.escapeCsvCell(this.soldColLabels[k])).join(',');
    const lines = [header];
    for (const row of rows) {
      const cells = cols.map((col) => {
        const v = (row as unknown as Record<string, unknown>)[col];
        if (col === 'sellDate' || col === 'buyDate') return this.escapeCsvCell(this.formatDate(String(v ?? '')));
        if (col === 'type') return this.escapeCsvCell(this.typeLabel(String(v ?? '')));
        if (col === 'qtySold') return this.escapeCsvCell(String(v ?? ''));
        if (col === 'priceBoughtUsd' || col === 'priceSellUsd' || ['buyValueInr', 'sellValueInr', 'gainBeforeTaxInr', 'taxPaidInr'].includes(col)) return this.escapeCsvCell(this.formatForExport(Number(v)));
        if (col === 'profitPercent' || col === 'taxPercent') return this.escapeCsvCell(this.formatForExport(Number(v), 1));
        return this.escapeCsvCell(String(v ?? ''));
      });
      lines.push(cells.join(','));
    }
    const csv = lines.join('\r\n');
    const filename = `sold-shares-${new Date().toISOString().slice(0, 10)}.csv`;
    this.downloadCsv(csv, filename);
  }
}
