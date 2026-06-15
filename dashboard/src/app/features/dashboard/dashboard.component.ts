import {
  AfterViewInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  HostListener,
  OnDestroy,
  OnInit,
  ViewChild,
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { ActivatedRoute, Router } from '@angular/router';
import { Chart } from 'chart.js/auto';
import 'chartjs-adapter-date-fns';
import { CandlestickController, CandlestickElement, OhlcController, OhlcElement } from 'chartjs-chart-financial';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import { forkJoin, Subscription } from 'rxjs';
import { DashboardService } from '../../core/services/dashboard.service';

/** Dashboard tabs, each mapped to a URL path segment (e.g. /holdings). */
export type DashboardTab = 'holdings' | 'sold' | 'playground' | 'data' | 'financial' | 'tax' | 'icici';
export const DASHBOARD_TABS: readonly DashboardTab[] = ['holdings', 'sold', 'playground', 'financial', 'icici', 'tax', 'data'];

Chart.register(ChartDataLabels, CandlestickController, CandlestickElement, OhlcController, OhlcElement);
import {
  BreezeAccountStatus,
  BreezePortfolioHoldingsDisplayRow,
  BreezePortfolioHoldingsTotals,
  BreezePortfolioSortCol,
  DashboardResponse,
  HoldingRow,
  LivePriceDayTickerItem,
  LivePriceHistoryPoint,
  SoldRow,
} from '../../core/models/dashboard.types';

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
import { OverviewTimeCardComponent } from './overview-time-card.component';
import { LivePriceExtendedHintComponent } from './live-price-extended-hint.component';
import { FinancialPlanningComponent } from './financial-planning/financial-planning.component';

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
  imports: [CommonModule, FormsModule, StatCardComponent, OverviewTimeCardComponent, LivePriceExtendedHintComponent, FinancialPlanningComponent],
  templateUrl: './dashboard.component.html',
  styleUrl: './dashboard.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
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
  private lastShownInrChangeBreakdown: { priceEffectInr: number; fxEffectInr: number; prevRate: number; curRate: number } | null = null;
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
  /** Override current price (USD) for Holdings table simulation; null = use live price. */
  holdingsSimulatePriceUsd: number | null = null;
  /** Override USD→INR for Holdings table simulation; null = use dashboard rate. */
  holdingsSimulateUsdToInr: number | null = null;
  /** Raw text in "unvested shares" simulate field (parsed on blur). */
  holdingsUnvestedGrossText = '';
  /** Gross unvested share count after last blur commit; null if empty/invalid. */
  holdingsUnvestedGrossShares: number | null = null;
  /** US withholding on gross proceeds (NVIDIA RSU / US equity). */
  readonly UNVESTED_US_WITHHOLDING_FRACTION = 0.3714;
  /** India: short-term vs long-term capital gains on amount after US tax (taxable gain × rate). */
  holdingsUnvestedIndiaCgMode: 'stcg' | 'ltcg' = 'stcg';
  /** Indian STCG % on taxable gain (default aligns with common slab for foreign equity). */
  holdingsUnvestedIndiaStcgPct = 30;
  /** Indian LTCG % on taxable gain (editable; rules vary — set to match your situation). */
  holdingsUnvestedIndiaLtcgPct = 12.5;
  /** Last row clicked (without shift) for shift-click range selection. */
  private lastClickedRowKey: string | null = null;
  /** Tab: Holdings vs Sold Shares vs Playground vs Financial planning */
  activeTab: DashboardTab = 'holdings';
  private routeSub: Subscription | null = null;

  /** ICICI Direct Breeze — multi-account support. */
  breezeAccounts: Record<string, BreezeAccountStatus> = {};
  breezeAccountIds: string[] = [];
  breezeStatusLoading = false;
  breezeConnectError: string | null = null;
  /** Per-account raw API data (merged for display). */
  breezePortfolioDataByAcct: Record<string, unknown> = {};
  breezePortfolioExchangeCode = 'NSE';
  breezePortfolioFromDate = '';
  breezePortfolioToDate = '';
  breezePortfolioStockCode = '';
  breezePortfolioTableFilter = '';
  breezePortfolioSortCol: BreezePortfolioSortCol = 'stockCode';
  breezePortfolioSortDir: 1 | -1 = 1;
  breezeDataLoading = false;
  breezeDataError: string | null = null;

  taxConfig: any = null;
  taxConfigRaw = '';
  taxConfigLoading = false;
  taxConfigError: string | null = null;
  taxConfigSaving = false;
  taxConfigSaved = false;
  taxDocOutput = '';
  taxDocFyLabel = '';
  taxDocLoading = false;
  taxDocError: string | null = null;
  taxDocSelectedFy: number = new Date().getFullYear();
  readonly taxDocFyOptions: number[] = (() => {
    const cur = new Date().getFullYear();
    const opts: number[] = [];
    for (let y = cur + 1; y >= cur - 5; y--) opts.push(y);
    return opts;
  })();
  /** FA-A3 export (Holdings section): selected assessment year + in-progress flag. */
  faExportFy: number = new Date().getFullYear();
  faExportInProgress = false;
  faExportError: string | null = null;
  /** Cached sanitized URL for Data tab iframe (set once to avoid reload on every change detection). */
  webAppIframeSrc!: SafeResourceUrl;
  soldRows: SoldRow[] = [];
  soldLoading = false;
  private soldLoaded = false;
  soldSortKey: keyof SoldRow | '' = 'sellDate';
  soldSortDir: 1 | -1 = -1;
  soldFilter: Record<string, string> = {};
  /** Sold filter by Indian financial year (by sell date). 'all' = no FY filter; otherwise FY start year (e.g. 2024 = FY 2024–25). */
  soldFyFilter: number | 'all' = 'all';
  readonly soldCols = SOLD_COLS;
  readonly soldColLabels = SOLD_COL_LABELS;
  /** Selected sold row keys for summary card (key = getSoldRowKey(row)). */
  selectedSoldKeys = new Set<string>();
  /** Last sold row clicked (without shift) for shift-click range selection. */
  private lastClickedSoldRowKey: string | null = null;

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

  /** Multi-price playground: selected lots (independent of Holdings tab). */
  playMultiSelectedKeys = new Set<string>();
  playMultiRowQty: Record<string, number> = {};
  /** Up to five extra USD prices to compare with live (same tax slab per lot as dashboard). */
  readonly playMultiPriceSlotIndices = [0, 1, 2, 3, 4] as const;
  playMultiScenarioPrices: (number | null)[] = [null, null, null, null, null];
  /** Multi-price table — Buy $ column header tooltip */
  readonly playMultiBuyColumnTitle =
    'RSU: grant USD price. ESPP: TDS/FMV (tax cost) vs what you paid; INR cost uses TDS × ₹ on the buy date. Tax in Value/Tax/Net compares today’s ₹/USD to that INR cost—not live USD vs “paid” alone.';

  /** Live price polling: history for chart (max 7 days, max 5000 points) */
  livePriceHistory: LivePriceHistoryPoint[] = [];
  /** Cached per-day ticker chips; rebuilt only when history / open price changes (not on every CD). */
  livePriceDayTickerItems: LivePriceDayTickerItem[] = [];
  /** Per-day (ET) open price for live chart: date key (YYYY-MM-DD) -> first price that day. Used for "diff from open" in tooltip. */
  private livePriceOpenByDay = new Map<string, number>();
  /** Data indices that are the first point of their day (ET); for per-day Start labels. */
  private livePriceFirstOfDayIndices = new Set<number>();
  /** Data indices that are the last point of their day (ET); for per-day End labels. */
  private livePriceLastOfDayIndices = new Set<number>();
  /** Points currently plotted on the line chart (null entries = day-boundary gaps); used by tooltip callbacks. */
  private livePriceLinePoints: ({ timestamp: number; livePriceUsd: number; usdToInrRate: number } | null)[] = [];
  /** Overall average of the plotted line points (for the dashed Average line + tooltip). */
  private livePriceLineAvg = 0;
  /** Max plotted points per day for the line chart (downsamples dense intraday data so 2 weeks stays responsive). */
  private static readonly LIVE_PRICE_LINE_MAX_PER_DAY = 400;
  /** Whether US market is open (from /api/market-status). */
  marketOpen = false;
  /** True when in pre-market (4–9:30 AM ET) or post-market (4–8 PM ET). */
  isPreMarketSession = false;
  isPostMarketSession = false;
  /** Next market open (9:30 AM ET), close (4:00 PM ET), pre-market start (4:00 AM ET) in Unix ms; from /api/market-status */
  marketNextOpenMs: number | null = null;
  marketNextCloseMs: number | null = null;
  marketNextPreMarketStartMs: number | null = null;
  /** Show data label when |diff from open %| is at least this (e.g. 1.6). */
  livePriceChartMode: 'line' | 'candlestick' = 'line';
  /** Visible range for the live price chart, by most-recent ET session days. Default 1 week. */
  livePriceRange: '1d' | '1w' | '2w' = '1w';
  /** Calendar days fetched for each range option (server picks bucket resolution to fit). */
  private static readonly LIVE_PRICE_RANGE_DAYS: Record<'1d' | '1w' | '2w', number> = { '1d': 1, '1w': 7, '2w': 14 };
  /** True while clearing the live price history (graph) from the backend. */
  clearGraphInProgress = false;
  private livePricePollingInterval: ReturnType<typeof setInterval> | null = null;
  private static readonly LIVE_PRICE_POLL_MS = 14000;
  private static readonly LIVE_PRICE_HISTORY_DAYS = 14;
  private static readonly LIVE_PRICE_HISTORY_DAYS_MS = DashboardComponent.LIVE_PRICE_HISTORY_DAYS * 24 * 60 * 60 * 1000;
  private static readonly LIVE_PRICE_HISTORY_MAX = 50000;

  constructor(
    private dashboardService: DashboardService,
    private cdr: ChangeDetectorRef,
    private sanitizer: DomSanitizer,
    private router: Router,
    private route: ActivatedRoute,
  ) {}

  ngOnInit(): void {
    this.webAppIframeSrc = this.sanitizer.bypassSecurityTrustResourceUrl('assets/web-app/index.html');
    this.loadHoldingsColumnOrder();
    this.initBreezePortfolioDefaultDates();
    this.handleBreezeCallbackParams();
    // Drive the active tab from the URL (:tab); applies side-effects (data loads, chart lifecycle) on each change.
    this.routeSub = this.route.paramMap.subscribe((pm) => {
      const raw = (pm.get('tab') || '').toLowerCase();
      const tab = (DASHBOARD_TABS as readonly string[]).includes(raw) ? (raw as DashboardTab) : 'holdings';
      this.applyTab(tab);
    });
    this.load();
    this.updateCanUndoMarkSold();
  }

  /** If redirected back from ICICI callback, switch to ICICI tab and show status. */
  private handleBreezeCallbackParams(): void {
    const params = new URLSearchParams(window.location.search);
    const tab = params.get('tab');
    const connected = params.get('breeze_connected');
    const error = params.get('breeze_error');
    if (tab === 'icici') {
      if (error) {
        this.breezeConnectError = error === 'no_token' ? 'No session token received from ICICI.' : error;
      }
      // Move to the dedicated ICICI URL (clears the callback query params); applyTab loads breeze status.
      this.router.navigate(['/icici'], { replaceUrl: true });
    }
  }

  /** Default ICICI portfolio holdings range: 2015-01-01 local → now (datetime-local). */
  private initBreezePortfolioDefaultDates(): void {
    const pad = (n: number) => String(n).padStart(2, '0');
    this.breezePortfolioFromDate = '2015-01-01T00:00';
    const d = new Date();
    this.breezePortfolioToDate = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
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

  /** When Data tab iframe adds/updates/deletes a row, refresh dashboard so holdings/totals reflect it. */
  @HostListener('window:message', ['$event'])
  onWindowMessage(event: MessageEvent): void {
    if (event?.data?.type === 'nvSharesDataChanged') {
      this.load(true, true);
    }
  }

  load(refresh = false, silent = false): void {
    if (!silent) {
      this.loading = true;
      this.error = null;
    }
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
        if (res.openPriceUsd != null && res.openPriceUsd > 0 && res.livePriceUsd != null) {
          const d = res.livePriceUsd - res.openPriceUsd;
          if (Math.abs(d) >= 0.01) {
            this.lastShownLivePriceMovement = { diffUsd: d, diffPct: (d / res.openPriceUsd) * 100 };
          }
        }
        this.lastRefreshedAt = new Date();
        this.loading = false;
        this.loadHoldings();
        if (this.soldLoaded) this.loadSold();
        this.updateCanUndoMarkSold();
        this.loadLivePriceHistoryFromDb();
        this.startLivePricePolling();
        this.cdr.markForCheck();
      },
      error: (err: { error?: { error?: string }; message?: string }) => {
        if (!silent) {
          this.error = err?.error?.error || err?.message || 'Failed to load dashboard';
        }
        this.loading = false;
        this.cdr.markForCheck();
      },
    });
    if (!silent) {
      this.cdr.markForCheck();
    }
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
    const fetchDays = this.livePriceRangeFetchDays();
    this.dashboardService.getLivePriceHistory(fetchDays).subscribe({
      next: (stored) => {
        const combined = stored.length ? [...stored] : [];
        combined.push(currentPoint);
        combined.sort((a, b) => a.timestamp - b.timestamp);
        const cutoff = now - fetchDays * 24 * 60 * 60 * 1000;
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
          if (this.data!.openPriceUsd == null || this.data!.openPriceUsd <= 0) {
            const diffUsd = this.data!.livePriceUsd - prev.livePriceUsd;
            if (prev.livePriceUsd !== 0 && Math.abs(diffUsd) >= 0.01) {
              this.lastShownLivePriceMovement = { diffUsd, diffPct: (diffUsd / prev.livePriceUsd) * 100 };
            }
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
        this.refreshLivePriceDayTickerCache();
        this.initOrUpdateLivePriceChart();
        this.cdr.markForCheck();
      },
      error: () => {
        this.livePriceHistory = [currentPoint];
        this.refreshLivePriceDayTickerCache();
        this.initOrUpdateLivePriceChart();
        this.cdr.markForCheck();
      },
    });
  }

  private startLivePricePolling(): void {
    this.stopLivePricePolling();
    const poll = (): void => {
      this.dashboardService.getMarketStatus().subscribe({
        next: (status) => {
          this.marketOpen = status.marketOpen;
          this.isPreMarketSession = status.isPreMarketSession ?? false;
          this.isPostMarketSession = status.isPostMarketSession ?? false;
          this.marketNextOpenMs = status.nextOpen ?? null;
          this.marketNextCloseMs = status.nextClose ?? null;
          this.marketNextPreMarketStartMs = status.nextPreMarketStart ?? null;
          this.cdr.markForCheck();
          // Always fetch live price so we get pre-market/post-market price when in those sessions
          this.fetchAndPushLivePrice();
        },
        error: () => {
          this.marketOpen = false;
          this.isPreMarketSession = false;
          this.isPostMarketSession = false;
          this.marketNextOpenMs = null;
          this.marketNextCloseMs = null;
          this.marketNextPreMarketStartMs = null;
          this.cdr.markForCheck();
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
        const roundedUsd = Math.round(totalValueUsd * 100) / 100;
        const roundedInr = Math.round(totalValueInr * 100) / 100;
        const prevInr = this.data.totalValueInr;
        const ratio = prevInr > 0 ? roundedInr / prevInr : 1;
        const newTax = Math.min(
          Math.max(0, Math.round(this.data.totalTaxToPay * ratio * 100) / 100),
          roundedInr
        );
        const newNet = Math.round((roundedInr - newTax) * 100) / 100;
        this.data = {
          ...this.data,
          livePriceUsd: res.livePriceUsd,
          usdToInrRate: res.usdToInrRate,
          ...(res.openPriceUsd != null && { openPriceUsd: res.openPriceUsd }),
          preMarketPriceUsd: res.preMarketPriceUsd ?? undefined,
          postMarketPriceUsd: res.postMarketPriceUsd ?? undefined,
          totalValueUsd: roundedUsd,
          totalValueInr: roundedInr,
          unrealisedProfitAfterTax: Math.round(this.data.unrealisedProfitAfterTax * ratio * 100) / 100,
          unrealisedProfitBeforeTax: Math.round(this.data.unrealisedProfitBeforeTax * ratio * 100) / 100,
          totalTaxToPay: newTax,
          netInBankIfSellNow: newNet,
        };
        this.lastRefreshedAt = new Date(res.lastUpdated);
        const now = Date.now();
        const point = { timestamp: now, livePriceUsd: res.livePriceUsd, usdToInrRate: res.usdToInrRate };
        if (this.marketOpen) {
          this.livePriceHistory.push(point);
          const cutoff = now - DashboardComponent.LIVE_PRICE_HISTORY_DAYS_MS;
          this.livePriceHistory = this.livePriceHistory.filter((p) => p.timestamp >= cutoff);
          if (this.livePriceHistory.length > DashboardComponent.LIVE_PRICE_HISTORY_MAX) {
            this.livePriceHistory = this.livePriceHistory.slice(-DashboardComponent.LIVE_PRICE_HISTORY_MAX);
          }
          this.dashboardService.appendLivePriceHistory(point).subscribe({ error: () => { /* persist best-effort */ } });
        }
        this.refreshLivePriceDayTickerCache();
        this.initOrUpdateLivePriceChart();
        this.updateLastShownMovements();
        this.cdr.markForCheck();
      },
      error: () => { /* keep last values */ },
    });
  }

  /** Clear stored graph data (live price history) and reset the chart. */
  clearGraph(): void {
    if (!confirm('Clear all stored graph data? The chart will be empty until new points are recorded.')) {
      return;
    }
    this.clearGraphInProgress = true;
    this.cdr.markForCheck();
    this.dashboardService.clearLivePriceHistory().subscribe({
      next: () => {
        if (this.livePriceChart) {
          this.livePriceChart.destroy();
          this.livePriceChart = null;
        }
        this.livePriceHistory = [];
        this.refreshLivePriceDayTickerCache();
        this.clearGraphInProgress = false;
        this.cdr.markForCheck();
      },
      error: () => {
        this.clearGraphInProgress = false;
        this.cdr.markForCheck();
      },
    });
  }

  loadBreezeStatus(): void {
    this.breezeStatusLoading = true;
    this.breezeConnectError = null;
    this.dashboardService.getBreezeStatusAll().subscribe({
      next: (resp) => {
        this.breezeAccounts = resp.accounts || {};
        this.breezeAccountIds = Object.keys(this.breezeAccounts).sort();
        this.breezeStatusLoading = false;
        const anyConnected = this.breezeAccountIds.some((id) => this.breezeAccounts[id]?.connected);
        if (anyConnected) {
          this.fetchBreezePortfolioHoldings();
        }
        this.cdr.markForCheck();
      },
      error: () => {
        this.breezeAccounts = {};
        this.breezeAccountIds = [];
        this.breezeStatusLoading = false;
        this.cdr.markForCheck();
      },
    });
  }

  disconnectBreeze(acct: string): void {
    this.dashboardService.postBreezeDisconnect(acct).subscribe({
      next: () => {
        this.breezePortfolioDataByAcct[acct] = null;
        this.loadBreezeStatus();
      },
    });
  }

  get breezeAnyConfigured(): boolean {
    return this.breezeAccountIds.some((id) => this.breezeAccounts[id]?.configured);
  }

  get breezeAnyConnected(): boolean {
    return this.breezeAccountIds.some((id) => this.breezeAccounts[id]?.connected);
  }

  fetchBreezePortfolioHoldings(): void {
    const ex = this.breezePortfolioExchangeCode.trim();
    if (!ex) {
      this.breezeDataError = 'Choose an exchange (NSE or NFO).';
      this.cdr.markForCheck();
      return;
    }
    const connectedAccts = this.breezeAccountIds.filter((id) => this.breezeAccounts[id]?.connected);
    if (connectedAccts.length === 0) return;
    this.breezeDataLoading = true;
    this.breezeDataError = null;
    const fd = this.breezePortfolioFromDate?.trim();
    const td = this.breezePortfolioToDate?.trim();
    const sc = this.breezePortfolioStockCode?.trim();
    const reqs: Record<string, ReturnType<typeof this.dashboardService.getBreezePortfolioHoldings>> = {};
    for (const acct of connectedAccts) {
      reqs[acct] = this.dashboardService.getBreezePortfolioHoldings({
        exchangeCode: ex,
        ...(fd ? { fromDate: new Date(fd).toISOString() } : {}),
        ...(td ? { toDate: new Date(td).toISOString() } : {}),
        ...(sc ? { stockCode: sc } : {}),
        acct,
      });
    }
    forkJoin(reqs).subscribe({
      next: (results) => {
        this.breezePortfolioDataByAcct = results;
        this.breezeDataLoading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        this.breezeDataLoading = false;
        this.breezeDataError = err?.error?.error || err?.message || 'Request failed';
        this.cdr.markForCheck();
      },
    });
  }

  /** Are there any portfolio results loaded? */
  get breezeHasPortfolioData(): boolean {
    return Object.values(this.breezePortfolioDataByAcct).some((d) => d != null);
  }

  /** Portfolio holdings: note + filtered/sorted rows + footer totals. */
  breezePortfolioHoldingsView(): {
    note: string | null;
    rows: BreezePortfolioHoldingsDisplayRow[];
    totals: BreezePortfolioHoldingsTotals;
  } {
    const rows = this.breezePortfolioHoldingsTableRows();
    return {
      note: this.breezePortfolioHoldingsApiNote(),
      rows,
      totals: this.breezePortfolioHoldingsTotalsFromRows(rows),
    };
  }

  setBreezePortfolioSort(col: BreezePortfolioSortCol): void {
    if (this.breezePortfolioSortCol === col) {
      this.breezePortfolioSortDir = (this.breezePortfolioSortDir === 1 ? -1 : 1) as 1 | -1;
    } else {
      this.breezePortfolioSortCol = col;
      this.breezePortfolioSortDir = 1;
    }
    this.cdr.markForCheck();
  }

  onBreezePortfolioTableFilterChange(): void {
    this.cdr.markForCheck();
  }

  breezePortfolioSortIndicator(col: BreezePortfolioSortCol): string {
    if (this.breezePortfolioSortCol !== col) return '';
    return this.breezePortfolioSortDir === 1 ? '↑' : '↓';
  }

  private breezePortfolioHoldingsBaseRows(): BreezePortfolioHoldingsDisplayRow[] {
    const out: BreezePortfolioHoldingsDisplayRow[] = [];
    for (const [acct, data] of Object.entries(this.breezePortfolioDataByAcct)) {
      if (data == null || typeof data !== 'object') continue;
      const root = data as Record<string, unknown>;
      const success = root['Success'];
      if (!Array.isArray(success) || success.length === 0) continue;
      for (const raw of success) {
        if (raw == null || typeof raw !== 'object') continue;
        const row = raw as Record<string, unknown>;
        const stockCode = this.breezePortfolioStringField(row, ['stock_code', 'stockCode']);
        const qty = this.breezePortfolioParseNum(row, ['quantity', 'qty', 'Quantity']);
        const avg = this.breezePortfolioParseNum(row, ['average_price', 'averagePrice', 'avg_price']);
        const bookedRaw = this.breezePortfolioRawField(row, [
          'booked_profit_loss', 'bookedProfitLoss', 'realized_profit', 'booked_profit',
        ]);
        const curPx = this.breezePortfolioParseNum(row, ['current_market_price', 'currentMarketPrice', 'ltp', 'last_price']);
        const invested = avg != null && qty != null ? avg * qty : null;
        const currentAmtNum = curPx != null && qty != null ? curPx * qty : null;
        const bookedPnlNum = this.breezePortfolioParseNumValue(bookedRaw);
        let plKind: BreezePortfolioHoldingsDisplayRow['plKind'] = 'na';
        let plStr = '—';
        let plPctStr = '—';
        let plNumValue: number | null = null;
        let plPctNumValue: number | null = null;
        if (invested != null && currentAmtNum != null && Number.isFinite(invested) && Number.isFinite(currentAmtNum)) {
          plNumValue = currentAmtNum - invested;
          if (Math.abs(plNumValue) < 1e-6) plKind = 'zero';
          else if (plNumValue > 0) plKind = 'pos';
          else plKind = 'neg';
          plStr = this.breezePortfolioFormatMoney(plNumValue);
          if (Math.abs(invested) > 1e-9) {
            plPctNumValue = (plNumValue / invested) * 100;
            plPctStr = `${plPctNumValue.toFixed(1)}%`;
          }
        }
        out.push({
          stockCode: stockCode ?? '—',
          quantity: this.breezePortfolioFormatQty(qty),
          avgPrice: this.breezePortfolioFormatMoney(avg),
          bookedPnl: this.breezePortfolioFormatBooked(bookedRaw),
          currentPrice: this.breezePortfolioFormatMoney(curPx),
          investedAmt: this.breezePortfolioFormatMoney(invested),
          currentAmt: this.breezePortfolioFormatMoney(currentAmtNum),
          pl: plStr,
          plPct: plPctStr,
          plKind,
          qtyNum: qty,
          avgNum: avg,
          bookedPnlNum,
          investedNum: invested,
          currentNum: currentAmtNum,
          plNum: plNumValue,
          plPctNum: plPctNumValue,
          curPxNum: curPx,
          account: acct,
        });
      }
    }
    return out;
  }

  private breezePortfolioHoldingsTableRows(): BreezePortfolioHoldingsDisplayRow[] {
    const base = this.breezePortfolioHoldingsBaseRows();
    const q = this.breezePortfolioTableFilter.trim().toLowerCase();
    let filtered = base;
    if (q) {
      filtered = base.filter(
        (row) =>
          row.stockCode.toLowerCase().includes(q) ||
          row.quantity.toLowerCase().includes(q) ||
          row.avgPrice.toLowerCase().includes(q) ||
          row.bookedPnl.toLowerCase().includes(q) ||
          row.currentPrice.toLowerCase().includes(q) ||
          row.investedAmt.toLowerCase().includes(q) ||
          row.currentAmt.toLowerCase().includes(q) ||
          row.pl.toLowerCase().includes(q) ||
          row.plPct.toLowerCase().includes(q) ||
          row.account.toLowerCase().includes(q)
      );
    }
    const col = this.breezePortfolioSortCol;
    const dir = this.breezePortfolioSortDir;
    const nullLast = (n: number | null): number =>
      n == null || !Number.isFinite(n) ? (dir === 1 ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY) : n;
    return [...filtered].sort((a, b) => {
      let cmp = 0;
      switch (col) {
        case 'account':
          cmp = a.account.localeCompare(b.account);
          break;
        case 'stockCode':
          cmp = a.stockCode.localeCompare(b.stockCode, undefined, { sensitivity: 'base' });
          break;
        case 'qty':
          cmp = nullLast(a.qtyNum) - nullLast(b.qtyNum);
          break;
        case 'avgPrice':
          cmp = nullLast(a.avgNum) - nullLast(b.avgNum);
          break;
        case 'bookedPnl':
          cmp = nullLast(a.bookedPnlNum) - nullLast(b.bookedPnlNum);
          break;
        case 'currentPrice':
          cmp = nullLast(a.curPxNum) - nullLast(b.curPxNum);
          break;
        case 'invested':
          cmp = nullLast(a.investedNum) - nullLast(b.investedNum);
          break;
        case 'current':
          cmp = nullLast(a.currentNum) - nullLast(b.currentNum);
          break;
        case 'pl':
          cmp = nullLast(a.plNum) - nullLast(b.plNum);
          break;
        case 'plPct':
          cmp = nullLast(a.plPctNum) - nullLast(b.plPctNum);
          break;
        default:
          cmp = 0;
      }
      return cmp * dir;
    });
  }

  private breezePortfolioHoldingsTotalsFromRows(rows: BreezePortfolioHoldingsDisplayRow[]): BreezePortfolioHoldingsTotals {
    let inv = 0;
    let cur = 0;
    let pl = 0;
    for (const r of rows) {
      if (r.investedNum != null && Number.isFinite(r.investedNum)) inv += r.investedNum;
      if (r.currentNum != null && Number.isFinite(r.currentNum)) cur += r.currentNum;
      if (r.plNum != null && Number.isFinite(r.plNum)) pl += r.plNum;
    }
    let plKind: BreezePortfolioHoldingsTotals['plKind'] = 'na';
    if (Math.abs(pl) < 1e-6) plKind = 'zero';
    else if (pl > 0) plKind = 'pos';
    else plKind = 'neg';
    const plPctVal = Math.abs(inv) > 1e-9 ? (pl / inv) * 100 : null;
    return {
      invested: this.breezePortfolioFormatMoney(inv),
      current: this.breezePortfolioFormatMoney(cur),
      pl: this.breezePortfolioFormatMoney(pl),
      plPct: plPctVal != null ? `${plPctVal.toFixed(1)}%` : '—',
      plKind,
    };
  }

  private breezePortfolioHoldingsApiNote(): string | null {
    const notes: string[] = [];
    for (const [acct, data] of Object.entries(this.breezePortfolioDataByAcct)) {
      if (data == null || typeof data !== 'object') continue;
      const root = data as Record<string, unknown>;
      const err = root['Error'] ?? root['error'];
      if (err !== null && err !== undefined && err !== '') {
        const es = typeof err === 'object' ? JSON.stringify(err) : String(err);
        if (es && es !== 'null' && es !== 'undefined') notes.push(`Account ${acct}: ${es}`);
      }
    }
    return notes.length ? notes.join(' · ') : null;
  }

  trackPortfolioHoldingRow(_index: number, row: BreezePortfolioHoldingsDisplayRow): string {
    return `${row.account}-${row.stockCode}-${_index}`;
  }

  private breezePortfolioStringField(row: Record<string, unknown>, keys: string[]): string | null {
    const v = this.breezePortfolioRawField(row, keys);
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    return s.length ? s : null;
  }

  private breezePortfolioRawField(row: Record<string, unknown>, keys: string[]): unknown {
    const want = new Set(keys.map((k) => k.toLowerCase()));
    for (const [k, v] of Object.entries(row)) {
      if (want.has(k.toLowerCase())) return v;
    }
    return undefined;
  }

  private breezePortfolioParseNum(row: Record<string, unknown>, keys: string[]): number | null {
    const v = this.breezePortfolioRawField(row, keys);
    return this.breezePortfolioParseNumValue(v);
  }

  private breezePortfolioParseNumValue(v: unknown): number | null {
    if (v === null || v === undefined || v === '') return null;
    if (typeof v === 'number') return Number.isFinite(v) ? v : null;
    const s = String(v).replace(/,/g, '').trim();
    if (!s.length) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  private breezePortfolioFormatMoney(n: number | null): string {
    if (n == null || !Number.isFinite(n)) return '—';
    return n.toLocaleString('en-IN', { maximumFractionDigits: 2, minimumFractionDigits: 2 });
  }

  private breezePortfolioFormatQty(n: number | null): string {
    if (n == null || !Number.isFinite(n)) return '—';
    if (Number.isInteger(n) || Math.abs(n - Math.round(n)) < 1e-9) return String(Math.round(n));
    return n.toLocaleString('en-IN', { maximumFractionDigits: 4, minimumFractionDigits: 0 });
  }

  private breezePortfolioFormatBooked(v: unknown): string {
    if (v === null || v === undefined || v === '') return '—';
    const n = this.breezePortfolioParseNumValue(v);
    if (n != null) return this.breezePortfolioFormatMoney(n);
    return String(v);
  }

  /** After a poll, update last-shown movements when current diff is non-zero so we keep showing it when next poll is unchanged. */
  private updateLastShownMovements(): void {
    if (!this.data || !this.lastRefreshHoldings) return;
    const last = this.lastRefreshHoldings;
    const cur = this.data;
    const openToday = cur.openPriceUsd;
    if (openToday != null && openToday > 0) {
      const diffUsd = cur.livePriceUsd - openToday;
      if (Math.abs(diffUsd) >= 0.01) {
        this.lastShownLivePriceMovement = { diffUsd, diffPct: (diffUsd / openToday) * 100 };
      }
    } else if (last.livePriceUsd !== 0) {
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
        this.cdr.markForCheck();
      },
      error: () => {
        this.canUndoMarkSold = false;
        this.cdr.markForCheck();
      },
    });
  }

  loadHoldings(): void {
    this.dashboardService.getHoldings().subscribe({
      next: (rows) => {
        this.holdings = rows;
        this.cdr.markForCheck();
        if (this.activeTab === 'holdings') setTimeout(() => this.initOrUpdateHoldingsChart(), 0);
      },
      error: () => { this.holdings = []; this.cdr.markForCheck(); },
    });
  }

  ngAfterViewInit(): void {
    if (this.activeTab === 'holdings' && this.holdings.length > 0) setTimeout(() => this.initOrUpdateHoldingsChart(), 0);
  }

  ngOnDestroy(): void {
    this.stopLivePricePolling();
    if (this.routeSub) {
      this.routeSub.unsubscribe();
      this.routeSub = null;
    }
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
      this.cdr.markForCheck();
      if (this.livePriceChart) {
        this.updateLivePriceChartData();
      } else if (this.livePriceChartCanvas?.nativeElement) {
        this.createLivePriceChart();
      }
    }, 100);
  }

  /** Eastern-time date key (YYYY-MM-DD) for a timestamp. Used to group by trading day. */
  private getEtDateKey(timestamp: number): string {
    return new Date(timestamp).toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
  }

  /** Short ET calendar label for ticker chips. */
  private formatEtDayLabel(timestamp: number): string {
    return new Date(timestamp).toLocaleDateString('en-US', {
      weekday: 'short',
      month: 'short',
      day: 'numeric',
      timeZone: 'America/New_York',
    });
  }

  private refreshLivePriceDayTickerCache(): void {
    const h = this.livePriceHistory;
    if (h.length === 0) {
      this.livePriceDayTickerItems = [];
      return;
    }
    const openByDay = this.buildLivePriceOpenByDay(h, this.data?.openPriceUsd);
    const dayToPoints = new Map<string, LivePriceHistoryPoint[]>();
    for (const p of h) {
      const k = this.getEtDateKey(p.timestamp);
      if (!dayToPoints.has(k)) dayToPoints.set(k, []);
      dayToPoints.get(k)!.push(p);
    }
    const keys = [...dayToPoints.keys()].sort().reverse();
    this.livePriceDayTickerItems = keys.map((dateKey) => {
      const pts = dayToPoints.get(dateKey)!;
      const close = pts[pts.length - 1]!.livePriceUsd;
      const open = openByDay.get(dateKey) ?? pts[0]!.open ?? pts[0]!.livePriceUsd;
      // Use per-bucket OHLC highs/lows when present (server rollups); else fall back to close prices.
      const highs = pts.map((p) => p.high ?? p.livePriceUsd);
      const lows = pts.map((p) => p.low ?? p.livePriceUsd);
      // Keep OHLC internally consistent even when polling started after the official session open.
      const low = Math.min(...lows, open, close);
      const high = Math.max(...highs, open, close);
      const diffUsd = close - open;
      const diffPct = open !== 0 ? (diffUsd / open) * 100 : 0;
      return {
        dateKey,
        label: this.formatEtDayLabel(pts[0]!.timestamp),
        open,
        close,
        high,
        low,
        diffUsd,
        diffPct,
        samples: pts.length,
      };
    });
  }

  livePriceDayTickerTooltip(d: LivePriceDayTickerItem): string {
    return (
      `Open $${this.formatUsd(d.open)} · High $${this.formatUsd(d.high)} · Low $${this.formatUsd(d.low)} · Last $${this.formatUsd(d.close)} · ` +
      `${d.samples} sample(s). Change vs open. (US Eastern day)`
    );
  }

  /** Build map of ET date -> open price that day. Uses API today open when provided; else first point per day from history. */
  private buildLivePriceOpenByDay(history: { timestamp: number; livePriceUsd: number; open?: number }[], todayOpenUsd?: number): Map<string, number> {
    const map = new Map<string, number>();
    for (const p of history) {
      const key = this.getEtDateKey(p.timestamp);
      if (!map.has(key)) map.set(key, p.open ?? p.livePriceUsd);
    }
    if (todayOpenUsd != null && todayOpenUsd > 0) {
      const todayKey = this.getEtDateKey(Date.now());
      map.set(todayKey, todayOpenUsd);
    }
    return map;
  }

  /** Fill first/last-of-day index sets from livePriceHistory for per-day Start/End labels. */
  private buildLivePriceDayBoundaryIndices(): void {
    this.livePriceFirstOfDayIndices.clear();
    this.livePriceLastOfDayIndices.clear();
    const h = this.livePriceHistory;
    const dayToIndices = new Map<string, number[]>();
    h.forEach((p, i) => {
      const key = this.getEtDateKey(p.timestamp);
      if (!dayToIndices.has(key)) dayToIndices.set(key, []);
      dayToIndices.get(key)!.push(i);
    });
    dayToIndices.forEach((indices) => {
      if (indices.length > 0) {
        this.livePriceFirstOfDayIndices.add(indices[0]);
        this.livePriceLastOfDayIndices.add(indices[indices.length - 1]);
      }
    });
  }

  setLivePriceChartMode(mode: 'line' | 'candlestick'): void {
    if (this.livePriceChartMode === mode) return;
    this.livePriceChartMode = mode;
    if (this.livePriceChart) { this.livePriceChart.destroy(); this.livePriceChart = null; }
    this.initOrUpdateLivePriceChart();
  }

  setLivePriceRange(range: '1d' | '1w' | '2w'): void {
    if (this.livePriceRange === range) return;
    this.livePriceRange = range;
    // Range drives the fetch so each range gets an appropriate server-side resolution.
    if (this.livePriceChart) { this.livePriceChart.destroy(); this.livePriceChart = null; }
    this.loadLivePriceHistoryFromDb();
  }

  /** Calendar days to fetch for the selected range. */
  private livePriceRangeFetchDays(): number {
    return DashboardComponent.LIVE_PRICE_RANGE_DAYS[this.livePriceRange];
  }

  get livePriceLineSessionLabel(): string {
    const items = this.livePriceDayTickerItems;
    if (!items.length) return 'Latest sessions';
    const newest = items[0]?.label ?? '';
    const oldest = items[items.length - 1]?.label ?? '';
    return items.length > 1 ? `${oldest} → ${newest}` : newest;
  }

  get livePriceCandlestickSummary(): string {
    const latest = this.livePriceDayTickerItems[0];
    if (!latest) return 'Multi-day OHLC view';
    return `O $${this.formatUsd(latest.open)} | H $${this.formatUsd(latest.high)} | L $${this.formatUsd(latest.low)} | C $${this.formatUsd(latest.close)}`;
  }

  /** Evenly downsample a day's points to at most maxN, always keeping the first and last. */
  private downsampleLinePoints<T>(pts: T[], maxN: number): T[] {
    if (pts.length <= maxN) return pts;
    const stride = Math.ceil(pts.length / maxN);
    const out: T[] = [];
    for (let i = 0; i < pts.length; i += stride) out.push(pts[i]);
    if (out[out.length - 1] !== pts[pts.length - 1]) out.push(pts[pts.length - 1]);
    return out;
  }

  /**
   * Build the multi-day line series over the full history. Inserts a null between ET days so the line
   * breaks across overnight/weekend gaps, and emits a per-day "Session open" value. Downsamples dense
   * intraday data per day for responsiveness. Also caches livePriceLinePoints / livePriceLineAvg for tooltips.
   */
  private buildLivePriceLineSeries(): { labels: string[]; prices: (number | null)[]; openLine: (number | null)[] } {
    const h = this.livePriceHistory;
    const labels: string[] = [];
    const prices: (number | null)[] = [];
    const openLine: (number | null)[] = [];
    const points: ({ timestamp: number; livePriceUsd: number; usdToInrRate: number } | null)[] = [];

    // Group consecutive points by ET day (preserves chronological order). Range is already scoped by the fetch.
    const dayGroups: { key: string; pts: typeof h }[] = [];
    for (const p of h) {
      const key = this.getEtDateKey(p.timestamp);
      const last = dayGroups[dayGroups.length - 1];
      if (!last || last.key !== key) dayGroups.push({ key, pts: [p] });
      else last.pts.push(p);
    }

    let sum = 0;
    let count = 0;
    dayGroups.forEach((g, gi) => {
      if (gi > 0) {
        // Gap separator so the line doesn't connect across days.
        labels.push('');
        prices.push(null);
        openLine.push(null);
        points.push(null);
      }
      const pts = this.downsampleLinePoints(g.pts, DashboardComponent.LIVE_PRICE_LINE_MAX_PER_DAY);
      const dayOpen = this.livePriceOpenByDay.get(g.key) ?? pts[0].livePriceUsd;
      for (const p of pts) {
        labels.push(this.formatXLabel(p.timestamp, false));
        prices.push(p.livePriceUsd);
        openLine.push(dayOpen);
        points.push(p);
        sum += p.livePriceUsd;
        count++;
      }
    });

    this.livePriceLinePoints = points;
    this.livePriceLineAvg = count ? sum / count : 0;
    return { labels, prices, openLine };
  }

  private updateLivePriceChartData(): void {
    if (!this.livePriceChart || this.livePriceHistory.length < 2) return;
    this.livePriceOpenByDay = this.buildLivePriceOpenByDay(this.livePriceHistory, this.data?.openPriceUsd);
    this.buildLivePriceDayBoundaryIndices();
    if (this.livePriceChartMode === 'candlestick') {
      const canvas = this.livePriceChartCanvas?.nativeElement;
      if (!canvas) return;
      this.livePriceChart.destroy();
      this.livePriceChart = null;
      this.createCandlestickChart(canvas);
      return;
    } else {
      const { labels, prices, openLine } = this.buildLivePriceLineSeries();
      const avg = this.livePriceLineAvg;
      this.livePriceChart.data.labels = labels;
      (this.livePriceChart.data.datasets[0] as any).data = prices;
      (this.livePriceChart.data.datasets[1] as any).data = openLine;
      (this.livePriceChart.data.datasets[2] as any).data = prices.map((v) => (v === null ? null : avg));
    }
    this.livePriceChart.update('none');
  }

  private formatXLabel(ts: number, intraday = false): string {
    return new Date(ts).toLocaleString('en-US', intraday ? {
      hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'America/New_York',
    } : {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
      hour12: false, timeZone: 'America/New_York',
    });
  }

  private buildCandlestickData(): { labels: string[]; data: { x: number; o: number; h: number; l: number; c: number }[] } {
    const items = [...this.livePriceDayTickerItems].reverse();
    return {
      labels: items.map((d) => d.label),
      // Financial controller runs with parsing disabled, so each candle still needs a raw x value.
      data: items.map((d, index) => ({ x: index, o: d.open, h: d.high, l: d.low, c: d.close })),
    };
  }

  private getCandlestickYBounds(data: { o: number; h: number; l: number; c: number }[]): { min: number; max: number } {
    if (!data.length) return { min: 0, max: 1 };

    let low = Number.POSITIVE_INFINITY;
    let high = Number.NEGATIVE_INFINITY;
    for (const candle of data) {
      low = Math.min(low, candle.l);
      high = Math.max(high, candle.h);
    }

    const span = Math.max(high - low, 0.01);
    const midpoint = (high + low) / 2;
    const reference = Math.max(Math.abs(midpoint), 1);
    const halfRange = Math.max(
      span * (data.length <= 3 ? 1.4 : 0.78),
      reference * 0.008,
      0.9,
    );

    return {
      min: Math.max(0, midpoint - halfRange),
      max: midpoint + halfRange,
    };
  }

  private getCandlestickFillColor(candle: { o: number; c: number }): string {
    if (candle.c > candle.o) return 'rgba(63, 185, 80, 0.65)';
    if (candle.c < candle.o) return 'rgba(248, 81, 73, 0.65)';
    return 'rgba(139, 148, 158, 0.65)';
  }

  private getCandlestickStrokeColor(candle: { o: number; c: number }): string {
    if (candle.c > candle.o) return '#3fb950';
    if (candle.c < candle.o) return '#f85149';
    return '#8b949e';
  }

  private createLivePriceChart(): void {
    const canvas = this.livePriceChartCanvas?.nativeElement;
    if (!canvas || this.livePriceHistory.length < 2 || this.livePriceChart) return;
    this.livePriceOpenByDay = this.buildLivePriceOpenByDay(this.livePriceHistory, this.data?.openPriceUsd);
    this.buildLivePriceDayBoundaryIndices();

    if (this.livePriceChartMode === 'candlestick') {
      this.createCandlestickChart(canvas);
    } else {
      this.createLineChart(canvas);
    }
  }

  private createLineChart(canvas: HTMLCanvasElement): void {
    if (this.livePriceHistory.length < 2) return;
    const { labels, prices, openLine } = this.buildLivePriceLineSeries();
    const realCount = this.livePriceLinePoints.filter((p) => p !== null).length;
    if (realCount < 2) return;
    const avg = this.livePriceLineAvg;
    const lastRealIndex = prices.length - 1;
    const avgLine = prices.map((v) => (v === null ? null : avg));

    const ctx2d = canvas.getContext('2d')!;
    const gradient = ctx2d.createLinearGradient(0, 0, 0, canvas.parentElement?.clientHeight || 400);
    gradient.addColorStop(0, 'rgba(56, 139, 253, 0.22)');
    gradient.addColorStop(0.68, 'rgba(56, 139, 253, 0.05)');
    gradient.addColorStop(1, 'rgba(56, 139, 253, 0)');

    this.livePriceChart = new Chart(canvas, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'NVDA (USD)',
            data: prices,
            borderColor: '#388bfd',
            borderWidth: 2,
            pointRadius: (ctx: any) => ctx.dataIndex === lastRealIndex ? 3 : 0,
            pointHoverRadius: 5,
            pointBackgroundColor: '#388bfd',
            pointHoverBackgroundColor: '#58a6ff',
            backgroundColor: gradient,
            fill: true,
            spanGaps: false,
            tension: 0.25,
            datalabels: { display: false },
          },
          {
            label: 'Session open',
            data: openLine,
            borderColor: 'rgba(139, 148, 158, 0.75)',
            borderDash: [5, 4],
            borderWidth: 1,
            pointRadius: 0,
            fill: false,
            spanGaps: false,
            tension: 0,
            datalabels: { display: false },
          },
          {
            label: 'Average',
            data: avgLine,
            borderColor: 'rgba(126, 231, 135, 0.95)',
            borderDash: [3, 3],
            borderWidth: 1,
            pointRadius: 0,
            fill: false,
            spanGaps: false,
            tension: 0,
            datalabels: { display: false },
          },
        ],
      },
      options: {
        animation: false,
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: { top: 8, right: 8, bottom: 4, left: 4 } },
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            display: true,
            position: 'top',
            align: 'start',
            labels: { color: '#8b949e', boxWidth: 14, usePointStyle: false, padding: 14 },
          },
          tooltip: {
            enabled: true,
            backgroundColor: 'rgba(22, 27, 34, 0.95)',
            borderColor: '#30363d',
            borderWidth: 1,
            titleColor: '#e6edf3',
            bodyColor: '#c9d1d9',
            padding: 10,
            cornerRadius: 6,
            displayColors: false,
            callbacks: {
              title: (items: any[]) => {
                if (!items.length) return '';
                const p = this.livePriceLinePoints[items[0].dataIndex];
                if (!p) return '';
                return new Date(p.timestamp).toLocaleString('en-US', {
                  weekday: 'short', month: 'short', day: 'numeric',
                  hour: '2-digit', minute: '2-digit', hour12: true, timeZone: 'America/New_York',
                }) + ' ET';
              },
              label: (item: any) => {
                const p = this.livePriceLinePoints[item.dataIndex];
                const dayOpen = p ? (this.livePriceOpenByDay.get(this.getEtDateKey(p.timestamp)) ?? p.livePriceUsd) : 0;
                if (item.datasetIndex === 0) return `NVDA: $${Number(item.raw).toFixed(2)}`;
                if (item.datasetIndex === 1) return `Session open: $${dayOpen.toFixed(2)}`;
                return `Average: $${this.livePriceLineAvg.toFixed(2)}`;
              },
              afterBody: (items: any[]) => {
                if (!items.length) return '';
                const p = this.livePriceLinePoints[items[0].dataIndex];
                if (!p) return '';
                const dayOpen = this.livePriceOpenByDay.get(this.getEtDateKey(p.timestamp)) ?? p.livePriceUsd;
                const diff = p.livePriceUsd - dayOpen;
                const pct = dayOpen ? (diff / dayOpen) * 100 : 0;
                const sign = diff >= 0 ? '+' : '';
                return `Change from open: ${sign}$${diff.toFixed(2)} (${sign}${pct.toFixed(2)}%)`;
              },
            },
          },
        },
        scales: {
          x: {
            ticks: {
              color: '#8b949e',
              maxRotation: 0,
              autoSkip: true,
              maxTicksLimit: 10,
              font: { size: 10 },
            },
            grid: { display: false },
          },
          y: {
            position: 'right' as const,
            ticks: {
              color: '#8b949e',
              font: { size: 11 },
              padding: 8,
              callback: (value: any) => '$' + Number(value).toFixed(2),
            },
            grid: { color: 'rgba(48, 54, 61, 0.4)', drawTicks: false },
            border: { display: false },
            grace: '4%',
          },
        },
      },
    });
  }

  private createCandlestickChart(canvas: HTMLCanvasElement): void {
    const ohlc = this.buildCandlestickData();
    if (!ohlc.data.length) return;
    const bounds = this.getCandlestickYBounds(ohlc.data);
    const candleCount = ohlc.data.length;
    const maxBodyThickness = candleCount === 1 ? 84 : candleCount <= 4 ? 48 : 28;
    const barPercentage = candleCount === 1 ? 0.96 : candleCount <= 4 ? 0.82 : 0.58;
    const categoryPercentage = candleCount === 1 ? 0.42 : candleCount <= 4 ? 0.64 : 0.76;
    const decorationPlugin = {
      id: 'live-price-candlestick-decoration',
      afterDatasetsDraw: (chart: Chart) => {
        const meta = chart.getDatasetMeta(0);
        const yScale = chart.scales['y'];
        const chartArea = chart.chartArea;
        if (!meta?.data?.length || !yScale || !chartArea) return;

        const ctx = chart.ctx;
        ctx.save();
        ctx.lineWidth = 1.5;
        ctx.font = '600 11px Inter, system-ui, sans-serif';
        ctx.textBaseline = 'middle';

        ohlc.data.forEach((candle, index) => {
          const bar = meta.data[index] as any;
          if (!bar) return;

          const x = bar.x;
          const openPx = yScale.getPixelForValue(candle.o);
          const closePx = yScale.getPixelForValue(candle.c);
          const highPx = yScale.getPixelForValue(candle.h);
          const lowPx = yScale.getPixelForValue(candle.l);
          const bodyTop = Math.min(openPx, closePx);
          const bodyBottom = Math.max(openPx, closePx);

          ctx.strokeStyle = this.getCandlestickStrokeColor(candle);
          ctx.beginPath();
          ctx.moveTo(x, highPx);
          ctx.lineTo(x, bodyTop);
          ctx.moveTo(x, bodyBottom);
          ctx.lineTo(x, lowPx);
          ctx.stroke();
        });

        const latestIndex = ohlc.data.length - 1;
        const latest = ohlc.data[latestIndex];
        const latestBar = meta.data[latestIndex] as any;
        if (!latest || !latestBar) {
          ctx.restore();
          return;
        }

        const accent = this.getCandlestickStrokeColor(latest);
        const neutral = '#8b949e';
        const bodyHalfWidth = Math.max(14, Math.min(maxBodyThickness / 2, Number(latestBar.width ?? maxBodyThickness) / 2));
        const centerX = Number(latestBar.x ?? 0);
        const openPx = yScale.getPixelForValue(latest.o);
        const closePx = yScale.getPixelForValue(latest.c);
        const highPx = yScale.getPixelForValue(latest.h);
        const lowPx = yScale.getPixelForValue(latest.l);
        const bullish = latest.c >= latest.o;

        const annotations: Array<{
          key: 'O' | 'H' | 'L' | 'C';
          value: number;
          targetY: number;
          y?: number;
          side: 'left' | 'right';
          anchorX: number;
          color: string;
        }> = bullish
          ? [
              { key: 'H', value: latest.h, targetY: highPx, side: 'right', anchorX: centerX + bodyHalfWidth, color: neutral },
              { key: 'C', value: latest.c, targetY: closePx, side: 'right', anchorX: centerX + bodyHalfWidth, color: accent },
              { key: 'O', value: latest.o, targetY: openPx, side: 'left', anchorX: centerX - bodyHalfWidth, color: accent },
              { key: 'L', value: latest.l, targetY: lowPx, side: 'left', anchorX: centerX - bodyHalfWidth, color: neutral },
            ]
          : [
              { key: 'H', value: latest.h, targetY: highPx, side: 'right', anchorX: centerX + bodyHalfWidth, color: neutral },
              { key: 'O', value: latest.o, targetY: openPx, side: 'right', anchorX: centerX + bodyHalfWidth, color: accent },
              { key: 'C', value: latest.c, targetY: closePx, side: 'left', anchorX: centerX - bodyHalfWidth, color: accent },
              { key: 'L', value: latest.l, targetY: lowPx, side: 'left', anchorX: centerX - bodyHalfWidth, color: neutral },
            ];

        const layoutSide = (side: 'left' | 'right'): void => {
          const items = annotations.filter((ann) => ann.side === side).sort((a, b) => a.targetY - b.targetY);
          if (!items.length) return;

          const minY = chartArea.top + 14;
          const maxY = chartArea.bottom - 14;
          const gap = 18;

          for (let i = 0; i < items.length; i++) {
            const current = items[i];
            current.y = Math.min(maxY, Math.max(minY, current.targetY));
            if (i > 0 && current.y < (items[i - 1].y ?? minY) + gap) {
              current.y = (items[i - 1].y ?? minY) + gap;
            }
          }

          const overflow = (items[items.length - 1].y ?? maxY) - maxY;
          if (overflow > 0) {
            items.forEach((item) => item.y = (item.y ?? maxY) - overflow);
          }

          const underflow = minY - (items[0].y ?? minY);
          if (underflow > 0) {
            items.forEach((item) => item.y = (item.y ?? minY) + underflow);
          }
        };

        const drawAnnotation = (ann: typeof annotations[number]): void => {
          const lineEndX = ann.side === 'right' ? ann.anchorX + 18 : ann.anchorX - 18;
          const text = `${ann.key} $${ann.value.toFixed(2)}`;
          const boxPaddingX = 6;
          const boxHeight = 18;
          const boxWidth = ctx.measureText(text).width + boxPaddingX * 2;
          const labelY = ann.y ?? ann.targetY;
          const boxX = ann.side === 'right' ? lineEndX + 6 : lineEndX - 6 - boxWidth;
          const boxY = labelY - boxHeight / 2;

          ctx.strokeStyle = ann.color;
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.moveTo(ann.anchorX, ann.targetY);
          ctx.lineTo(lineEndX, ann.targetY);
          if (Math.abs(labelY - ann.targetY) > 0.5) {
            ctx.lineTo(lineEndX, labelY);
          }
          ctx.stroke();

          ctx.fillStyle = 'rgba(13, 17, 23, 0.94)';
          ctx.fillRect(boxX, boxY, boxWidth, boxHeight);
          ctx.strokeStyle = ann.color;
          ctx.strokeRect(boxX, boxY, boxWidth, boxHeight);

          ctx.fillStyle = '#e6edf3';
          ctx.fillText(text, boxX + boxPaddingX, labelY);
        };

        layoutSide('left');
        layoutSide('right');
        annotations.forEach(drawAnnotation);

        ctx.restore();
      },
    };

    this.livePriceChart = new Chart(canvas, {
      type: 'bar',
      data: {
        labels: ohlc.labels,
        datasets: [
          {
            label: 'Daily candle',
            data: ohlc.data.map((candle) => [candle.o, candle.c]) as any,
            backgroundColor: ohlc.data.map((candle) => this.getCandlestickFillColor(candle)),
            borderColor: ohlc.data.map((candle) => this.getCandlestickStrokeColor(candle)),
            borderWidth: 1,
            borderSkipped: false,
            borderRadius: 2,
            barPercentage,
            categoryPercentage,
            maxBarThickness: maxBodyThickness,
            minBarLength: 6,
            datalabels: { display: false },
          },
        ],
      },
      plugins: [decorationPlugin],
      options: {
        animation: false,
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: { top: 12, right: 28, bottom: 8, left: 8 } },
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            display: true,
            position: 'top',
            align: 'start',
            labels: { color: '#8b949e', boxWidth: 14, padding: 14 },
          },
          tooltip: {
            enabled: true,
            backgroundColor: 'rgba(22, 27, 34, 0.95)',
            borderColor: '#30363d',
            borderWidth: 1,
            titleColor: '#e6edf3',
            bodyColor: '#c9d1d9',
            padding: 10,
            cornerRadius: 6,
            displayColors: false,
            callbacks: {
              title: (items: any[]) => items?.[0]?.label ?? '',
              label: (item: any) => {
                const r = ohlc.data[item.dataIndex];
                if (!r) return '';
                return [
                  `Open: $${r.o.toFixed(2)}`,
                  `High: $${r.h.toFixed(2)}`,
                  `Low:  $${r.l.toFixed(2)}`,
                  `Close: $${r.c.toFixed(2)}`,
                ] as any;
              },
              afterBody: (items: any[]) => {
                const r = ohlc.data[items?.[0]?.dataIndex ?? -1];
                if (!r || !r.o) return '';
                const diff = r.c - r.o;
                const pct = (diff / r.o) * 100;
                const sign = diff >= 0 ? '+' : '';
                return `Change: ${sign}$${diff.toFixed(2)} (${sign}${pct.toFixed(2)}%)`;
              },
            },
          },
        },
        scales: {
          x: {
            offset: true,
            ticks: { color: '#8b949e', font: { size: 11 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 8 },
            grid: { display: false },
            border: { display: false },
          },
          y: {
            position: 'right' as const,
            min: bounds.min,
            max: bounds.max,
            ticks: {
              color: '#8b949e',
              font: { size: 11 },
              padding: 8,
              callback: (value: any) => '$' + Number(value).toFixed(2),
            },
            grid: { color: 'rgba(48, 54, 61, 0.4)', drawTicks: false },
            border: { display: false },
            grace: '6%',
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
        this.cdr.markForCheck();
      },
      error: () => {
        this.refreshLiveInProgress = false;
        this.cdr.markForCheck();
      },
    });
  }

  loadTaxConfig(): void {
    this.taxConfigLoading = true;
    this.taxConfigError = null;
    this.taxConfigSaved = false;
    this.dashboardService.getTaxConfig().subscribe({
      next: (data: any) => {
        this.taxConfig = data;
        this.taxConfigRaw = JSON.stringify(data, null, 2);
        this.taxConfigLoading = false;
        this.cdr.markForCheck();
      },
      error: (err: any) => {
        this.taxConfigError = err?.error?.error || 'Failed to load tax config';
        this.taxConfigLoading = false;
        this.cdr.markForCheck();
      },
    });
  }

  saveTaxConfig(): void {
    this.taxConfigSaving = true;
    this.taxConfigSaved = false;
    this.taxConfigError = null;
    let parsed: any;
    try {
      parsed = JSON.parse(this.taxConfigRaw);
    } catch {
      this.taxConfigError = 'Invalid JSON — fix syntax errors before saving';
      this.taxConfigSaving = false;
      return;
    }
    this.dashboardService.putTaxConfig(parsed).subscribe({
      next: () => {
        this.taxConfig = parsed;
        this.taxConfigSaving = false;
        this.taxConfigSaved = true;
        this.cdr.markForCheck();
      },
      error: (err: any) => {
        this.taxConfigError = err?.error?.error || 'Failed to save';
        this.taxConfigSaving = false;
        this.cdr.markForCheck();
      },
    });
  }

  generateTaxDoc(): void {
    this.taxDocLoading = true;
    this.taxDocError = null;
    this.dashboardService.generateTaxDoc(this.taxDocSelectedFy).subscribe({
      next: (resp) => {
        this.taxDocFyLabel = resp.fyLabel;
        this.taxDocOutput = JSON.stringify(resp.rows, null, 2);
        this.taxDocLoading = false;
        this.cdr.markForCheck();
      },
      error: (err: any) => {
        this.taxDocError = err?.error?.error || 'Failed to generate tax doc';
        this.taxDocLoading = false;
        this.cdr.markForCheck();
      },
    });
  }

  taxDocFyDisplayLabel(fy: number): string {
    return `FY ${fy - 1}\u2013${String(fy).slice(-2)} (AY ${fy}\u2013${String(fy + 1).slice(-2)})`;
  }

  copyTaxDoc(): void {
    navigator.clipboard.writeText(this.taxDocOutput).catch(() => {});
  }

  downloadTaxDoc(): void {
    const blob = new Blob([this.taxDocOutput], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `AY_${this.taxDocSelectedFy}_Shares.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  /** Lot keys (buyDate|type|qty) for the holdings currently checked — used to filter the FA-A3 export. */
  private selectedHoldingLotKeys(): string[] {
    const keys: string[] = [];
    for (const r of this.holdingsFilteredSorted) {
      if (this.selectedRowKeys.has(this.getRowKey(r))) {
        const date = String(r.buyDate ?? '').split('T')[0];
        keys.push(`${date}|${r.type}|${r.qty}`);
      }
    }
    return keys;
  }

  /** Download the ClearTax Schedule FA template with FA-A3 filled from holdings (xlsx). Selected lots only when any are checked. */
  exportFaA3(): void {
    if (this.faExportInProgress) return;
    this.faExportInProgress = true;
    this.faExportError = null;
    this.cdr.markForCheck();
    const selectedKeys = this.selectedRowKeys.size > 0 ? this.selectedHoldingLotKeys() : undefined;
    this.dashboardService.exportFaA3(this.faExportFy, selectedKeys).subscribe({
      next: (resp) => {
        const blob = resp.body;
        if (!blob) {
          this.faExportError = 'Empty file returned.';
          this.faExportInProgress = false;
          this.cdr.markForCheck();
          return;
        }
        const cd = resp.headers.get('Content-Disposition') || '';
        const match = /filename="?([^"]+)"?/.exec(cd);
        const filename = match?.[1] || `schedule_fa_a3_AY_${this.faExportFy}.xlsx`;
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
        this.faExportInProgress = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        this.faExportError = err?.error?.error || err?.message || 'Export failed.';
        this.faExportInProgress = false;
        this.cdr.markForCheck();
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
        this.cdr.markForCheck();
        setTimeout(() => this.initOrUpdateSoldChart(), 0);
      },
      error: () => { this.soldRows = []; this.soldLoading = false; },
    });
  }

  /** Navigate to a tab's URL; the route param subscription applies it. */
  setActiveTab(tab: DashboardTab): void {
    if (tab === this.activeTab) return;
    this.router.navigate(['/', tab]);
  }

  /** Apply a tab: set active state and run its data-load / chart lifecycle side-effects. Called from the route param subscription. */
  private applyTab(tab: DashboardTab): void {
    this.activeTab = tab;
    if (tab === 'icici') {
      this.loadBreezeStatus();
    }
    if (tab === 'tax') {
      if (!this.taxConfig && !this.taxConfigLoading) this.loadTaxConfig();
      if (this.holdingsChart) { this.holdingsChart.destroy(); this.holdingsChart = null; }
      if (this.soldChart) { this.soldChart.destroy(); this.soldChart = null; }
    } else if (tab === 'sold') {
      if (this.holdingsChart) {
        this.holdingsChart.destroy();
        this.holdingsChart = null;
      }
      if (!this.soldLoading) this.loadSold();
      else if (this.soldRows.length > 0) setTimeout(() => this.initOrUpdateSoldChart(), 0);
    } else if (tab === 'playground' || tab === 'financial' || tab === 'icici') {
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
      if (this.holdings.length === 0) {
        this.loadHoldings();
      } else {
        setTimeout(() => this.initOrUpdateHoldingsChart(), 0);
      }
    }
  }

  /** Movement vs previous day close: total value USD. Falls back to last refresh if prev close unavailable. */
  get totalValueUsdMovement(): { diffUsd: number; diffPct: number } | null {
    if (!this.data) return this.lastShownTotalValueUsdMovement;
    const cur = this.data.totalValueUsd;
    const prevCloseValUsd = this.data.previousCloseValueUsd;
    if (prevCloseValUsd != null && prevCloseValUsd > 0) {
      const diffUsd = cur - prevCloseValUsd;
      if (Math.abs(diffUsd) >= 0.01) {
        this.lastShownTotalValueUsdMovement = { diffUsd, diffPct: (diffUsd / prevCloseValUsd) * 100 };
      }
      return this.lastShownTotalValueUsdMovement;
    }
    if (!this.lastRefreshHoldings) return this.lastShownTotalValueUsdMovement;
    const last = this.lastRefreshHoldings.totalValueUsd;
    if (last === 0) return this.lastShownTotalValueUsdMovement;
    const diffUsd = cur - last;
    if (Math.abs(diffUsd) >= 0.01) return { diffUsd, diffPct: (diffUsd / last) * 100 };
    return this.lastShownTotalValueUsdMovement;
  }

  /** Movement vs previous day close: total value INR. Falls back to last refresh if prev close unavailable. */
  get totalValueInrMovement(): { diffInr: number; diffPct: number } | null {
    if (!this.data) return this.lastShownTotalValueInrMovement;
    const cur = this.data.totalValueInr;
    const prevCloseValInr = this.data.previousCloseValueInr;
    if (prevCloseValInr != null && prevCloseValInr > 0) {
      const diffInr = cur - prevCloseValInr;
      if (Math.abs(diffInr) >= 1) {
        this.lastShownTotalValueInrMovement = { diffInr, diffPct: (diffInr / prevCloseValInr) * 100 };
      }
      return this.lastShownTotalValueInrMovement;
    }
    if (!this.lastRefreshHoldings) return this.lastShownTotalValueInrMovement;
    const last = this.lastRefreshHoldings.totalValueInr;
    if (last === 0) return this.lastShownTotalValueInrMovement;
    const diffInr = cur - last;
    if (Math.abs(diffInr) >= 1) return { diffInr, diffPct: (diffInr / last) * 100 };
    return this.lastShownTotalValueInrMovement;
  }

  /**
   * Decompose the INR value change (vs previous day close) into price effect vs FX effect.
   * Price effect = (curValueUsd − prevCloseValueUsd) × curRate  (stock price moved)
   * FX effect    = prevCloseValueUsd × (curRate − prevRate)      (exchange rate moved)
   * Uses previous close from Finnhub `pc` field; falls back to last refresh if unavailable.
   */
  get totalValueInrChangeBreakdown(): { priceEffectInr: number; fxEffectInr: number; prevRate: number; curRate: number } | null {
    if (!this.data) return this.lastShownInrChangeBreakdown;
    const curRate = this.data.usdToInrRate;
    const curUsd = this.data.totalValueUsd;

    const prevCloseUsd = this.data.previousCloseValueUsd;
    const prevCloseRate = this.data.previousCloseUsdToInrRate;
    let lastUsd: number;
    let prevRate: number;

    if (prevCloseUsd != null && prevCloseUsd > 0 && prevCloseRate != null && prevCloseRate > 0) {
      lastUsd = prevCloseUsd;
      prevRate = prevCloseRate;
    } else if (this.lastRefreshHoldings && this.lastRefreshHoldings.totalValueUsd > 0 && this.lastRefreshHoldings.totalValueInr > 0) {
      lastUsd = this.lastRefreshHoldings.totalValueUsd;
      prevRate = this.lastRefreshHoldings.totalValueInr / lastUsd;
    } else {
      return this.lastShownInrChangeBreakdown;
    }

    const priceEffectInr = (curUsd - lastUsd) * curRate;
    const fxEffectInr = lastUsd * (curRate - prevRate);
    const last = this.lastShownInrChangeBreakdown;
    const rateMoved =
      last == null ||
      Math.abs(last.curRate - curRate) >= 0.001 ||
      Math.abs(last.prevRate - prevRate) >= 0.001;
    if (Math.abs(priceEffectInr) >= 1 || Math.abs(fxEffectInr) >= 1 || rateMoved) {
      this.lastShownInrChangeBreakdown = { priceEffectInr, fxEffectInr, prevRate, curRate };
    }
    return this.lastShownInrChangeBreakdown;
  }

  /** Sum of all holdings' totalPurchaseInr = your total INR cost basis (what you paid). */
  get totalCostBasisInr(): number {
    return this.holdings.reduce((s, r) => s + Math.round(Number(r.totalPurchaseInr) || 0), 0);
  }

  /** Unrealised gain/loss = current total value INR − cost basis INR. */
  get totalGainLossInr(): number {
    return (this.data?.totalValueInr ?? 0) - this.totalCostBasisInr;
  }

  get totalGainLossPct(): number {
    const cost = this.totalCostBasisInr;
    return cost > 0 ? (this.totalGainLossInr / cost) * 100 : 0;
  }

  get totalSharesMovement(): { diff: number; diffPct: number } | null {
    if (!this.data || !this.lastRefreshHoldings) return null;
    const cur = this.data.totalShares;
    const last = this.lastRefreshHoldings.totalShares;
    if (last === 0) return this.lastShownTotalSharesMovement;
    const diff = cur - last;
    if (diff !== 0) return { diff, diffPct: (diff / last) * 100 };
    return this.lastShownTotalSharesMovement;
  }
  /** Movement vs previous day close when available, else today's open, else last refresh. */
  get livePriceMovement(): { diffUsd: number; diffPct: number } | null {
    if (!this.data) return null;
    const cur = this.data.livePriceUsd;
    const prevClose = this.data.previousCloseUsd;
    if (prevClose != null && prevClose > 0) {
      const diffUsd = cur - prevClose;
      if (Math.abs(diffUsd) >= 0.01) {
        this.lastShownLivePriceMovement = { diffUsd, diffPct: (diffUsd / prevClose) * 100 };
      }
      return this.lastShownLivePriceMovement;
    }
    const openToday = this.data.openPriceUsd;
    if (openToday != null && openToday > 0) {
      const diffUsd = cur - openToday;
      if (Math.abs(diffUsd) >= 0.01) return { diffUsd, diffPct: (diffUsd / openToday) * 100 };
      return this.lastShownLivePriceMovement;
    }
    if (!this.lastRefreshHoldings) return null;
    const last = this.lastRefreshHoldings.livePriceUsd;
    if (last === 0) return this.lastShownLivePriceMovement;
    const diffUsd = cur - last;
    if (Math.abs(diffUsd) >= 0.01) return { diffUsd, diffPct: (diffUsd / last) * 100 };
    return this.lastShownLivePriceMovement;
  }

  /** True when previous close is known (live card diff is vs prev close). */
  get livePriceUsesPrevCloseBaseline(): boolean {
    const pc = this.data?.previousCloseUsd;
    return pc != null && pc > 0;
  }

  /** Short label before the $ / % change on the live card. */
  get livePriceMovementDiffPrefix(): string {
    if (this.livePriceUsesPrevCloseBaseline) return 'vs prev close';
    const o = this.data?.openPriceUsd;
    if (o != null && o > 0) return 'vs open';
    return 'vs last quote';
  }

  /** Tooltip for the live price change span. */
  get livePriceMovementTooltip(): string {
    if (!this.data) return '';
    if (this.livePriceUsesPrevCloseBaseline) {
      const pc = this.data.previousCloseUsd!;
      return `Previous close $${this.formatUsd(pc)}. Change from last day's closing price.`;
    }
    const o = this.data.openPriceUsd;
    if (o != null && o > 0) {
      return `Session open $${this.formatUsd(o)}. Change from today's open.`;
    }
    return 'Change since the previous live quote (session open unavailable).';
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

  /** Scenarios for multi-price table: live first, then filled scenario slots. */
  get playMultiScenarioList(): { key: string; label: string; priceUsd: number }[] {
    const out: { key: string; label: string; priceUsd: number }[] = [];
    const live = this.data?.livePriceUsd;
    if (live != null && live > 0 && Number.isFinite(live)) {
      out.push({ key: 'live', label: `Live ($${live.toFixed(2)})`, priceUsd: live });
    }
    this.playMultiPriceSlotIndices.forEach((idx) => {
      const p = this.playMultiScenarioPrices[idx];
      if (p != null && p > 0 && Number.isFinite(p)) {
        out.push({ key: `s${idx}`, label: `Scenario ${idx + 1} ($${p.toFixed(2)})`, priceUsd: p });
      }
    });
    return out;
  }

  get playMultiSelectedRows(): HoldingRow[] {
    return this.holdingsFilteredSorted.filter((r) => this.playMultiSelectedKeys.has(this.getRowKey(r)));
  }

  getPlayMultiQty(row: HoldingRow): number {
    const v = this.playMultiRowQty[this.getRowKey(row)];
    const stored = typeof v === 'number' && !isNaN(v) ? v : 0;
    return Math.min(Math.max(0, stored), row.qty);
  }

  setPlayMultiScenarioPrice(index: number, value: number | string | null | undefined): void {
    const arr = [...this.playMultiScenarioPrices];
    if (value === '' || value == null) {
      arr[index] = null;
    } else {
      const n = typeof value === 'number' ? value : parseFloat(String(value));
      arr[index] = !isNaN(n) && n > 0 ? n : null;
    }
    this.playMultiScenarioPrices = arr;
    this.cdr.markForCheck();
  }

  togglePlayMultiRow(row: HoldingRow): void {
    const key = this.getRowKey(row);
    if (this.playMultiSelectedKeys.has(key)) {
      const next = new Set(this.playMultiSelectedKeys);
      next.delete(key);
      this.playMultiSelectedKeys = next;
      const q = { ...this.playMultiRowQty };
      delete q[key];
      this.playMultiRowQty = q;
    } else {
      this.playMultiSelectedKeys = new Set(this.playMultiSelectedKeys).add(key);
      this.playMultiRowQty = { ...this.playMultiRowQty, [key]: row.qty };
    }
    this.cdr.markForCheck();
  }

  isPlayMultiSelected(row: HoldingRow): boolean {
    return this.playMultiSelectedKeys.has(this.getRowKey(row));
  }

  playMultiSelectAllVisible(): void {
    const next = new Set(this.playMultiSelectedKeys);
    const qty = { ...this.playMultiRowQty };
    for (const r of this.holdingsFilteredSorted) {
      const k = this.getRowKey(r);
      next.add(k);
      if (qty[k] == null) qty[k] = r.qty;
    }
    this.playMultiSelectedKeys = next;
    this.playMultiRowQty = qty;
    this.cdr.markForCheck();
  }

  playMultiClearSelection(): void {
    this.playMultiSelectedKeys = new Set();
    this.playMultiRowQty = {};
    this.cdr.markForCheck();
  }

  onPlayMultiQtyInput(row: HoldingRow, event: Event): void {
    const input = event.target as HTMLInputElement;
    const raw = input.value;
    const n = parseInt(raw, 10);
    const val = raw === '' || isNaN(n) ? 0 : Math.min(Math.max(0, n), row.qty);
    const key = this.getRowKey(row);
    this.playMultiRowQty = { ...this.playMultiRowQty, [key]: val };
    input.value = String(val);
    this.cdr.markForCheck();
  }

  /** Value / tax / net for one lot at a price, scaled by playground multi qty. */
  playMultiLotMetrics(row: HoldingRow, priceUsd: number): { valueInr: number; taxInr: number; netInr: number } {
    const ratio = row.qty > 0 ? Math.min(this.getPlayMultiQty(row), row.qty) / row.qty : 0;
    const v = this.getHoldingValueTodayInrAtPrice(row, priceUsd) * ratio;
    const t = this.getHoldingTaxToPayInrAtPrice(row, priceUsd) * ratio;
    const n = this.getHoldingNetInrAtPrice(row, priceUsd) * ratio;
    return {
      valueInr: Math.round(v * 100) / 100,
      taxInr: Math.round(t * 100) / 100,
      netInr: Math.round(n * 100) / 100,
    };
  }

  get playMultiTotalsByScenario(): { key: string; label: string; totalValueInr: number; totalTaxInr: number; totalNetInr: number }[] {
    const scenarios = this.playMultiScenarioList;
    const rows = this.playMultiSelectedRows;
    if (rows.length === 0 || scenarios.length === 0) return [];
    return scenarios.map((sc) => {
      let totalValueInr = 0;
      let totalTaxInr = 0;
      let totalNetInr = 0;
      for (const row of rows) {
        const m = this.playMultiLotMetrics(row, sc.priceUsd);
        totalValueInr += m.valueInr;
        totalTaxInr += m.taxInr;
        totalNetInr += m.netInr;
      }
      return {
        key: sc.key,
        label: sc.label,
        totalValueInr: Math.round(totalValueInr * 100) / 100,
        totalTaxInr: Math.round(totalTaxInr * 100) / 100,
        totalNetInr: Math.round(totalNetInr * 100) / 100,
      };
    });
  }

  get playMultiDiffFromLive(): { label: string; dValueInr: number; dTaxInr: number; dNetInr: number }[] {
    const totals = this.playMultiTotalsByScenario;
    const live = totals.find((t) => t.key === 'live');
    if (!live) return [];
    return totals
      .filter((t) => t.key !== 'live')
      .map((t) => ({
        label: t.label,
        dValueInr: Math.round((t.totalValueInr - live.totalValueInr) * 100) / 100,
        dTaxInr: Math.round((t.totalTaxInr - live.totalTaxInr) * 100) / 100,
        dNetInr: Math.round((t.totalNetInr - live.totalNetInr) * 100) / 100,
      }));
  }

  get playMultiPairwiseDiffs(): { fromLabel: string; toLabel: string; dValueInr: number; dTaxInr: number; dNetInr: number }[] {
    const totals = this.playMultiTotalsByScenario;
    if (totals.length < 2) return [];
    const out: { fromLabel: string; toLabel: string; dValueInr: number; dTaxInr: number; dNetInr: number }[] = [];
    for (let i = 0; i < totals.length; i++) {
      for (let j = i + 1; j < totals.length; j++) {
        const a = totals[i];
        const b = totals[j];
        out.push({
          fromLabel: a.label,
          toLabel: b.label,
          dValueInr: Math.round((b.totalValueInr - a.totalValueInr) * 100) / 100,
          dTaxInr: Math.round((b.totalTaxInr - a.totalTaxInr) * 100) / 100,
          dNetInr: Math.round((b.totalNetInr - a.totalNetInr) * 100) / 100,
        });
      }
    }
    return out;
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

  /** Indian FY start year for a date (Apr–Mar). Apr 2024–Mar 2025 -> 2024. Returns null if unparseable. */
  private fyStartYear(dateStr: string): number | null {
    const t = this.parseDate(String(dateStr ?? ''));
    if (!t) return null;
    const d = new Date(t);
    return d.getMonth() >= 3 ? d.getFullYear() : d.getFullYear() - 1;
  }

  /** FY label like "FY 2024–25 (AY 2025–26)" from a FY start year. */
  soldFyLabel(startYear: number): string {
    return `FY ${startYear}\u2013${String(startYear + 1).slice(-2)} (AY ${startYear + 1}\u2013${String(startYear + 2).slice(-2)})`;
  }

  /** Distinct FY start years present in sold rows (by sell date), newest first. */
  get soldFyOptions(): number[] {
    const years = new Set<number>();
    for (const r of this.soldRows) {
      const y = this.fyStartYear(String(r.sellDate ?? ''));
      if (y != null) years.add(y);
    }
    return Array.from(years).sort((a, b) => b - a);
  }

  get soldFilteredSorted(): SoldRow[] {
    let list = this.soldRows.slice();
    if (this.soldFyFilter !== 'all') {
      list = list.filter((r) => this.fyStartYear(String(r.sellDate ?? '')) === this.soldFyFilter);
    }
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

  /** Handle select control click on sold table: normal click toggles row; shift+click selects range. */
  onSoldRowSelectClick(row: SoldRow, event: MouseEvent): void {
    const rows = this.soldFilteredSorted;
    const currentKey = this.getSoldRowKey(row);
    const currentIndex = rows.findIndex((r) => this.getSoldRowKey(r) === currentKey);
    if (event.shiftKey && this.lastClickedSoldRowKey != null) {
      const anchorIndex = rows.findIndex((r) => this.getSoldRowKey(r) === this.lastClickedSoldRowKey);
      if (anchorIndex >= 0 && currentIndex >= 0) {
        const from = Math.min(anchorIndex, currentIndex);
        const to = Math.max(anchorIndex, currentIndex);
        const next = new Set<string>();
        for (let i = from; i <= to; i++) next.add(this.getSoldRowKey(rows[i]));
        this.selectedSoldKeys = next;
      } else {
        this.toggleSoldRowSelection(row);
      }
    } else {
      this.toggleSoldRowSelection(row);
    }
    this.lastClickedSoldRowKey = currentKey;
    this.cdr.markForCheck();
  }

  toggleSoldRowSelection(row: SoldRow): void {
    const key = this.getSoldRowKey(row);
    if (this.selectedSoldKeys.has(key)) {
      this.selectedSoldKeys.delete(key);
      this.selectedSoldKeys = new Set(this.selectedSoldKeys);
    } else {
      this.selectedSoldKeys = new Set(this.selectedSoldKeys).add(key);
    }
    this.cdr.markForCheck();
  }

  /** Select only the currently visible (filtered) sold rows; replaces selection. */
  selectAllSoldVisible(): void {
    const rows = this.soldFilteredSorted;
    const next = new Set<string>();
    for (const r of rows) next.add(this.getSoldRowKey(r));
    this.selectedSoldKeys = next;
    this.cdr.markForCheck();
  }

  /** Deselect only the currently visible (filtered) sold rows. */
  deselectAllSoldVisible(): void {
    const rows = this.soldFilteredSorted;
    const next = new Set(this.selectedSoldKeys);
    for (const r of rows) next.delete(this.getSoldRowKey(r));
    this.selectedSoldKeys = next;
    this.cdr.markForCheck();
  }

  /** Totals for sold: when rows selected, use selected only; else use all filtered. Avg prices are qty-weighted (USD).
   * Profit % accrued = (total gain before tax / total buy value) and after tax (gain − tax) / buy value, both INR-based. */
  get soldSummaryTotals(): {
    totalQtySold: number;
    totalBuyValueInr: number;
    totalSellValueInr: number;
    totalTaxPaidInr: number;
    totalGainBeforeTaxInr: number;
    avgBuyPriceUsd: number;
    avgSellPriceUsd: number;
    profitPercentBeforeTax: number;
    profitPercentAfterTax: number;
    selectedCount: number;
  } {
    const rows = this.soldFilteredSorted;
    const useSelected = this.selectedSoldKeys.size > 0;
    let totalQtySold = 0;
    let totalBuyValueInr = 0;
    let totalSellValueInr = 0;
    let totalTaxPaidInr = 0;
    let totalGainBeforeTaxInr = 0;
    let buyPriceQtySum = 0;
    let sellPriceQtySum = 0;
    let selectedCount = 0;
    for (const r of rows) {
      if (useSelected && !this.selectedSoldKeys.has(this.getSoldRowKey(r))) continue;
      selectedCount++;
      totalQtySold += r.qtySold;
      totalBuyValueInr += Number(r.buyValueInr);
      totalSellValueInr += r.sellValueInr;
      totalTaxPaidInr += r.taxPaidInr;
      totalGainBeforeTaxInr += Number(r.gainBeforeTaxInr);
      buyPriceQtySum += Number(r.priceBoughtUsd) * r.qtySold;
      sellPriceQtySum += Number(r.priceSellUsd) * r.qtySold;
    }
    const avgBuyPriceUsd = totalQtySold > 0 ? buyPriceQtySum / totalQtySold : 0;
    const avgSellPriceUsd = totalQtySold > 0 ? sellPriceQtySum / totalQtySold : 0;
    const profitPercentBeforeTax = totalBuyValueInr > 0 ? (totalGainBeforeTaxInr / totalBuyValueInr) * 100 : 0;
    const profitPercentAfterTax = totalBuyValueInr > 0 ? ((totalGainBeforeTaxInr - totalTaxPaidInr) / totalBuyValueInr) * 100 : 0;
    return {
      totalQtySold,
      totalBuyValueInr,
      totalSellValueInr,
      totalTaxPaidInr,
      totalGainBeforeTaxInr,
      avgBuyPriceUsd,
      avgSellPriceUsd,
      profitPercentBeforeTax,
      profitPercentAfterTax,
      selectedCount,
    };
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

  /** USD rounded to exactly 1 decimal (used for average summaries). */
  formatUsd1(n: number): string {
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(n);
  }

  /** USD→INR spot: extra decimals so small feed moves are visible (ECB-only rates barely budge intraday). */
  formatUsdInrRate(n: number): string {
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 }).format(n);
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
        if (this.data) this.load(true);
        if (this.soldLoaded) this.loadSold();
        this.cdr.markForCheck();
      },
      error: (err: { error?: { error?: string }; message?: string }) => {
        this.markSoldInProgress = false;
        this.markSoldError = err?.error?.error || err?.message || 'Failed to mark as sold.';
        this.cdr.markForCheck();
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
        this.cdr.markForCheck();
      },
      error: (err: { status?: number; error?: { error?: string }; message?: string }) => {
        this.undoMarkSoldInProgress = false;
        this.undoMarkSoldError = err?.status === 404 ? 'Nothing to undo.' : (err?.error?.error || err?.message || 'Undo failed.');
        if (err?.status === 404) this.canUndoMarkSold = false;
        this.cdr.markForCheck();
      },
    });
  }

  onHoldingsSimulatePriceChange(value: number | string | null): void {
    if (value === '' || value == null) {
      this.holdingsSimulatePriceUsd = null;
      this.cdr.markForCheck();
      return;
    }
    const n = Number(value);
    this.holdingsSimulatePriceUsd = Number.isFinite(n) && n >= 0 ? n : null;
    this.cdr.markForCheck();
  }

  onHoldingsSimulateUsdToInrChange(value: number | string | null): void {
    if (value === '' || value == null) {
      this.holdingsSimulateUsdToInr = null;
      this.cdr.markForCheck();
      return;
    }
    const n = Number(value);
    this.holdingsSimulateUsdToInr = Number.isFinite(n) && n > 0 ? n : null;
    this.cdr.markForCheck();
  }

  /**
   * Unvested shares after US withholding: floor(gross × (1 − 37.14%)); null if no gross.
   * US side is share-based; INR lines derive from these counts × price × FX.
   */
  get holdingsUnvestedSharesAfterUs(): number | null {
    const g = this.holdingsUnvestedGrossShares;
    if (g == null) return null;
    return Math.floor(g * (1 - this.UNVESTED_US_WITHHOLDING_FRACTION));
  }

  /**
   * Unvested shares (through year-end): user enters gross share count; on blur we normalize to whole shares.
   * US withholding uses share math; India uses INR (see holdingsUnvestedSimTotals).
   */
  onHoldingsUnvestedGrossBlur(): void {
    const t = this.holdingsUnvestedGrossText.trim().replace(/,/g, '');
    if (t === '') {
      this.holdingsUnvestedGrossShares = null;
      this.cdr.markForCheck();
      return;
    }
    const n = Number(t);
    if (!Number.isFinite(n) || n < 0) {
      this.holdingsUnvestedGrossShares = null;
      this.cdr.markForCheck();
      return;
    }
    const gross = Math.floor(n);
    this.holdingsUnvestedGrossShares = gross;
    this.holdingsUnvestedGrossText = String(gross);
    this.cdr.markForCheck();
  }

  /** Indian CG rate % used for unvested sim (from mode + inputs). */
  get holdingsUnvestedIndiaCgRatePct(): number {
    return this.holdingsUnvestedIndiaCgMode === 'stcg'
      ? Math.max(0, this.holdingsUnvestedIndiaStcgPct)
      : Math.max(0, this.holdingsUnvestedIndiaLtcgPct);
  }

  get holdingsUnvestedIndiaCgModeLabel(): 'STCG' | 'LTCG' {
    return this.holdingsUnvestedIndiaCgMode === 'stcg' ? 'STCG' : 'LTCG';
  }

  /** Buy (cost) USD/share for India CG in unvested sim: live / current price. */
  get holdingsUnvestedIndiaBuyPriceUsd(): number {
    const pLive = this.data?.livePriceUsd ?? 0;
    const pSim = this.holdingsSimulatePriceUsd;
    const pSell =
      pSim != null && pSim > 0 ? pSim : pLive > 0 ? pLive : this.holdingsPriceUsd;
    return pLive > 0 ? pLive : pSell;
  }

  /** Sell USD/share for unvested sim: Simulate price when set (future sale), else live. */
  get holdingsUnvestedIndiaSellPriceUsd(): number {
    const pLive = this.data?.livePriceUsd ?? 0;
    const pSim = this.holdingsSimulatePriceUsd;
    if (pSim != null && pSim > 0) return pSim;
    if (pLive > 0) return pLive;
    return this.holdingsPriceUsd;
  }

  /**
   * Unvested sim: US math at **sell** price (Simulate = future sale; empty → live).
   * India CG: **buy** = live (current), **sell** = Simulate or live; gain = sell proceeds − cost at buy.
   */
  get holdingsUnvestedSimTotals(): {
    totalValueTodayInr: number;
    usWithholdingInr: number;
    valueAfterUsInr: number;
    /** India cost: after-US shares × buy USD × FX (buy = live). */
    indiaCostBasisUsedInr: number;
    taxableGainIndiaInr: number;
    indiaTaxInr: number;
    totalTaxToPayInr: number;
    totalNetInAccountInr: number;
  } | null {
    const gross = this.holdingsUnvestedGrossShares;
    if (gross == null) return null;
    const pLive = this.data?.livePriceUsd ?? 0;
    const pSim = this.holdingsSimulatePriceUsd;
    const r = this.holdingsUsdToInr;
    const pSell =
      pSim != null && pSim > 0 ? pSim : pLive > 0 ? pLive : this.holdingsPriceUsd;
    const pBuy = pLive > 0 ? pLive : pSell;
    if (pSell <= 0 || r <= 0) return null;
    const sharesAfterUs = Math.floor(gross * (1 - this.UNVESTED_US_WITHHOLDING_FRACTION));
    const valueGross = Math.round(gross * pSell * r * 100) / 100;
    const valueAfterUsInr = Math.round(sharesAfterUs * pSell * r * 100) / 100;
    const usWithholdingInr = Math.round((valueGross - valueAfterUsInr) * 100) / 100;
    const indiaCostBasisUsedInr = Math.round(sharesAfterUs * pBuy * r * 100) / 100;
    const taxableGainIndiaInr = Math.round(Math.max(0, valueAfterUsInr - indiaCostBasisUsedInr) * 100) / 100;
    const rate = this.holdingsUnvestedIndiaCgRatePct / 100;
    const indiaTaxInr = Math.round(taxableGainIndiaInr * rate * 100) / 100;
    const totalTaxToPayInr = Math.round((usWithholdingInr + indiaTaxInr) * 100) / 100;
    const totalNetInAccountInr = Math.round((valueAfterUsInr - indiaTaxInr) * 100) / 100;
    return {
      totalValueTodayInr: valueGross,
      usWithholdingInr,
      valueAfterUsInr,
      indiaCostBasisUsedInr,
      taxableGainIndiaInr,
      indiaTaxInr,
      totalTaxToPayInr,
      totalNetInAccountInr,
    };
  }

  /** Net INR at sim price/FX (same as unvested sim “after tax” total). */
  get holdingsUnvestedNetValueInr(): number | null {
    return this.holdingsUnvestedSimTotals?.totalNetInAccountInr ?? null;
  }

  /** Price (USD) used for Holdings table: override if set, else live. */
  get holdingsPriceUsd(): number {
    return this.holdingsSimulatePriceUsd ?? this.data?.livePriceUsd ?? 0;
  }

  /** When simulating price: difference vs current live price (USD and %). Positive = simulated is above live. */
  get holdingsSimulatePriceDiffFromLive(): { diffUsd: number; diffPct: number } | null {
    const sim = this.holdingsSimulatePriceUsd;
    const live = this.data?.livePriceUsd;
    if (sim == null || live == null || live <= 0) return null;
    const diffUsd = sim - live;
    const diffPct = (diffUsd / live) * 100;
    return { diffUsd, diffPct };
  }

  /** USD→INR rate for Holdings simulation (override when simulating FX). */
  get holdingsUsdToInr(): number {
    return this.holdingsSimulateUsdToInr ?? this.data?.usdToInrRate ?? 0;
  }

  /** When simulating FX: difference vs dashboard USD→INR (absolute and %). */
  get holdingsSimulateUsdToInrDiffFromLive(): { diff: number; diffPct: number } | null {
    const sim = this.holdingsSimulateUsdToInr;
    const live = this.data?.usdToInrRate;
    if (sim == null || live == null || live <= 0) return null;
    const diff = sim - live;
    const diffPct = (diff / live) * 100;
    return { diff, diffPct };
  }

  /**
   * USD price used as fallback to infer effective tax rate when snapshot gross is unusable.
   * Aligns with `getHoldingValueTodayInrAtPrice`: qty × price × rate.
   */
  private getTaxSlabAnchorPriceUsd(): number {
    const live = this.data?.livePriceUsd;
    if (live != null && live > 0) return live;
    return this.holdingsPriceUsd > 0 ? this.holdingsPriceUsd : 0;
  }

  /**
   * Tax rate (0–1) for recomputing tax at another USD price.
   * Prefer tax ÷ taxable gain from the API row (gross at fetch − cost basis), so the slab matches `taxToPayInr`
   * even if dashboard live/FX has moved slightly. Fallback: gain from qty × anchor × rate − totalPurchaseInr.
   * Otherwise statutory `taxPercent`.
   */
  /** @param usdToInrForFallback optional rate for slab fallback (e.g. live rate when comparing vs live). */
  private getHoldingTaxSlab(row: HoldingRow, usdToInrForFallback?: number): number {
    const rate = usdToInrForFallback ?? this.holdingsUsdToInr;
    const grossSnapshot = row.netIfSellTodayInr + row.taxToPayInr;
    const gainSnapshot = grossSnapshot - row.totalPurchaseInr;
    if (gainSnapshot > 1e-6 && row.taxToPayInr > 0) {
      const fromSnapshot = row.taxToPayInr / gainSnapshot;
      if (fromSnapshot > 0) return fromSnapshot;
    }
    const anchor = this.getTaxSlabAnchorPriceUsd();
    if (anchor > 0 && rate > 0) {
      const gainAtAnchor = row.qty * anchor * rate - row.totalPurchaseInr;
      if (gainAtAnchor > 1e-6 && row.taxToPayInr > 0) {
        const fromActual = row.taxToPayInr / gainAtAnchor;
        if (fromActual > 0) return fromActual;
      }
    }
    const pct = row.taxPercent ?? 0;
    return pct > 0 ? pct / 100 : 0;
  }

  /**
   * USD/share at the current dashboard USD→INR rate where value = INR cost basis (taxable gain → 0).
   */
  getHoldingBreakEvenUsd(row: HoldingRow): number | null {
    const rate = this.holdingsUsdToInr;
    const q = row.qty;
    if (rate <= 0 || q <= 0) return null;
    return row.totalPurchaseInr / (q * rate);
  }

  /**
   * Explain ₹0 tax on a scenario when live still shows tax (price is at/below break-even at today’s rate).
   */
  playMultiTaxHint(row: HoldingRow, priceUsd: number, taxInrAtPrice: number): string | null {
    if (taxInrAtPrice > 0.005) return null;
    const live = this.data?.livePriceUsd;
    if (live == null || live <= 0) return null;
    const taxAtLive = this.getHoldingTaxToPayInrAtPrice(row, live);
    if (taxAtLive <= 0.005) return null;
    const be = this.getHoldingBreakEvenUsd(row);
    if (be == null) return null;
    return `No tax: value at $${priceUsd.toFixed(2)} (this ₹/USD) is at or below INR cost basis. Approx. break-even ~$${be.toFixed(2)}. ESPP “Buy $” is TDS/FMV for cost in data, not always equal to break-even at today’s rate.`;
  }

  /** Tax cell tooltip: zero-tax scenarios, or ESPP when live USD is below TDS but INR gain is still positive. */
  playMultiTaxCellHint(row: HoldingRow, priceUsd: number, taxInr: number): string | null {
    const zeroHint = this.playMultiTaxHint(row, priceUsd, taxInr);
    if (zeroHint) return zeroHint;
    if (row.type !== 'ESPP' || taxInr <= 0.005) return null;
    const be = this.getHoldingBreakEvenUsd(row);
    if (be == null) return null;
    return `ESPP tax uses INR: (qty × this $ × today’s ₹) minus cost ₹ ${this.formatInr(row.totalPurchaseInr)} (TDS $${this.formatUsd(row.buyPriceUsd)} × historical ₹ on buy). Break-even at today’s rate ≈ $${be.toFixed(2)}/sh — can be below TDS when ₹/USD moved, so live below TDS can still show tax.`;
  }

  /** ESPP Buy column: clarify TDS vs paid and INR cost (hover). */
  playMultiBuyCellTitle(row: HoldingRow): string | null {
    if (row.type !== 'ESPP') return null;
    const be = this.getHoldingBreakEvenUsd(row);
    const beStr = be != null ? `$${be.toFixed(2)}/sh` : '—';
    return `Tax compares Value ₹ to Cost ₹ (TDS × ₹ on buy date), not “live $ vs TDS $”. “Paid” is not the capital-gains cost basis here. Break-even at today’s USD→INR ≈ ${beStr}.`;
  }

  /** Value today (INR) for full lot at an explicit USD price (uses dashboard USD→INR). */
  getHoldingValueTodayInrAtPrice(row: HoldingRow, priceUsd: number): number {
    const rate = this.holdingsUsdToInr;
    if (priceUsd <= 0 || rate <= 0) return row.netIfSellTodayInr + row.taxToPayInr;
    return row.qty * priceUsd * rate;
  }

  /** Tax to pay (INR) for full lot at an explicit USD price (same slab as row’s current tax/gain). */
  getHoldingTaxToPayInrAtPrice(row: HoldingRow, priceUsd: number): number {
    const valueToday = this.getHoldingValueTodayInrAtPrice(row, priceUsd);
    const gain = valueToday - row.totalPurchaseInr;
    if (gain <= 0) return 0;
    const slab = this.getHoldingTaxSlab(row);
    return Math.round(gain * slab * 100) / 100;
  }

  /** Net in hand (INR) for full lot at an explicit USD price. */
  getHoldingNetInrAtPrice(row: HoldingRow, priceUsd: number): number {
    const valueToday = this.getHoldingValueTodayInrAtPrice(row, priceUsd);
    const tax = this.getHoldingTaxToPayInrAtPrice(row, priceUsd);
    return Math.round((valueToday - tax) * 100) / 100;
  }

  /** Value today (INR) for this lot at current or simulated price. */
  getHoldingValueTodayInr(row: HoldingRow): number {
    return this.getHoldingValueTodayInrAtPrice(row, this.holdingsPriceUsd);
  }

  /** Tax to pay (INR) for this lot at current or simulated price. */
  getHoldingTaxToPayInr(row: HoldingRow): number {
    return this.getHoldingTaxToPayInrAtPrice(row, this.holdingsPriceUsd);
  }

  /** Net in hand (INR) for this lot at current or simulated price. */
  getHoldingNetInr(row: HoldingRow): number {
    return this.getHoldingNetInrAtPrice(row, this.holdingsPriceUsd);
  }

  /** Profit % for this lot at current or simulated price. */
  getHoldingProfitPercent(row: HoldingRow): number {
    const valueToday = this.getHoldingValueTodayInr(row);
    const tax = this.getHoldingTaxToPayInr(row);
    const net = valueToday - tax;
    const cost = row.totalPurchaseInr;
    if (cost <= 0) return 0;
    return Math.round(((net - cost) / cost) * 1000) / 10;
  }

  /** Totals for selected rows only, scaled by sell qty per row (for sell-strategy simulation). Uses simulated price when set. Avg buy/profit% are qty-weighted over selected rows. */
  get holdingsSelectedTotals(): {
    totalValueTodayInr: number;
    totalTaxToPayInr: number;
    totalNetInAccountInr: number;
    avgBuyPriceUsd: number;
    avgProfitPercent: number;
    selectedCount: number;
  } {
    const rows = this.holdingsFilteredSorted;
    let totalValueTodayInr = 0;
    let totalTaxToPayInr = 0;
    let totalNetInAccountInr = 0;
    let buyPriceQtySum = 0;
    let profitPctQtySum = 0;
    let qtySum = 0;
    let selectedCount = 0;
    for (const r of rows) {
      if (!this.selectedRowKeys.has(this.getRowKey(r))) continue;
      selectedCount++;
      const sellQty = this.getSellQty(r);
      const ratio = r.qty > 0 ? Math.min(sellQty, r.qty) / r.qty : 0;
      const valueTodayRow = this.getHoldingValueTodayInr(r);
      const taxRow = this.getHoldingTaxToPayInr(r);
      const netRow = this.getHoldingNetInr(r);
      totalValueTodayInr += valueTodayRow * ratio;
      totalTaxToPayInr += taxRow * ratio;
      totalNetInAccountInr += netRow * ratio;
      buyPriceQtySum += Number(r.buyPriceUsd) * r.qty;
      profitPctQtySum += this.getHoldingProfitPercent(r) * r.qty;
      qtySum += r.qty;
    }
    const avgBuyPriceUsd = qtySum > 0 ? buyPriceQtySum / qtySum : 0;
    const avgProfitPercent = qtySum > 0 ? profitPctQtySum / qtySum : 0;
    return { totalValueTodayInr, totalTaxToPayInr, totalNetInAccountInr, avgBuyPriceUsd, avgProfitPercent, selectedCount };
  }

  /** When simulating price or FX: totals vs live USD × live dashboard USD→INR (INR). */
  get holdingsSelectedTotalsDiffFromLive(): { diffValueTodayInr: number; diffNetInAccountInr: number } | null {
    const simPrice = this.holdingsSimulatePriceUsd != null;
    const simFx = this.holdingsSimulateUsdToInr != null;
    if ((!simPrice && !simFx) || this.data?.livePriceUsd == null || this.data.livePriceUsd <= 0) return null;
    const current = this.holdingsSelectedTotals;
    if (current.selectedCount === 0) return null;
    const liveRate = this.data.usdToInrRate ?? 0;
    if (liveRate <= 0) return null;
    let valueAtLive = 0;
    let netAtLive = 0;
    for (const r of this.holdingsFilteredSorted) {
      if (!this.selectedRowKeys.has(this.getRowKey(r))) continue;
      const sellQty = this.getSellQty(r);
      const ratio = r.qty > 0 ? Math.min(sellQty, r.qty) / r.qty : 0;
      const valueTodayRow = r.qty * this.data!.livePriceUsd * liveRate;
      const gain = valueTodayRow - r.totalPurchaseInr;
      const slab = this.getHoldingTaxSlab(r, liveRate);
      const taxRow = gain > 0 ? Math.round(gain * slab * 100) / 100 : 0;
      const netRow = Math.round((valueTodayRow - taxRow) * 100) / 100;
      valueAtLive += valueTodayRow * ratio;
      netAtLive += netRow * ratio;
    }
    return {
      diffValueTodayInr: Math.round((current.totalValueTodayInr - valueAtLive) * 100) / 100,
      diffNetInAccountInr: Math.round((current.totalNetInAccountInr - netAtLive) * 100) / 100,
    };
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

  /** Display value for a cell (formatted). Uses simulated price when holdingsSimulatePriceUsd is set. */
  formatCell(row: HoldingRow, key: keyof HoldingRow): string {
    if (key === 'netIfSellTodayInr') return '₹ ' + this.formatInr(this.getHoldingNetInr(row));
    if (key === 'taxToPayInr') return '₹ ' + this.formatInr(this.getHoldingTaxToPayInr(row));
    if (key === 'profitPercent') return this.getHoldingProfitPercent(row).toFixed(1) + '%';
    const v = row[key];
    if (key === 'buyPriceUsd') {
      const tdsOrBuy = this.formatUsd(Number(v));
      if (row.type === 'ESPP' && row.priceBoughtUsd != null) {
        return tdsOrBuy + '(' + this.formatUsd(row.priceBoughtUsd) + ')';
      }
      return tdsOrBuy;
    }
    if (key === 'type') return this.typeLabel(String(v));
    if (key === 'totalPurchaseInr') return '₹ ' + this.formatInr(Number(v));
    if (key === 'buyDate') return this.formatDate(String(v));
    if (key === 'qty') return String(v);
    if (key === 'taxPercent') return (Number(v) ?? 0).toFixed(1) + '%';
    return String(v ?? '');
  }

  private escapeCsvCell(s: string): string {
    const str = String(s ?? '');
    if (/[",\r\n]/.test(str)) return '"' + str.replace(/"/g, '""') + '"';
    return str;
  }

  /** ISO date (YYYY-MM-DD) for CSV export so spreadsheets parse it as a real date. */
  private formatDateForExport(value: string): string {
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

  /** Export holdings to CSV: selected rows if any are checked, else all visible (filtered/sorted). No commas/currency symbols. */
  exportHoldingsCsv(): void {
    const all = this.holdingsFilteredSorted;
    const rows = this.selectedRowKeys.size > 0 ? all.filter((r) => this.selectedRowKeys.has(this.getRowKey(r))) : all;
    const cols = this.holdingsColumnOrder;
    const header = cols.map((k) => this.escapeCsvCell(this.getColLabel(k))).join(',');
    const lines = [header];
    for (const row of rows) {
      const cells = cols.map((k) => {
        const v = row[k];
        if (k === 'buyPriceUsd' || k === 'totalPurchaseInr' || k === 'netIfSellTodayInr' || k === 'taxToPayInr') return this.escapeCsvCell(this.formatForExport(Number(v)));
        if (k === 'type') return this.escapeCsvCell(this.typeLabel(String(v)));
        if (k === 'buyDate') return this.escapeCsvCell(this.formatDateForExport(String(v)));
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

  /** Export sold shares to CSV: selected rows if any are checked, else all visible (filtered/sorted). No commas/currency symbols. */
  exportSoldCsv(): void {
    const all = this.soldFilteredSorted;
    const rows = this.selectedSoldKeys.size > 0 ? all.filter((r) => this.selectedSoldKeys.has(this.getSoldRowKey(r))) : all;
    const cols = this.soldCols;
    const header = cols.map((k) => this.escapeCsvCell(this.soldColLabels[k])).join(',');
    const lines = [header];
    for (const row of rows) {
      const cells = cols.map((col) => {
        const v = (row as unknown as Record<string, unknown>)[col];
        if (col === 'sellDate' || col === 'buyDate') return this.escapeCsvCell(this.formatDateForExport(String(v ?? '')));
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
