import { AfterViewInit, ChangeDetectorRef, Component, ElementRef, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Chart } from 'chart.js/auto';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import { DashboardService } from '../../core/services/dashboard.service';

Chart.register(ChartDataLabels);
import { DashboardResponse, HoldingRow, SoldRow } from '../../core/models/dashboard.types';

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
  /** Tab: Holdings vs Sold Shares */
  activeTab: 'holdings' | 'sold' = 'holdings';
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
  private holdingsChart: InstanceType<typeof Chart> | null = null;
  private soldChart: InstanceType<typeof Chart> | null = null;

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

  constructor(
    private dashboardService: DashboardService,
    private cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.loadHoldingsColumnOrder();
    this.load();
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

  load(): void {
    this.loading = true;
    this.error = null;
    this.dashboardService.getDashboardData().subscribe({
      next: (res: DashboardResponse) => {
        this.data = res;
        this.loading = false;
        this.loadHoldings();
      },
      error: (err: { error?: { error?: string }; message?: string }) => {
        this.error = err?.error?.error || err?.message || 'Failed to load dashboard';
        this.loading = false;
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
    if (this.holdingsChart) {
      this.holdingsChart.destroy();
      this.holdingsChart = null;
    }
    if (this.soldChart) {
      this.soldChart.destroy();
      this.soldChart = null;
    }
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
              formatter: (value: number) => (value === 0 ? '' : value),
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
              formatter: (value: number) => (value === 0 ? '' : value),
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
              formatter: (value: number) => (value === 0 ? '' : value),
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
              formatter: (value: number) => (value === 0 ? '' : value),
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
        this.data = res;
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

  setActiveTab(tab: 'holdings' | 'sold'): void {
    this.activeTab = tab;
    if (tab === 'sold') {
      if (this.holdingsChart) {
        this.holdingsChart.destroy();
        this.holdingsChart = null;
      }
      if (!this.soldLoaded && !this.soldLoading) this.loadSold();
      else if (this.soldRows.length > 0) setTimeout(() => this.initOrUpdateSoldChart(), 0);
    } else {
      if (this.soldChart) {
        this.soldChart.destroy();
        this.soldChart = null;
      }
      if (this.holdings.length > 0) setTimeout(() => this.initOrUpdateHoldingsChart(), 0);
    }
  }

  /** Display text for a sold row cell (used for contains filter). */
  private soldCellDisplay(row: SoldRow, col: keyof SoldRow): string {
    const v = (row as unknown as Record<string, unknown>)[col];
    if (col === 'sellDate' || col === 'buyDate') return this.formatDate(String(v ?? ''));
    if (col === 'type') return this.typeLabel(String(v ?? ''));
    if (col === 'qtySold') return String(v ?? '');
    if (col === 'priceBoughtUsd' || col === 'priceSellUsd') return this.formatUsd(Number(v));
    if (['buyValueInr', 'sellValueInr', 'gainBeforeTaxInr', 'taxPaidInr'].includes(col)) return '₹ ' + this.formatInr(Number(v));
    if (col === 'profitPercent' || col === 'taxPercent') return String(v ?? '') + '%';
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
    return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2, minimumFractionDigits: 2 }).format(n);
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
    if (key === 'profitPercent' || key === 'taxPercent') return String(v) + '%';
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
