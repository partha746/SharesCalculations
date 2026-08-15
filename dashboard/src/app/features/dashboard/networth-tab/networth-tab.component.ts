import { ChangeDetectionStrategy, ChangeDetectorRef, Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChanges } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { DashboardService } from '../../../core/services/dashboard.service';
import { IndianNumberDirective } from '../../../core/directives/indian-number.directive';
import { IncomePayout, NetworthItem, NetworthKind, NetworthLiquidity } from '../../../core/models/dashboard.types';

/** One ICICI equity/REIT/InvIT holding, as passed in from the dashboard shell. */
export interface IncomeHoldingInput {
  account: string;
  stockCode: string;
  qty: number;
  valueInr: number | null;
}

/** A holding with its expected payout worked out. */
interface IncomeRow {
  key: string;
  label: string;
  account: string;
  qty: number;
  symbol: string;
  payoutPerUnit: number | null;
  currency: string;
  source: string;
  annualInr: number;
  monthlyInr: number;
  valueInr: number | null;
  yieldPct: number | null;
}

/** An asset the dashboard already knows about; value comes from live data, so it is read-only here. */
interface TrackedRow {
  key: string;
  label: string;
  category: string;
  liquidity: NetworthLiquidity;
  valueInr: number | null;
  hint: string;
}

/**
 * Net worth tab: live "tracked" assets (NVDA, mutual funds, ICICI equity) combined with
 * manually-entered rows (bank, property, gold, loans...) into assets / liabilities / net worth,
 * split by liquidity.
 */
@Component({
  selector: 'app-networth-tab',
  standalone: true,
  imports: [CommonModule, FormsModule, IndianNumberDirective],
  templateUrl: './networth-tab.component.html',
  styleUrl: './networth-tab.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class NetworthTabComponent implements OnInit, OnChanges {
  /** NVDA position value after tax if sold today (Overview "In bank if you sell now"). */
  @Input() nvdaNetInr: number | null = null;
  /** Live mutual-fund value (sum of AMFI NAV × units). */
  @Input() mfValueInr: number | null = null;
  /** Live ICICI equity holdings value; null until the ICICI holdings have been fetched. */
  @Input() iciciEquityValueInr: number | null = null;
  /** True once ICICI equity holdings have actually been loaded (else we show a hint). */
  @Input() iciciEquityLoaded = false;
  /** True while the parent is refreshing tracked sources. */
  @Input() trackedLoading = false;
  /** ICICI equity/REIT/InvIT holdings, for expected-income calculation. */
  @Input() iciciEquityRows: IncomeHoldingInput[] = [];
  /** Account id -> display name (holder name). */
  @Input() accountNames: Record<string, string> = {};
  /** NVDA share count and the USD→INR rate, for the NVIDIA dividend row. */
  @Input() nvdaShares = 0;
  @Input() usdToInr = 0;
  /** The NVDA grant is held by the primary account holder, so its income is attributed there. */
  @Input() nvdaAccountId = '1';

  /** Ask the parent to re-fetch the tracked sources (live price, MF NAV, ICICI holdings). */
  @Output() refreshTracked = new EventEmitter<void>();

  items: NetworthItem[] = [];
  /** `items` in the current sort order; recomputed on load/sort/save so inline edits don't reshuffle mid-typing. */
  sortedItems: NetworthItem[] = [];
  sortKey: 'label' | 'kind' | 'category' | 'liquidity' | 'valueInr' | 'note' | '' = '';
  sortDir: 1 | -1 = 1;
  itemsSearch = '';
  loading = false;
  error: string | null = null;
  savingId: number | null = null;
  adding = false;

  readonly assetCategories = [
    'Cash & bank', 'Fixed deposit', 'Stocks (India)', 'Stocks (US)', 'Mutual funds',
    'Gold / silver', 'Crypto', 'EPF / PPF / NPS', 'Real estate', 'Vehicle',
    'Insurance / ULIP', 'Money lent', 'Other asset',
  ];
  readonly liabilityCategories = ['Home loan', 'Car loan', 'Personal loan', 'Education loan', 'Credit card', 'Other loan'];

  /** Sensible default liquidity per asset category (user can still override per row). */
  private readonly defaultLiquidity: Record<string, NetworthLiquidity> = {
    'Cash & bank': 'liquid', 'Fixed deposit': 'liquid', 'Stocks (India)': 'liquid',
    'Stocks (US)': 'liquid', 'Mutual funds': 'liquid', 'Gold / silver': 'liquid', 'Crypto': 'liquid',
    'EPF / PPF / NPS': 'illiquid', 'Real estate': 'illiquid', 'Vehicle': 'illiquid',
    'Insurance / ULIP': 'illiquid', 'Money lent': 'illiquid', 'Other asset': 'illiquid',
  };

  // New-row form
  newKind: NetworthKind = 'asset';
  newLabel = '';
  newCategory = 'Cash & bank';
  newLiquidity: NetworthLiquidity = 'liquid';
  newValue: number | null = null;
  newNote = '';

  constructor(private dashboardService: DashboardService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.loadItems();
    this.resolvePayouts();
  }

  ngOnChanges(changes: SimpleChanges): void {
    // Holdings arrive asynchronously (after the ICICI fetch), so resolve once the instrument set changes.
    if (changes['iciciEquityRows'] || changes['nvdaShares']) this.resolvePayouts();
  }

  // ===== Expected income (dividends / REIT & InvIT distributions) =====

  /** 'all' or an ICICI account id. */
  incomeAccount = 'all';
  incomeLoading = false;
  incomeError: string | null = null;
  showIncomeGaps = false;
  /** instrument key -> resolved payout. */
  payouts: Record<string, IncomePayout> = {};
  /** Draft edits for the override inputs, keyed by instrument. */
  symbolDraft: Record<string, string> = {};
  payoutDraft: Record<string, number | null> = {};
  private resolvedSignature = '';

  /** Unique instruments to price: every held stock code, plus NVDA when shares are held. */
  private instrumentItems(): Array<{ key: string; symbolHint: string }> {
    const map = new Map<string, string>();
    for (const r of this.iciciEquityRows || []) {
      const code = (r.stockCode || '').trim();
      if (code) map.set(code, `${code.toUpperCase()}.NS`);
    }
    if (this.nvdaShares > 0) map.set('NVDA', 'NVDA');
    return [...map.entries()].map(([key, symbolHint]) => ({ key, symbolHint }));
  }

  resolvePayouts(force = false): void {
    const items = this.instrumentItems();
    if (items.length === 0) return;
    const signature = items.map((i) => i.key).sort().join('|');
    if (!force && signature === this.resolvedSignature) return;
    this.resolvedSignature = signature;
    this.incomeLoading = true;
    this.incomeError = null;
    this.cdr.markForCheck();
    this.dashboardService.resolveIncomePayouts(items).subscribe({
      next: (res) => {
        const next: Record<string, IncomePayout> = {};
        for (const p of res.payouts || []) next[p.key] = p;
        this.payouts = next;
        this.incomeLoading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        this.incomeError = err?.error?.error || 'Could not fetch payout data.';
        this.incomeLoading = false;
        this.cdr.markForCheck();
      },
    });
  }

  /** Account ids present in the income rows (for the selector), including NVDA's holder. */
  get incomeAccountIds(): string[] {
    const ids = new Set((this.iciciEquityRows || []).map((r) => r.account));
    if (this.nvdaShares > 0) ids.add(this.nvdaAccountId);
    return [...ids].filter((id) => !!id).sort((a, b) => Number(a) - Number(b));
  }

  acctLabel(id: string): string {
    return this.accountNames[id] || `Account ${id}`;
  }

  /** Every holding with its payout worked out (unfiltered). */
  private allIncomeRows(): IncomeRow[] {
    const rows: IncomeRow[] = [];
    const build = (key: string, label: string, account: string, qty: number, valueInr: number | null): IncomeRow => {
      const p = this.payouts[key];
      const perUnit = p?.annualPayout ?? null;
      const fx = p?.currency === 'USD' ? this.usdToInr || 0 : 1;
      const annual = perUnit != null ? qty * perUnit * fx : 0;
      return {
        key, label, account, qty,
        symbol: p?.symbol || '',
        payoutPerUnit: perUnit,
        currency: p?.currency || 'INR',
        source: p?.source || 'unresolved',
        annualInr: annual,
        monthlyInr: annual / 12,
        valueInr,
        yieldPct: valueInr && valueInr > 0 ? (annual / valueInr) * 100 : null,
      };
    };
    for (const r of this.iciciEquityRows || []) {
      const code = (r.stockCode || '').trim();
      if (!code) continue;
      rows.push(build(code, this.payouts[code]?.name || code, r.account, r.qty, r.valueInr));
    }
    if (this.nvdaShares > 0) {
      const p = this.payouts['NVDA'];
      const row = build('NVDA', 'NVIDIA (NVDA)', this.nvdaAccountId, this.nvdaShares, null);
      if (p) rows.push(row);
    }
    return rows;
  }

  // --- Income table: search + sort ---
  incomeSearch = '';
  incomeSortKey: 'label' | 'account' | 'qty' | 'payout' | 'annual' | 'monthly' | 'yield' | 'source' | '' = '';
  incomeSortDir: 1 | -1 = 1;

  setIncomeSort(key: 'label' | 'account' | 'qty' | 'payout' | 'annual' | 'monthly' | 'yield' | 'source'): void {
    if (this.incomeSortKey === key) this.incomeSortDir = this.incomeSortDir === 1 ? -1 : 1;
    else {
      this.incomeSortKey = key;
      this.incomeSortDir = 1;
    }
  }

  incomeSortIndicator(key: string): string {
    if (this.incomeSortKey !== key) return '';
    return this.incomeSortDir === 1 ? '↑' : '↓';
  }

  private matchesIncome(r: IncomeRow): boolean {
    return this.matches([r.label, this.acctLabel(r.account), r.symbol, r.source], this.incomeSearch);
  }

  /** Rows that actually pay, honouring the account filter, search and sort. */
  get incomeRows(): IncomeRow[] {
    const rows = this.allIncomeRows()
      .filter((r) => (r.payoutPerUnit ?? 0) > 0)
      .filter((r) => (this.incomeAccount === 'all' ? true : r.account === this.incomeAccount))
      .filter((r) => this.matchesIncome(r));
    const key = this.incomeSortKey;
    const dir = this.incomeSortDir;
    // Default view leads with the biggest contributors.
    if (!key) return rows.sort((a, b) => b.monthlyInr - a.monthlyInr);
    return rows.sort((a, b) => {
      switch (key) {
        case 'label': return this.byText(a.label, b.label, dir);
        case 'account': return this.byText(this.acctLabel(a.account), this.acctLabel(b.account), dir);
        case 'qty': return this.byNum(a.qty, b.qty, dir);
        case 'payout': return this.byNum(a.payoutPerUnit, b.payoutPerUnit, dir);
        case 'annual': return this.byNum(a.annualInr, b.annualInr, dir);
        case 'monthly': return this.byNum(a.monthlyInr, b.monthlyInr, dir);
        case 'yield': return this.byNum(a.yieldPct, b.yieldPct, dir);
        case 'source': return this.byText(a.symbol || a.source, b.symbol || b.source, dir);
        default: return 0;
      }
    });
  }

  /** Instruments with no payout data or a zero payout — shown separately so a symbol can be fixed. */
  get incomeGapRows(): IncomeRow[] {
    return this.allIncomeRows()
      .filter((r) => !((r.payoutPerUnit ?? 0) > 0))
      .filter((r) => (this.incomeAccount === 'all' ? true : r.account === this.incomeAccount))
      .filter((r) => this.matchesIncome(r));
  }

  get incomeTotals(): { monthly: number; annual: number; value: number; yieldPct: number | null } {
    let annual = 0;
    let value = 0;
    for (const r of this.incomeRows) {
      annual += r.annualInr;
      if (r.valueInr != null) value += r.valueInr;
    }
    return {
      monthly: annual / 12,
      annual,
      value,
      yieldPct: value > 0 ? (annual / value) * 100 : null,
    };
  }

  /** Persist a symbol / payout override for one instrument, then re-resolve. */
  saveIncomeOverride(key: string): void {
    const symbol = (this.symbolDraft[key] ?? '').trim();
    const payout = this.payoutDraft[key];
    this.dashboardService
      .setIncomeOverride(key, { yahooSymbol: symbol, annualPayout: payout ?? null })
      .subscribe({
        next: () => this.resolvePayouts(true),
        error: (err) => {
          this.incomeError = err?.error?.error || 'Could not save the override.';
          this.cdr.markForCheck();
        },
      });
  }

  /** Drop an override so the payout goes back to the auto-fetched value. */
  clearIncomeOverride(key: string): void {
    this.dashboardService.deleteIncomeOverride(key).subscribe({
      next: () => {
        delete this.symbolDraft[key];
        delete this.payoutDraft[key];
        this.resolvePayouts(true);
      },
      error: () => this.resolvePayouts(true),
    });
  }

  loadItems(): void {
    this.loading = true;
    this.dashboardService.getNetworthItems().subscribe({
      next: (res) => {
        this.items = res.items ?? [];
        this.applySort();
        this.loading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        this.error = err?.error?.error || 'Could not load net worth items.';
        this.loading = false;
        this.cdr.markForCheck();
      },
    });
  }

  categoryOptions(kind: NetworthKind): string[] {
    return kind === 'liability' ? this.liabilityCategories : this.assetCategories;
  }

  // --- Shared sort/search helpers ---

  private byText(a: unknown, b: unknown, dir: 1 | -1): number {
    return String(a ?? '').localeCompare(String(b ?? '')) * dir;
  }

  /** Nulls sort last regardless of direction is overkill here; treat them as the smallest value. */
  private byNum(a: number | null | undefined, b: number | null | undefined, dir: 1 | -1): number {
    return ((a ?? -Infinity) - (b ?? -Infinity)) * dir;
  }

  private matches(haystacks: Array<string | null | undefined>, query: string): boolean {
    const q = query.trim().toLowerCase();
    if (!q) return true;
    return haystacks.some((h) => (h ?? '').toLowerCase().includes(q));
  }

  // --- Sorting ---

  setSort(key: 'label' | 'kind' | 'category' | 'liquidity' | 'valueInr' | 'note'): void {
    if (this.sortKey === key) this.sortDir = this.sortDir === 1 ? -1 : 1;
    else {
      this.sortKey = key;
      this.sortDir = 1;
    }
    this.applySort();
    this.cdr.markForCheck();
  }

  sortIndicator(key: string): string {
    if (this.sortKey !== key) return '';
    return this.sortDir === 1 ? '↑' : '↓';
  }

  /** Recomputed on load/sort/search/save (not per keystroke in a row) so edits don't reshuffle. */
  private applySort(): void {
    const rows = this.items.filter((i) => this.matches([i.label, i.category, i.kind, i.liquidity, i.note], this.itemsSearch));
    const key = this.sortKey;
    if (!key) {
      this.sortedItems = rows; // server order: kind, category, id
      return;
    }
    const dir = this.sortDir;
    this.sortedItems = [...rows].sort((a, b) => {
      if (key === 'valueInr') return this.byNum(Number(a.valueInr) || 0, Number(b.valueInr) || 0, dir);
      return this.byText(a[key], b[key], dir);
    });
  }

  onItemsSearchChange(): void {
    this.applySort();
  }

  // --- Tracked (auto-populated) assets ---

  get trackedRows(): TrackedRow[] {
    return [
      {
        key: 'nvda',
        label: 'NVIDIA (NVDA)',
        category: 'Stocks (US)',
        liquidity: 'liquid',
        valueInr: this.nvdaNetInr,
        hint: 'Net in hand if sold today (after tax) — from Holdings',
      },
      {
        key: 'mf',
        label: 'Mutual funds',
        category: 'Mutual funds',
        liquidity: 'liquid',
        valueInr: this.mfValueInr,
        hint: 'Live AMFI NAV × units — from ICICI Direct tab',
      },
      {
        key: 'equity-in',
        label: 'ICICI equity holdings',
        category: 'Stocks (India)',
        liquidity: 'liquid',
        valueInr: this.iciciEquityValueInr,
        hint: this.iciciEquityLoaded
          ? 'Live market value — from ICICI Direct'
          : 'Connect ICICI Direct and load holdings to include this',
      },
    ];
  }

  /** Unfiltered: this is the real tracked total, and the manual-assets figure is derived from it. */
  get trackedTotalInr(): number {
    return this.trackedRows.reduce((s, r) => s + (r.valueInr ?? 0), 0);
  }

  // --- Tracked table: search + sort ---
  trackedSearch = '';
  trackedSortKey: 'label' | 'category' | 'liquidity' | 'value' | 'source' | '' = '';
  trackedSortDir: 1 | -1 = 1;

  setTrackedSort(key: 'label' | 'category' | 'liquidity' | 'value' | 'source'): void {
    if (this.trackedSortKey === key) this.trackedSortDir = this.trackedSortDir === 1 ? -1 : 1;
    else {
      this.trackedSortKey = key;
      this.trackedSortDir = 1;
    }
  }

  trackedSortIndicator(key: string): string {
    if (this.trackedSortKey !== key) return '';
    return this.trackedSortDir === 1 ? '↑' : '↓';
  }

  get trackedRowsView(): TrackedRow[] {
    let rows = this.trackedRows.filter((r) => this.matches([r.label, r.category, r.liquidity, r.hint], this.trackedSearch));
    const key = this.trackedSortKey;
    if (key) {
      const dir = this.trackedSortDir;
      rows = [...rows].sort((a, b) => {
        if (key === 'value') return this.byNum(a.valueInr, b.valueInr, dir);
        if (key === 'source') return this.byText(a.hint, b.hint, dir);
        return this.byText(a[key], b[key], dir);
      });
    }
    return rows;
  }

  // --- Manual rows ---

  get manualAssets(): NetworthItem[] {
    return this.items.filter((i) => i.kind === 'asset');
  }

  get manualLiabilities(): NetworthItem[] {
    return this.items.filter((i) => i.kind === 'liability');
  }

  get totals(): {
    assets: number; liabilities: number; net: number;
    liquid: number; illiquid: number; liquidPct: number; illiquidPct: number;
  } {
    let assets = 0;
    let liabilities = 0;
    let liquid = 0;
    let illiquid = 0;
    for (const r of this.trackedRows) {
      const v = r.valueInr ?? 0;
      assets += v;
      if (r.liquidity === 'liquid') liquid += v;
      else illiquid += v;
    }
    for (const it of this.items) {
      const v = Number(it.valueInr) || 0;
      if (it.kind === 'liability') {
        liabilities += v;
        continue;
      }
      assets += v;
      if (it.liquidity === 'liquid') liquid += v;
      else illiquid += v;
    }
    return {
      assets,
      liabilities,
      net: assets - liabilities,
      liquid,
      illiquid,
      liquidPct: assets > 0 ? (liquid / assets) * 100 : 0,
      illiquidPct: assets > 0 ? (illiquid / assets) * 100 : 0,
    };
  }

  /** Asset mix by category, biggest first (for the breakdown bars). */
  get categoryBreakdown(): Array<{ category: string; valueInr: number; pct: number }> {
    const map = new Map<string, number>();
    const add = (cat: string, v: number) => map.set(cat, (map.get(cat) ?? 0) + v);
    for (const r of this.trackedRows) if (r.valueInr != null) add(r.category, r.valueInr);
    for (const it of this.manualAssets) add(it.category, Number(it.valueInr) || 0);
    const total = [...map.values()].reduce((s, v) => s + v, 0);
    return [...map.entries()]
      .filter(([, v]) => v > 0)
      .map(([category, valueInr]) => ({ category, valueInr, pct: total > 0 ? (valueInr / total) * 100 : 0 }))
      .sort((a, b) => b.valueInr - a.valueInr);
  }

  // --- Mutations ---

  onNewKindChange(): void {
    this.newCategory = this.categoryOptions(this.newKind)[0];
    this.onNewCategoryChange();
  }

  onNewCategoryChange(): void {
    if (this.newKind === 'asset') {
      this.newLiquidity = this.defaultLiquidity[this.newCategory] ?? 'liquid';
    }
  }

  addItem(): void {
    const label = this.newLabel.trim();
    if (!label) {
      this.error = 'Give the row a name first.';
      this.cdr.markForCheck();
      return;
    }
    this.adding = true;
    this.error = null;
    this.cdr.markForCheck();
    this.dashboardService
      .addNetworthItem({
        label,
        category: this.newCategory,
        liquidity: this.newLiquidity,
        kind: this.newKind,
        valueInr: Number(this.newValue) || 0,
        note: this.newNote.trim(),
      })
      .subscribe({
        next: () => {
          this.adding = false;
          this.newLabel = '';
          this.newValue = null;
          this.newNote = '';
          this.loadItems();
        },
        error: (err) => {
          this.adding = false;
          this.error = err?.error?.error || 'Could not add the row.';
          this.cdr.markForCheck();
        },
      });
  }

  /** Persist an inline edit (called on blur / select change). */
  saveItem(item: NetworthItem): void {
    this.savingId = item.id;
    this.error = null;
    this.cdr.markForCheck();
    this.dashboardService
      .updateNetworthItem(item.id, {
        label: item.label,
        category: item.category,
        liquidity: item.liquidity,
        kind: item.kind,
        valueInr: Number(item.valueInr) || 0,
        note: item.note ?? '',
      })
      .subscribe({
        next: () => {
          this.savingId = null;
          this.applySort(); // edit committed: let the row settle into its sorted position
          this.cdr.markForCheck();
        },
        error: (err) => {
          this.savingId = null;
          this.error = err?.error?.error || 'Could not save the change.';
          this.cdr.markForCheck();
        },
      });
  }

  deleteItem(item: NetworthItem): void {
    if (!confirm(`Remove "${item.label}" from your net worth?`)) return;
    this.dashboardService.deleteNetworthItem(item.id).subscribe({
      next: () => this.loadItems(),
      error: (err) => {
        this.error = err?.error?.error || 'Could not remove the row.';
        this.cdr.markForCheck();
      },
    });
  }

  formatInr(n: number | null | undefined): string {
    if (n == null || !Number.isFinite(n)) return '—';
    return new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0 }).format(Math.round(n));
  }

  /** Compact ₹ label for the headline (e.g. "1.06 Cr", "45.2 L"). */
  formatInrShort(n: number | null | undefined): string {
    if (n == null || !Number.isFinite(n)) return '—';
    const abs = Math.abs(n);
    const sign = n < 0 ? '-' : '';
    if (abs >= 1e7) return `${sign}${(abs / 1e7).toFixed(2)} Cr`;
    if (abs >= 1e5) return `${sign}${(abs / 1e5).toFixed(2)} L`;
    return `${sign}${this.formatInr(abs)}`;
  }
}
