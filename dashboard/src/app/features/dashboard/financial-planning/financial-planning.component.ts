import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import {
  FinancialRequirement,
  FinancialSimRow,
  MonthlyExpenseStream,
  MonthlyIncomeStream,
  PortfolioBucket,
  RentalIncomeStream,
  YearlyExpenseStream,
  YearlyIncomeStream,
} from './financial-planning.types';

/** Normalize shares to sum to 1 without mutating inputs */
function normalizedShareWeights(buckets: PortfolioBucket[]): number[] {
  const raw = buckets.map((b) => Math.max(0, b.sharePct));
  const sum = raw.reduce((a, b) => a + b, 0);
  if (sum <= 0) return buckets.map(() => 1 / buckets.length);
  return raw.map((r) => r / sum);
}

function netPortfolioGrowthFactor(buckets: PortfolioBucket[]): number {
  const w = normalizedShareWeights(buckets);
  return buckets.reduce((acc, b, i) => {
    const r = Math.max(0, b.returnPct) / 100;
    const t = Math.max(0, b.taxPct) / 100;
    return acc + w[i] * r * (1 - t);
  }, 0);
}

const WORD_ONES = [
  '',
  'one',
  'two',
  'three',
  'four',
  'five',
  'six',
  'seven',
  'eight',
  'nine',
  'ten',
  'eleven',
  'twelve',
  'thirteen',
  'fourteen',
  'fifteen',
  'sixteen',
  'seventeen',
  'eighteen',
  'nineteen',
];
const WORD_TENS = ['', '', 'twenty', 'thirty', 'forty', 'fifty', 'sixty', 'seventy', 'eighty', 'ninety'];

function wordsUnder100(v: number): string {
  if (v < 20) return WORD_ONES[v];
  const t = Math.floor(v / 10);
  const u = v % 10;
  return WORD_TENS[t] + (u ? ' ' + WORD_ONES[u] : '');
}

function wordsUnder1000(v: number): string {
  if (v < 100) return wordsUnder100(v);
  const h = Math.floor(v / 100);
  const r = v % 100;
  return WORD_ONES[h] + ' hundred' + (r ? ' ' + wordsUnder100(r) : '');
}

function capFirst(s: string): string {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}

/** Integer in words (thousand / million / billion chunks) — for rounded “X crore” headlines */
function integerToWesternWords(n: number): string {
  const x = Math.floor(Math.max(0, n));
  if (x === 0) return 'zero';
  let v = x;
  const chunks: number[] = [];
  while (v > 0) {
    chunks.push(v % 1000);
    v = Math.floor(v / 1000);
  }
  const scales = ['', 'thousand', 'million', 'billion', 'trillion'];
  const parts: string[] = [];
  for (let i = chunks.length - 1; i >= 0; i--) {
    const c = chunks[i];
    if (c === 0) continue;
    const scale = scales[i] ?? 'trillion';
    const piece = wordsUnder1000(c).trim();
    parts.push(scale ? `${piece} ${scale}` : piece);
  }
  return parts.join(' ');
}

/** Round balance to nearest whole crore for a short headline (e.g. ~5,044 crore) */
function estateCroresHeadline(inr: number): { croresRounded: number; words: string } {
  const n = Math.max(0, Math.round(inr));
  const croresRounded = Math.round(n / 1e7);
  if (croresRounded === 0) {
    const words = n <= 0 ? 'Zero crore rupees' : 'Less than one crore rupees';
    return { croresRounded: 0, words };
  }
  return {
    croresRounded,
    words: capFirst(integerToWesternWords(croresRounded)) + ' crore rupees',
  };
}

@Component({
  selector: 'app-financial-planning',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './financial-planning.component.html',
  styleUrl: './financial-planning.component.scss',
})
export class FinancialPlanningComponent {
  /** Dashboard Overview “In bank if you sell now”; optional one-click fill for NVIDIA net field */
  @Input() dashboardNetInBankIfSellNow: number | null = null;

  /** Timeline */
  currentAge = 36;
  retirementAge = 37;
  lifeExpectancy = 80;

  inflationPct = 7;
  capitalGainsTaxPct = 12.5;
  incomeTaxPct = 25;

  /** Starting corpus pieces (INR, today’s money) */
  startingCashSavingsInr = 500_000;
  startingIndianMarketInr = 2_000_000;
  /** NVDA position: value in INR if you sold everything net of tax (“net in bank” style) */
  nvidiaNetIfSellInr = 106_491_755;
  /** Of `nvidiaNetIfSellInr`, share left in NVIDIA (0–100); the rest is treated as cash for the breakdown */
  percentKeepInNvidia = 40;
  /** Annual return % on the NVIDIA-kept tranche (before tax); tax uses capital gains % below */
  nvidiaGrowthPct = 15;

  /** Total starting portfolio for simulation = cash + Indian + NVIDIA (split does not change total) */
  get portfolioStartingTotalInr(): number {
    const cash = Math.max(0, this.startingCashSavingsInr);
    const ind = Math.max(0, this.startingIndianMarketInr);
    const n = Math.max(0, this.nvidiaNetIfSellInr);
    return Math.round(cash + ind + n);
  }

  get nvidiaKeepFraction(): number {
    return Math.min(100, Math.max(0, this.percentKeepInNvidia)) / 100;
  }

  /** Cash bucket after moving (1 − keep%) of NVIDIA to cash */
  get effectiveCashIncludingNvidiaSoldInr(): number {
    const cash = Math.max(0, this.startingCashSavingsInr);
    const n = Math.max(0, this.nvidiaNetIfSellInr);
    return Math.round(cash + n * (1 - this.nvidiaKeepFraction));
  }

  /** Portion of NVIDIA net still held as shares */
  get nvidiaKeptInr(): number {
    return Math.round(Math.max(0, this.nvidiaNetIfSellInr) * this.nvidiaKeepFraction);
  }

  /**
   * Starting balance that follows the working/retired portfolio mix (cash + Indian + NVDA treated as sold).
   * The NVIDIA-kept slice grows separately at `nvidiaGrowthPct` (net of capital gains tax).
   */
  get startingRestPortfolioInr(): number {
    return Math.round(Math.max(0, this.effectiveCashIncludingNvidiaSoldInr) + Math.max(0, this.startingIndianMarketInr));
  }

  /** Net annual growth rate on kept NVIDIA (same shape as portfolio buckets: return × (1 − tax)) */
  get nvidiaNetGrowthFactor(): number {
    const r = Math.max(0, this.nvidiaGrowthPct) / 100;
    const t = Math.max(0, this.capitalGainsTaxPct) / 100;
    return r * (1 - t);
  }

  /** Post-retirement budget: monthly in today’s INR (×12 for annual, then inflated) */
  postRetirementMonthlyTodaysInr = 200_000;

  /** Annual step-up on “additional savings” while earning (%) */
  stepUpSavingsPct = 0;

  /** If true, use computed surplus as annual contribution while earning; else use manual */
  useComputedAnnualContribution = true;
  /** Manual annual contribution while working (INR, today’s money); inflated by step-up */
  manualAnnualContributionInr = 3_660_000;

  /**
   * Monthly income while earning (after tax), excluding rentals. Add/remove rows.
   * `earningYearsCap`: 0 = all earning years until retirement; N &gt; 0 = only first N plan years (while earning).
   */
  incomeStreams: MonthlyIncomeStream[] = [
    { id: 'mi1', label: 'My Salary', monthlyTodaysInr: 240_000, earningYearsCap: 0 },
    { id: 'mi2', label: 'Wife Salary', monthlyTodaysInr: 170_000, earningYearsCap: 8 },
  ];

  /**
   * Multiple rental streams; each runs from its start year to end of plan. Amounts use global inflation
   * plus optional `incrementPctPerYear` compounded from the first rent year.
   */
  rentalStreams: RentalIncomeStream[] = [
    { id: 'r1', label: 'Palladium Homes', monthlyTodaysInr: 25_000, startsInYears: 0, incrementPctPerYear: 10 },
    { id: 'r2', label: 'Godrej Elaris Shop 36', monthlyTodaysInr: 50_000, startsInYears: 3, incrementPctPerYear: 5 },
  ];

  /** Yearly income lines (bonuses etc.); add/remove. Today’s INR, inflated in surplus. */
  yearlyIncomeStreams: YearlyIncomeStream[] = [];

  /**
   * Monthly expense lines (add/remove like rentals). `endsAfterYears`: 0 = full plan; N &gt; 0 = years 0 … N−1 only.
   * Defaults only include non-zero lines; use + Add expense for more.
   */
  expenseStreams: MonthlyExpenseStream[] = [
    { id: 'e2', label: 'Loan EMI 1', monthlyTodaysInr: 46_000, category: 'need', endsAfterYears: 6 },
    { id: 'e5', label: 'Living 1', monthlyTodaysInr: 90_000, category: 'need', endsAfterYears: 0 },
    { id: 'e8', label: 'Desire 1', monthlyTodaysInr: 30_000, category: 'want', endsAfterYears: 0 },
  ];
  readonly expenseCategoryOptions: ('need' | 'want')[] = ['need', 'want'];

  /** Yearly lump expenses; add/remove. Today’s INR, inflated like monthly expenses. */
  yearlyExpenseStreams: YearlyExpenseStream[] = [{ id: 'ye1', label: 'Travel', yearlyTodaysInr: 500_000 }];

  workingPortfolio: PortfolioBucket[] = [
    { label: 'Fixed', returnPct: 7, taxPct: 12.5, sharePct: 30 },
    { label: 'Large cap MF', returnPct: 12, taxPct: 12.5, sharePct: 20 },
    { label: 'Midcap MF', returnPct: 15, taxPct: 12.5, sharePct: 30 },
    { label: 'Smallcap MF', returnPct: 18, taxPct: 12.5, sharePct: 20 },
  ];

  retiredPortfolio: PortfolioBucket[] = [
    { label: 'Fixed', returnPct: 7, taxPct: 12.5, sharePct: 50 },
    { label: 'Large cap MF', returnPct: 12, taxPct: 12.5, sharePct: 25 },
    { label: 'Midcap MF', returnPct: 15, taxPct: 12.5, sharePct: 15 },
    { label: 'Smallcap MF', returnPct: 18, taxPct: 12.5, sharePct: 10 },
  ];

  requirements: FinancialRequirement[] = [
    {
      id: '1',
      name: "Daughter's Education",
      yearsFromNow: 7,
      durationYears: 1,
      yearlyAmountTodaysInr: 2_500_000,
    },
    {
      id: '2',
      name: "Daughter's Marriage",
      yearsFromNow: 15,
      durationYears: 1,
      yearlyAmountTodaysInr: 5_000_000,
    },
  ];

  readonly Math = Math;

  applyDashboardNvidiaNet(): void {
    const v = this.dashboardNetInBankIfSellNow;
    if (v != null && Number.isFinite(v) && v > 0) this.nvidiaNetIfSellInr = Math.round(v);
  }

  formatInr(n: number): string {
    return new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0, minimumFractionDigits: 0 }).format(Math.round(n));
  }

  /** Income stream counts in simulation year y only while earning and within `earningYearsCap` (if set). */
  monthlyIncomeStreamActiveWhileEarning(y: number, st: MonthlyIncomeStream): boolean {
    const ca = Math.max(0, Math.floor(this.currentAge));
    const ra = Math.max(0, Math.floor(this.retirementAge));
    if (ca + y >= ra) return false;
    const cap = Math.floor(st.earningYearsCap ?? 0);
    if (cap <= 0) return true;
    return y < cap;
  }

  /** Monthly ₹ (today’s) for one stream in year y, or 0 if inactive */
  monthlyIncomeStreamTodaysInYear(y: number, st: MonthlyIncomeStream): number {
    if (!this.monthlyIncomeStreamActiveWhileEarning(y, st)) return 0;
    const v = st.monthlyTodaysInr;
    return Number.isFinite(v) ? Math.max(0, v) : 0;
  }

  /** Sum of monthly income streams (today’s ₹) active in year y — excludes rentals */
  monthlyIncomeTodaysTotalForYear(y: number): number {
    return this.incomeStreams.reduce((s, st) => s + this.monthlyIncomeStreamTodaysInYear(y, st), 0);
  }

  /** Nominal monthly ₹ from income streams in simulation year y (inflated) */
  monthlyIncomeNominalTotalForYear(y: number): number {
    const f = this.inflFactor(y);
    let s = 0;
    for (const st of this.incomeStreams) {
      if (!this.monthlyIncomeStreamActiveWhileEarning(y, st)) continue;
      const v = st.monthlyTodaysInr;
      s += (Number.isFinite(v) ? Math.max(0, v) : 0) * f;
    }
    return s;
  }

  /** All income streams at full monthly rate (ignores earning cap) + all rentals at full rate */
  get baseMonthlyIncomeFullInr(): number {
    const core = this.incomeStreams.reduce((s, st) => s + (Number.isFinite(st.monthlyTodaysInr) ? Math.max(0, st.monthlyTodaysInr) : 0), 0);
    return core + this.rentalStreams.reduce((s, x) => s + Math.max(0, x.monthlyTodaysInr), 0);
  }

  /** Monthly total in “today’s” INR for summary at plan start (year 0): income streams + rentals that have started */
  get totalMonthlyIncomeInr(): number {
    return this.monthlyIncomeTodaysTotalForYear(0) + this.rentalMonthlyTodaysTotalAtYear0();
  }

  /** All rental lines at base monthly (today’s ₹) when every stream is active — headline “full rental” */
  get totalMonthlyIncomeFullRunRateInr(): number {
    return this.baseMonthlyIncomeFullInr;
  }

  addIncomeStream(): void {
    const n = this.incomeStreams.length + 1;
    this.incomeStreams = [
      ...this.incomeStreams,
      { id: `mi-${Date.now()}`, label: `Income ${n}`, monthlyTodaysInr: 0, earningYearsCap: 0 },
    ];
  }

  removeIncomeStream(id: string): void {
    this.incomeStreams = this.incomeStreams.filter((s) => s.id !== id);
  }

  /** Sum of monthly (today’s ₹) for streams active in year index 0 */
  rentalMonthlyTodaysTotalAtYear0(): number {
    return this.rentalStreams.reduce((acc, st) => {
      if (Math.max(0, Math.floor(st.startsInYears)) > 0) return acc;
      return acc + Math.max(0, st.monthlyTodaysInr);
    }, 0);
  }

  get hasRentalStartingAfterYear0(): boolean {
    return this.rentalStreams.some((s) => Math.floor(s.startsInYears) > 0 && s.monthlyTodaysInr > 0);
  }

  /**
   * One stream’s nominal monthly ₹ in simulation year y.
   * incrementPctPerYear is the TOTAL nominal annual growth rate.
   * If 0, rent keeps up with global inflation only (real value preserved).
   * If > 0, rent grows at this rate per year from its start (e.g. 10% = 10% total nominal growth).
   */
  rentalStreamMonthlyNominal(y: number, st: RentalIncomeStream): number {
    const start = Math.max(0, Math.floor(st.startsInYears));
    if (y < start) return 0;
    const base = Math.max(0, st.monthlyTodaysInr);
    const inc = Math.max(0, st.incrementPctPerYear) / 100;
    const yrsSince = y - start;
    if (inc <= 0) {
      return base * this.inflFactor(y);
    }
    return base * this.inflFactor(start) * Math.pow(1 + inc, yrsSince);
  }

  rentalMonthlyNominalTotalForYear(y: number): number {
    return this.rentalStreams.reduce((s, st) => s + this.rentalStreamMonthlyNominal(y, st), 0);
  }

  addRentalStream(): void {
    const n = this.rentalStreams.length + 1;
    this.rentalStreams = [
      ...this.rentalStreams,
      { id: `r${Date.now()}`, label: `Rental ${n}`, monthlyTodaysInr: 0, startsInYears: 0, incrementPctPerYear: 0 },
    ];
  }

  removeRentalStream(id: string): void {
    this.rentalStreams = this.rentalStreams.filter((r) => r.id !== id);
  }

  get totalYearlyExtraIncomeInr(): number {
    return this.yearlyIncomeStreams.reduce((a, st) => a + (Number.isFinite(st.yearlyTodaysInr) ? Math.max(0, st.yearlyTodaysInr) : 0), 0);
  }

  addYearlyIncomeStream(): void {
    const n = this.yearlyIncomeStreams.length + 1;
    this.yearlyIncomeStreams = [
      ...this.yearlyIncomeStreams,
      { id: `yi-${Date.now()}`, label: `Yearly income ${n}`, yearlyTodaysInr: 0 },
    ];
  }

  removeYearlyIncomeStream(id: string): void {
    this.yearlyIncomeStreams = this.yearlyIncomeStreams.filter((s) => s.id !== id);
  }

  get grandTotalYearlyIncomeInr(): number {
    return this.totalMonthlyIncomeInr * 12 + this.totalYearlyExtraIncomeInr;
  }

  /** Expense stream applies in simulation year index y */
  expenseStreamActiveInYear(y: number, st: MonthlyExpenseStream): boolean {
    const cap = Math.floor(st.endsAfterYears ?? 0);
    if (cap <= 0) return true;
    return y < cap;
  }

  addExpenseStream(): void {
    const n = this.expenseStreams.length + 1;
    this.expenseStreams = [
      ...this.expenseStreams,
      {
        id: `exp-${Date.now()}`,
        label: `Expense ${n}`,
        monthlyTodaysInr: 0,
        category: 'need',
        endsAfterYears: 0,
      },
    ];
  }

  removeExpenseStream(id: string): void {
    this.expenseStreams = this.expenseStreams.filter((e) => e.id !== id);
  }

  /** Sum of monthly expense lines active at plan start (today’s ₹) */
  get totalMonthlyExpenseInr(): number {
    let s = 0;
    for (const st of this.expenseStreams) {
      if (!this.expenseStreamActiveInYear(0, st)) continue;
      const v = st.monthlyTodaysInr;
      s += Number.isFinite(v) ? Math.max(0, v) : 0;
    }
    return s;
  }

  /** Total monthly expenses in nominal ₹ for simulation year y */
  monthlyExpenseNominalTotalForYear(y: number): number {
    const f = this.inflFactor(y);
    let s = 0;
    for (const st of this.expenseStreams) {
      if (!this.expenseStreamActiveInYear(y, st)) continue;
      const v = st.monthlyTodaysInr;
      s += (Number.isFinite(v) ? Math.max(0, v) : 0) * f;
    }
    return s;
  }

  get totalYearlyLumpyExpenseInr(): number {
    return this.yearlyExpenseStreams.reduce((a, st) => a + (Number.isFinite(st.yearlyTodaysInr) ? Math.max(0, st.yearlyTodaysInr) : 0), 0);
  }

  addYearlyExpenseStream(): void {
    const n = this.yearlyExpenseStreams.length + 1;
    this.yearlyExpenseStreams = [
      ...this.yearlyExpenseStreams,
      { id: `yl-${Date.now()}`, label: `Yearly expense ${n}`, yearlyTodaysInr: 0 },
    ];
  }

  removeYearlyExpenseStream(id: string): void {
    this.yearlyExpenseStreams = this.yearlyExpenseStreams.filter((s) => s.id !== id);
  }

  get totalYearlyExpenseInr(): number {
    return this.totalMonthlyExpenseInr * 12 + this.totalYearlyLumpyExpenseInr;
  }

  get monthlySurplusInr(): number {
    return this.totalMonthlyIncomeInr - this.totalMonthlyExpenseInr;
  }

  get yearlySurplusInr(): number {
    return this.grandTotalYearlyIncomeInr - this.totalYearlyExpenseInr;
  }

  get monthlySurplusPctOfIncome(): number {
    const t = this.totalMonthlyIncomeInr;
    return t > 0 ? (this.monthlySurplusInr / t) * 100 : 0;
  }

  get yearlySurplusPctOfIncome(): number {
    const t = this.grandTotalYearlyIncomeInr;
    return t > 0 ? (this.yearlySurplusInr / t) * 100 : 0;
  }

  /** Nominal annual rental (all streams) for simulation year index y */
  rentalAnnualNominalInr(y: number): number {
    return this.rentalMonthlyNominalTotalForYear(y) * 12;
  }

  /** Nominal yearly income − recurring expenses for surplus (year index y); used while earning */
  yearlySurplusNominalForYear(y: number): number {
    const f = this.inflFactor(y);
    const coreM = this.monthlyIncomeNominalTotalForYear(y);
    const rentM = this.rentalMonthlyNominalTotalForYear(y);
    const yearlyInc = (coreM + rentM) * 12 + this.totalYearlyExtraIncomeInr * f;
    const yearlyExp = this.monthlyExpenseNominalTotalForYear(y) * 12 + this.totalYearlyLumpyExpenseInr * f;
    return yearlyInc - yearlyExp;
  }

  surplusQuality(pct: number): string {
    if (pct >= 50) return 'Strong surplus';
    if (pct >= 25) return 'This is good';
    if (pct >= 10) return 'Moderate';
    if (pct >= 0) return 'Tight';
    return 'Deficit';
  }

  weightedPortfolioStats(buckets: PortfolioBucket[]): { ret: number; tax: number; netGrowth: number } {
    const w = normalizedShareWeights(buckets);
    let wRet = 0;
    let wTax = 0;
    for (let i = 0; i < buckets.length; i++) {
      wRet += w[i] * buckets[i].returnPct;
      wTax += w[i] * buckets[i].taxPct;
    }
    return { ret: wRet, tax: wTax, netGrowth: netPortfolioGrowthFactor(buckets) * 100 };
  }

  get workingStats(): { ret: number; tax: number; netGrowth: number } {
    return this.weightedPortfolioStats(this.workingPortfolio);
  }

  get retiredStats(): { ret: number; tax: number; netGrowth: number } {
    return this.weightedPortfolioStats(this.retiredPortfolio);
  }

  portfolioShareSum(buckets: PortfolioBucket[]): number {
    return buckets.reduce((s, b) => s + (Number.isFinite(b.sharePct) ? b.sharePct : 0), 0);
  }

  addRequirement(): void {
    const id = `${Date.now()}`;
    this.requirements = [
      ...this.requirements,
      { id, name: 'New goal', yearsFromNow: 10, durationYears: 1, yearlyAmountTodaysInr: 0 },
    ];
  }

  removeRequirement(id: string): void {
    this.requirements = this.requirements.filter((r) => r.id !== id);
  }

  /**
   * Binary-search the highest `postRetirementMonthlyTodaysInr` (in today's ₹) that keeps
   * the portfolio ending balance ≥ 0 at end of the last projected year (age = lifeExpectancy).
   * Returns the value rounded down to nearest ₹1,000 for safety margin.
   */
  get maxSustainableMonthlyWithdrawal(): number {
    const ca = Math.max(0, Math.floor(this.currentAge));
    const ra = Math.max(ca, Math.floor(this.retirementAge));
    const le = Math.max(ra, Math.floor(this.lifeExpectancy));
    const years = le - ca + 1;
    if (years <= 0 || years > 120) return 0;

    const step = Math.max(0, this.stepUpSavingsPct) / 100;
    const gWork = netPortfolioGrowthFactor(this.workingPortfolio);
    const gRet = netPortfolioGrowthFactor(this.retiredPortfolio);
    const gNv = this.nvidiaNetGrowthFactor;
    const manualBase = Math.max(0, this.manualAnnualContributionInr);

    const simulate = (monthlyDraw: number): number => {
      let bR = Math.max(0, this.startingRestPortfolioInr);
      let bN = Math.max(0, this.nvidiaKeptInr);
      for (let y = 0; y < years; y++) {
        const age = ca + y;
        const retired = age >= ra;
        const f = this.inflFactor(y);
        const rentalAnnual = this.rentalAnnualNominalInr(y);

        let planned = 0;
        if (retired) planned = monthlyDraw * 12 * f;

        let addl = 0;
        for (const req of this.requirements) {
          const s = Math.max(0, Math.floor(req.yearsFromNow));
          const d = Math.max(1, Math.floor(req.durationYears));
          if (y >= s && y < s + d) addl += Math.max(0, req.yearlyAmountTodaysInr) * f;
        }

        let contrib = 0;
        if (!retired) {
          if (this.useComputedAnnualContribution) {
            contrib = Math.max(0, this.yearlySurplusNominalForYear(y)) * Math.pow(1 + step, y);
          } else {
            contrib = manualBase * f * Math.pow(1 + step, y);
          }
        }

        const gR = retired ? gRet : gWork;
        let R = bR * (1 + gR) + contrib;
        if (retired) R += rentalAnnual;
        const N = bN * (1 + gNv);

        const E = planned + addl;
        const total = R + N;
        if (total <= 0 || E >= total) return -1;
        const rend = R - (E * R) / total;
        const nend = N - (E * N) / total;
        bR = Math.max(0, rend);
        bN = Math.max(0, nend);
      }
      return bR + bN;
    };

    let lo = 0;
    let hi = 100_000_000;
    for (let i = 0; i < 60; i++) {
      const mid = (lo + hi) / 2;
      if (simulate(mid) > 0) lo = mid;
      else hi = mid;
    }
    return Math.floor(lo / 1000) * 1000;
  }

  applyMaxWithdrawal(): void {
    this.postRetirementMonthlyTodaysInr = this.maxSustainableMonthlyWithdrawal;
  }

  /** Inflation factor from simulation start to year index y (0-based) */
  private inflFactor(y: number): number {
    const inf = Math.max(0, this.inflationPct) / 100;
    return Math.pow(1 + inf, y);
  }

  /**
   * Last-row ending balance (nominal). `todaysInr` divides by `inflFactor(lastYearIndex)` — same cumulative
   * factor as inflated flows in the final year — for rough purchasing power vs plan start.
   */
  get endOfLifeEstate(): {
    inr: number;
    croresRounded: number;
    words: string;
    todaysInr: number;
    todaysCroresRounded: number;
    todaysWords: string;
    deflateExponent: number;
  } {
    const rows = this.simulationRows;
    if (!rows.length) {
      const z = estateCroresHeadline(0);
      return {
        inr: 0,
        croresRounded: z.croresRounded,
        words: z.words,
        todaysInr: 0,
        todaysCroresRounded: z.croresRounded,
        todaysWords: z.words,
        deflateExponent: 0,
      };
    }
    const inr = rows[rows.length - 1].endingInr;
    const yLast = rows.length - 1;
    const f = this.inflFactor(yLast);
    const todaysInr = f > 0 ? Math.round(inr / f) : inr;
    const { croresRounded, words } = estateCroresHeadline(inr);
    const t = estateCroresHeadline(todaysInr);
    return {
      inr,
      croresRounded,
      words,
      todaysInr,
      todaysCroresRounded: t.croresRounded,
      todaysWords: t.words,
      deflateExponent: yLast,
    };
  }

  get simulationRows(): FinancialSimRow[] {
    const ca = Math.max(0, Math.floor(this.currentAge));
    const ra = Math.max(ca, Math.floor(this.retirementAge));
    const le = Math.max(ra, Math.floor(this.lifeExpectancy));
    const years = le - ca + 1;
    if (years <= 0 || years > 120) return [];

    const step = Math.max(0, this.stepUpSavingsPct) / 100;

    const gWork = netPortfolioGrowthFactor(this.workingPortfolio);
    const gRet = netPortfolioGrowthFactor(this.retiredPortfolio);
    const gNv = this.nvidiaNetGrowthFactor;

    const rows: FinancialSimRow[] = [];
    let balanceRest = Math.max(0, this.startingRestPortfolioInr);
    let balanceNvidia = Math.max(0, this.nvidiaKeptInr);

    const manualBase = Math.max(0, this.manualAnnualContributionInr);

    for (let y = 0; y < years; y++) {
      const age = ca + y;
      const retired = age >= ra;
      const status: 'Earning' | 'Retired' = retired ? 'Retired' : 'Earning';

      const f = this.inflFactor(y);
      const rentalAnnual = this.rentalAnnualNominalInr(y);

      let planned = 0;
      if (retired) {
        planned = this.postRetirementMonthlyTodaysInr * 12 * f;
      }

      let addl = 0;
      for (const req of this.requirements) {
        const start = Math.max(0, Math.floor(req.yearsFromNow));
        const dur = Math.max(1, Math.floor(req.durationYears));
        if (y >= start && y < start + dur) {
          addl += Math.max(0, req.yearlyAmountTodaysInr) * f;
        }
      }

      let contrib = 0;
      if (!retired) {
        if (this.useComputedAnnualContribution) {
          const sur = this.yearlySurplusNominalForYear(y);
          contrib = Math.max(0, sur) * Math.pow(1 + step, y);
        } else {
          contrib = manualBase * f * Math.pow(1 + step, y);
        }
      }

      const gRest = retired ? gRet : gWork;
      const r0 = balanceRest;
      const n0 = balanceNvidia;
      const startingInr = Math.round(r0 + n0);

      // Rest pool: portfolio return; new savings and (when retired) rent go here. NVIDIA pool: separate return.
      let R = r0 * (1 + gRest) + contrib;
      if (retired) {
        R += rentalAnnual;
      }
      const N = n0 * (1 + gNv);

      const E = planned + addl;
      const totalAfter = R + N;
      let ending: number;
      let warning = '';

      if (totalAfter <= 0) {
        ending = 0;
        warning = 'Negative balance';
        balanceRest = 0;
        balanceNvidia = 0;
      } else if (E >= totalAfter) {
        ending = 0;
        warning = 'Negative balance';
        balanceRest = 0;
        balanceNvidia = 0;
      } else {
        const takeR = (E * R) / totalAfter;
        const takeN = (E * N) / totalAfter;
        const rend = R - takeR;
        const nend = N - takeN;
        ending = Math.round(rend + nend);
        balanceRest = Math.round(Math.max(0, rend));
        balanceNvidia = Math.round(Math.max(0, nend));
      }

      if (!warning && retired && E > startingInr + r0 * gRest + n0 * gNv + rentalAnnual + 1) {
        warning = 'Drawdown exceeds income + growth';
      }

      rows.push({
        age,
        startingInr,
        plannedExpensesInr: planned,
        additionalExpensesInr: addl,
        rentalIncomeAnnualInr: rentalAnnual,
        additionalSavingsInr: contrib,
        endingInr: ending,
        status,
        warning,
      });
    }

    return rows;
  }
}
