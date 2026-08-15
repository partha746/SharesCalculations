/** ICICI Direct Breeze API status for a single account. */
export interface BreezeAccountStatus {
  sdkInstalled: boolean;
  configured: boolean;
  connected: boolean;
  /** Account holder name (from ICICI customer details) when connected. */
  name?: string | null;
  /** User-supplied label for custom (DB-stored) accounts. */
  label?: string | null;
  /** True for accounts added at runtime (removable); false for built-in env accounts. */
  custom?: boolean;
  loginUrl: string | null;
  callbackUrl?: string | null;
  message?: string;
}

/** Combined status response: { accounts: { "1": ..., "2": ... } }. */
export interface BreezeStatusAllResponse {
  accounts: Record<string, BreezeAccountStatus>;
}

/** Legacy single-account shape (kept for per-account endpoint). */
export type BreezeStatusResponse = BreezeAccountStatus;

/** Sortable column keys for the combined (Equity + MF) ICICI holdings table. */
export type IciciSortCol =
  | 'type'
  | 'account'
  | 'name'
  | 'qty'
  | 'avg'
  | 'price'
  | 'invested'
  | 'value'
  | 'pl'
  | 'plPct';

/** One row of the combined ICICI holdings table (display strings + numeric sort keys). */
export interface IciciCombinedRow {
  type: 'Equity' | 'MF';
  account: string;
  name: string;
  qty: string;
  avg: string;
  price: string;
  invested: string;
  value: string;
  pl: string;
  plPct: string;
  plKind: string;
  mfId: number | null;
  qtyNum: number | null;
  avgNum: number | null;
  priceNum: number | null;
  investedNum: number | null;
  valueNum: number | null;
  plNum: number | null;
  plPctNum: number | null;
}

/** Sortable column keys for ICICI portfolio table (UI). */
export type BreezePortfolioSortCol =
  | 'account'
  | 'stockCode'
  | 'qty'
  | 'avgPrice'
  | 'bookedPnl'
  | 'currentPrice'
  | 'invested'
  | 'current'
  | 'pl'
  | 'plPct';

/** One row for ICICI GetPortfolioHoldings table (derived from API + computed columns). */
export interface BreezePortfolioHoldingsDisplayRow {
  stockCode: string;
  quantity: string;
  avgPrice: string;
  bookedPnl: string;
  currentPrice: string;
  investedAmt: string;
  currentAmt: string;
  /** current − invested (display). */
  pl: string;
  /** % on invested cost. */
  plPct: string;
  plKind: 'pos' | 'neg' | 'zero' | 'na';
  /** Raw numbers for sort / totals (null when not applicable). */
  qtyNum: number | null;
  avgNum: number | null;
  bookedPnlNum: number | null;
  investedNum: number | null;
  currentNum: number | null;
  plNum: number | null;
  plPctNum: number | null;
  /** Current market price per share (for sorting Current mkt column). */
  curPxNum: number | null;
  /** Which ICICI account this row came from ("1" or "2"). */
  account: string;
}

/** Footer totals over filtered rows. */
export interface BreezePortfolioHoldingsTotals {
  invested: string;
  current: string;
  pl: string;
  plPct: string;
  plKind: 'pos' | 'neg' | 'zero' | 'na';
}

export interface NsuEsppStats {
  qty: number;
  profitAfterTax: number;
  avgBuyPrice: number;
  avgProfitPercent: number;
}

export interface LivePriceResponse {
  livePriceUsd: number;
  usdToInrRate: number;
  lastUpdated: string;
  /** Today's market open (USD); when set, Overview and chart use it for "diff from open". */
  openPriceUsd?: number;
  /** Pre-market price (USD) from yfinance when in pre-market session (4–9:30 AM ET). */
  preMarketPriceUsd?: number;
  /** Post-market price (USD) from yfinance when in post-market session (4–8 PM ET). */
  postMarketPriceUsd?: number;
}

/** One stored point for live price history (chart + diff baseline). */
export interface LivePriceHistoryPoint {
  timestamp: number;
  livePriceUsd: number;
  usdToInrRate: number;
  /** OHLC for the aggregation bucket this point represents (server rollups). livePriceUsd = close. */
  open?: number;
  high?: number;
  low?: number;
  avg?: number;
  n?: number;
}

/** Mutual fund scheme search result (AMFI via mfapi.in). */
export interface MfSearchResult {
  schemeCode: string;
  schemeName: string;
}

/** A manually-added mutual fund holding with live NAV-derived values. */
export interface MfHolding {
  id: number;
  account: string;
  schemeCode: string;
  schemeName: string;
  units: number;
  nav: number | null;
  navDate: string;
  value: number | null;
  invested: number | null;
  pnl: number | null;
  pnlPct: number | null;
  navUnavailable: boolean;
  folio: string;
}

export interface MfHoldingsResponse {
  holdings: MfHolding[];
  totals: {
    value: number;
    invested: number | null;
    pnl: number | null;
    pnlPct: number | null;
  };
}

/** One NVIDIA news article with computed sentiment. */
export interface NewsArticle {
  headline: string;
  summary: string;
  source: string;
  url: string;
  image: string;
  category: string;
  datetime: number; // ms
  sentiment: 'positive' | 'negative' | 'neutral';
  sentimentScore: number; // VADER compound, -1..1
}

export interface NewsSummary {
  count: number;
  positive: number;
  negative: number;
  neutral: number;
  avgScore: number;
  score100: number;
  overall: 'positive' | 'negative' | 'neutral';
  analyzerAvailable: boolean;
}

export interface NewsResponse {
  articles: NewsArticle[];
  summary: NewsSummary;
  fetchedAt: number;
  rangeDays: number;
}

/** One row in the live-price “by day” ticker (grouped by US Eastern calendar day). */
export interface LivePriceDayTickerItem {
  dateKey: string;
  /** Short label in America/New_York (e.g. Fri, Mar 20) */
  label: string;
  open: number;
  close: number;
  high: number;
  low: number;
  diffUsd: number;
  diffPct: number;
  samples: number;
}

export interface MarketStatusResponse {
  marketOpen: boolean;
  /** True when 4:00–9:30 AM ET (Mon–Fri) */
  isPreMarketSession?: boolean;
  /** True when 4:00–8:00 PM ET (Mon–Fri) */
  isPostMarketSession?: boolean;
  /** Next 9:30 AM ET (Mon–Fri) in Unix ms */
  nextOpen?: number;
  /** Next 4:00 PM ET (Mon–Fri) in Unix ms */
  nextClose?: number;
  /** Next 4:00 AM ET (Mon–Fri) pre-market start in Unix ms */
  nextPreMarketStart?: number;
}

export interface DashboardResponse {
  livePriceUsd: number;
  usdToInrRate: number;
  /** Today's market open (USD); when set, Overview and chart use it for "diff from open". */
  openPriceUsd?: number;
  /** Pre-market price (USD) from yfinance when in pre-market session. */
  preMarketPriceUsd?: number;
  /** Post-market price (USD) from yfinance when in post-market session. */
  postMarketPriceUsd?: number;
  totalShares: number;
  totalValueUsd: number;
  totalValueInr: number;
  unrealisedProfitAfterTax: number;
  unrealisedProfitBeforeTax: number;
  totalTaxToPay: number;
  /** Previous day's closing price (USD) from Finnhub quote `pc` field */
  previousCloseUsd?: number;
  /** Total portfolio value at previous close price in USD (shares × prevClose) */
  previousCloseValueUsd?: number;
  /** Total portfolio value at previous close price in INR (shares × prevClose × prevRate) */
  previousCloseValueInr?: number;
  /** USD→INR rate from the previous day (last stored rate before today midnight) */
  previousCloseUsdToInrRate?: number;
  /** Proceeds after tax if you sell everything now (total value − tax) = amount in bank */
  netInBankIfSellNow: number;
  realisedProfit: number | null;
  /** Total quantity of shares sold (from SellOut); only when realisedProfit is set */
  soldTotalQty?: number | null;
  /** Total sell value in INR (from SellOut); only when realisedProfit is set */
  soldTotalValueInr?: number | null;
  /** Sell value from RSU/NSU lots (INR); for pie chart */
  soldValueRsuInr?: number | null;
  /** Sell value from ESPP lots (INR); for pie chart */
  soldValueEsppInr?: number | null;
  nsu: NsuEsppStats | null;
  espp: NsuEsppStats | null;
  /** True if the last "mark as sold" can be undone */
  canUndoMarkSold?: boolean;
}

export interface HoldingRow {
  type: string;
  buyDate: string;
  qty: number;
  buyPriceUsd: number;
  /** ESPP only: actual buy price (USD); buyPriceUsd is TDS price for ESPP */
  priceBoughtUsd?: number;
  totalPurchaseInr: number;
  /** If you sell this lot today: proceeds in hand (value today − tax) */
  netIfSellTodayInr: number;
  profitPercent: number;
  taxToPayInr: number;
  taxPercent: number;
}

/** Resolved annual payout per unit for one instrument (dividend / REIT-InvIT distribution). */
export interface IncomePayout {
  key: string;
  symbol: string;
  /** Company name from ICICI's SecurityMaster (e.g. "Embassy Office Parks Reit"). */
  name?: string;
  /** Payout per unit over the trailing 12 months; null when it could not be resolved. */
  annualPayout: number | null;
  currency: string;
  source: 'manual' | 'yahoo' | 'unresolved';
}

export interface IncomeResolveResponse {
  payouts: IncomePayout[];
}

/** How quickly a net-worth holding can be converted to cash. */
export type NetworthLiquidity = 'liquid' | 'illiquid';

/** Whether a net-worth row adds to (asset) or subtracts from (liability) the total. */
export type NetworthKind = 'asset' | 'liability';

/** A manually-entered net-worth line item (things the app can't track automatically). */
export interface NetworthItem {
  id: number;
  label: string;
  category: string;
  liquidity: NetworthLiquidity;
  kind: NetworthKind;
  valueInr: number;
  note: string;
  updatedAt: string;
}

export interface NetworthItemsResponse {
  items: NetworthItem[];
}

/** A single lot's share reservation ("earmark") for a planned sale at a target price. */
export interface Earmark {
  id: number;
  batchId: string;
  /** Frontend lot key: "buyDate|type|price|qty|total". */
  lotKey: string;
  qty: number;
  priceUsd: number;
  label: string;
  createdAt: string;
}

export interface EarmarksResponse {
  earmarks: Earmark[];
}

export interface SoldRow {
  sellDate: string;
  buyDate: string;
  qtySold: number;
  type: string;
  priceBoughtUsd: number;
  priceSellUsd: number;
  buyValueInr: number;
  sellValueInr: number;
  gainBeforeTaxInr: number;
  taxPaidInr: number;
  profitPercent: number;
  taxPercent: number;
}
