/** ICICI Direct Breeze API status (optional integration). */
export interface BreezeStatusResponse {
  sdkInstalled: boolean;
  configured: boolean;
  connected: boolean;
  loginUrl: string | null;
  /** Register this exact URL as Redirect URL in ICICI Breeze app settings. */
  callbackUrl?: string | null;
  message?: string;
}

/** Sortable column keys for ICICI portfolio table (UI). */
export type BreezePortfolioSortCol =
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
