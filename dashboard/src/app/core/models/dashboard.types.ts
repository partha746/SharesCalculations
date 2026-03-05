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
}

/** One stored point for live price history (chart + diff baseline). */
export interface LivePriceHistoryPoint {
  timestamp: number;
  livePriceUsd: number;
  usdToInrRate: number;
}

export interface MarketStatusResponse {
  marketOpen: boolean;
  nextOpen?: string;
  nextClose?: string;
}

export interface DashboardResponse {
  livePriceUsd: number;
  usdToInrRate: number;
  totalShares: number;
  totalValueUsd: number;
  totalValueInr: number;
  unrealisedProfitAfterTax: number;
  unrealisedProfitBeforeTax: number;
  totalTaxToPay: number;
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
