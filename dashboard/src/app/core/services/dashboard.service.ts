import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpParams, HttpResponse } from '@angular/common/http';
import { Observable } from 'rxjs';
import {
  BreezeAccountStatus,
  BreezeStatusAllResponse,
  BreezeStatusResponse,
  DashboardResponse,
  Earmark,
  EarmarksResponse,
  HoldingRow,
  LivePriceHistoryPoint,
  LivePriceResponse,
  MarketStatusResponse,
  MfHoldingsResponse,
  MfSearchResult,
  NewsResponse,
  SoldRow,
} from '../models/dashboard.types';

@Injectable({ providedIn: 'root' })
export class DashboardService {
  private readonly apiUrl = '/api';

  constructor(private http: HttpClient) {}

  getDashboardData(refresh = false): Observable<DashboardResponse> {
    const url = refresh ? `${this.apiUrl}/dashboard?refresh=1` : `${this.apiUrl}/dashboard`;
    return this.http.get<DashboardResponse>(url);
  }

  getLivePrice(): Observable<LivePriceResponse> {
    // Cache-bust + explicit no-cache headers so USD→INR always reflects the latest poll.
    return this.http.get<LivePriceResponse>(`${this.apiUrl}/live-price`, {
      params: { _: String(Date.now()) },
      headers: new HttpHeaders({
        'Cache-Control': 'no-cache',
        Pragma: 'no-cache',
      }),
    });
  }

  getMarketStatus(): Observable<MarketStatusResponse> {
    return this.http.get<MarketStatusResponse>(`${this.apiUrl}/market-status`);
  }

  /** Latest NVIDIA news + sentiment. */
  getNews(days = 7): Observable<NewsResponse> {
    return this.http.get<NewsResponse>(`${this.apiUrl}/news?days=${days}&t=${Date.now()}`);
  }

  /** Mutual funds: scheme search (AMFI), holdings list (live NAV), add, delete. */
  mfSearch(q: string): Observable<MfSearchResult[]> {
    return this.http.get<MfSearchResult[]>(`${this.apiUrl}/mf/search?q=${encodeURIComponent(q)}`);
  }
  getMfHoldings(account: string = 'all'): Observable<MfHoldingsResponse> {
    const acct = account && account !== 'all' ? `&account=${encodeURIComponent(account)}` : '';
    return this.http.get<MfHoldingsResponse>(`${this.apiUrl}/mf/holdings?t=${Date.now()}${acct}`);
  }
  addMfHolding(body: { schemeCode: string; schemeName: string; units: number; invested?: number | null; folio?: string; account?: string }): Observable<{ id: number }> {
    return this.http.post<{ id: number }>(`${this.apiUrl}/mf/holdings`, body);
  }
  deleteMfHolding(id: number): Observable<{ ok: boolean }> {
    return this.http.delete<{ ok: boolean }>(`${this.apiUrl}/mf/holdings/${id}`);
  }
  importMfCsv(account: string, csv: string): Observable<{ imported: any[]; skipped: any[] }> {
    return this.http.post<{ imported: any[]; skipped: any[] }>(`${this.apiUrl}/mf/import`, { account, csv });
  }

  /** Server-aggregated OHLC live price history for the chart (bucketed to <= maxPoints). */
  getLivePriceHistory(days = 7, maxPoints = 1500): Observable<LivePriceHistoryPoint[]> {
    return this.http.get<LivePriceHistoryPoint[]>(`${this.apiUrl}/live-price-history?days=${days}&maxPoints=${maxPoints}`);
  }

  /** Persist one poll point to DB (fire-and-forget from component). */
  appendLivePriceHistory(point: LivePriceHistoryPoint): Observable<unknown> {
    return this.http.post(`${this.apiUrl}/live-price-history`, point);
  }

  /** Clear all stored live price history (graph data). */
  clearLivePriceHistory(): Observable<{ ok: boolean; deleted: number }> {
    return this.http.delete<{ ok: boolean; deleted: number }>(`${this.apiUrl}/live-price-history`);
  }

  getHoldings(): Observable<HoldingRow[]> {
    return this.http.get<HoldingRow[]>(`${this.apiUrl}/holdings`);
  }

  getSold(): Observable<SoldRow[]> {
    return this.http.get<SoldRow[]>(`${this.apiUrl}/sold`);
  }

  markSold(payload: {
    sellDate: string;
    priceSellUsd: number;
    items: Array<{
      type: string;
      buyDate: string;
      buyPriceUsd: number;
      priceBoughtUsd?: number;
      qtyToSell: number;
    }>;
  }): Observable<{ success: boolean; inserted?: number }> {
    return this.http.post<{ success: boolean; inserted?: number }>(`${this.apiUrl}/mark-sold`, payload);
  }

  /** Reverses the last "mark as sold" batch. Returns 404 when nothing to undo. */
  undoMarkSold(): Observable<{ success: boolean; undone?: number }> {
    return this.http.post<{ success: boolean; undone?: number }>(`${this.apiUrl}/mark-sold-undo`, {});
  }

  /** Check whether the last mark-as-sold can be undone. Cache-bust so we never get a stale value. */
  getMarkSoldCanUndo(): Observable<{ canUndo: boolean }> {
    return this.http.get<{ canUndo: boolean }>(`${this.apiUrl}/mark-sold-can-undo?t=${Date.now()}`);
  }

  getTaxConfig(): Observable<any> {
    return this.http.get<any>(`${this.apiUrl}/tax-config?t=${Date.now()}`);
  }

  putTaxConfig(body: any): Observable<{ success: boolean }> {
    return this.http.put<{ success: boolean }>(`${this.apiUrl}/tax-config`, body);
  }

  generateTaxDoc(fy?: number): Observable<{ fyLabel: string; rows: any[] }> {
    const params = fy ? `fy=${fy}&t=${Date.now()}` : `t=${Date.now()}`;
    return this.http.get<{ fyLabel: string; rows: any[] }>(`${this.apiUrl}/generate-tax-doc?${params}`);
  }

  /** Download the ClearTax Schedule FA template with the FA-A3 sheet filled from holdings (xlsx blob).
   * When `selectedKeys` is provided, only those lots (key = buyDate|type|qty) are exported. */
  exportFaA3(fy?: number, selectedKeys?: string[]): Observable<HttpResponse<Blob>> {
    let params = new HttpParams().set('t', String(Date.now()));
    if (fy) params = params.set('fy', String(fy));
    if (selectedKeys && selectedKeys.length > 0) params = params.set('keys', selectedKeys.join(';;'));
    return this.http.get(`${this.apiUrl}/export-fa-a3`, {
      params,
      responseType: 'blob',
      observe: 'response',
    });
  }

  /** Combined status for all accounts. */
  getBreezeStatusAll(): Observable<BreezeStatusAllResponse> {
    return this.http.get<BreezeStatusAllResponse>(`${this.apiUrl}/breeze/status?t=${Date.now()}`);
  }

  /** Status for a single account. */
  getBreezeStatus(acct: string = '1'): Observable<BreezeAccountStatus> {
    return this.http.get<BreezeAccountStatus>(`${this.apiUrl}/breeze/status/${acct}?t=${Date.now()}`);
  }

  postBreezeDisconnect(acct: string = '1'): Observable<{ success: boolean }> {
    return this.http.post<{ success: boolean }>(`${this.apiUrl}/breeze/disconnect/${acct}`, {});
  }

  // --- Financial planning: save/load the planner snapshot ---
  getFinancePlan(): Observable<{ plan: Record<string, unknown> | null; updatedAt: string | null }> {
    return this.http.get<{ plan: Record<string, unknown> | null; updatedAt: string | null }>(`${this.apiUrl}/finance-plan?t=${Date.now()}`);
  }

  saveFinancePlan(data: Record<string, unknown>): Observable<{ ok: boolean; updatedAt: string }> {
    return this.http.post<{ ok: boolean; updatedAt: string }>(`${this.apiUrl}/finance-plan`, { data });
  }

  // --- Earmarks: shares reserved for a planned sale at a target price ---
  getEarmarks(): Observable<EarmarksResponse> {
    return this.http.get<EarmarksResponse>(`${this.apiUrl}/earmarks?t=${Date.now()}`);
  }

  addEarmarks(payload: { priceUsd: number; label?: string; allocations: Array<{ lotKey: string; qty: number }> }): Observable<{ batchId: string; count: number }> {
    return this.http.post<{ batchId: string; count: number }>(`${this.apiUrl}/earmarks`, payload);
  }

  deleteEarmarkBatch(batchId: string): Observable<{ ok: boolean; deleted: number }> {
    return this.http.delete<{ ok: boolean; deleted: number }>(`${this.apiUrl}/earmarks/batch/${batchId}`);
  }

  deleteEarmark(id: number): Observable<{ ok: boolean }> {
    return this.http.delete<{ ok: boolean }>(`${this.apiUrl}/earmarks/${id}`);
  }

  /** Add a custom Breeze account (stored in the DB). */
  addBreezeAccount(payload: { label?: string; apiKey: string; apiSecret: string }): Observable<{ success: boolean; id: string }> {
    return this.http.post<{ success: boolean; id: string }>(`${this.apiUrl}/breeze/accounts`, payload);
  }

  /** Remove a custom Breeze account. */
  deleteBreezeAccount(acct: string): Observable<{ success: boolean }> {
    return this.http.delete<{ success: boolean }>(`${this.apiUrl}/breeze/accounts/${acct}`);
  }

  /** Mutual-fund unit holdings for a Breeze account (legacy ICICI /mf endpoint). */
  getBreezeMfHoldings(acct: string = '1'): Observable<unknown> {
    return this.http.get(`${this.apiUrl}/breeze/mf-holdings/${acct}?t=${Date.now()}`);
  }

  getBreezePortfolioHoldings(options: {
    exchangeCode: string;
    fromDate?: string;
    toDate?: string;
    stockCode?: string;
    portfolioType?: string;
    acct?: string;
  }): Observable<unknown> {
    const acct = options.acct || '1';
    let params = new HttpParams().set('exchange_code', options.exchangeCode.trim());
    if (options.fromDate?.trim()) params = params.set('from_date', options.fromDate.trim());
    if (options.toDate?.trim()) params = params.set('to_date', options.toDate.trim());
    if (options.stockCode?.trim()) params = params.set('stock_code', options.stockCode.trim());
    if (options.portfolioType?.trim()) params = params.set('portfolio_type', options.portfolioType.trim());
    return this.http.get(`${this.apiUrl}/breeze/portfolio-holdings/${acct}`, { params });
  }
}
