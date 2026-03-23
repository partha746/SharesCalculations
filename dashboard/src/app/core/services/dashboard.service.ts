import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { DashboardResponse, HoldingRow, LivePriceHistoryPoint, LivePriceResponse, MarketStatusResponse, SoldRow } from '../models/dashboard.types';

@Injectable({ providedIn: 'root' })
export class DashboardService {
  private readonly apiUrl = '/api';

  constructor(private http: HttpClient) {}

  getDashboardData(refresh = false): Observable<DashboardResponse> {
    const url = refresh ? `${this.apiUrl}/dashboard?refresh=1` : `${this.apiUrl}/dashboard`;
    return this.http.get<DashboardResponse>(url);
  }

  getLivePrice(): Observable<LivePriceResponse> {
    return this.http.get<LivePriceResponse>(`${this.apiUrl}/live-price`);
  }

  getMarketStatus(): Observable<MarketStatusResponse> {
    return this.http.get<MarketStatusResponse>(`${this.apiUrl}/market-status`);
  }

  /** Stored live price history from DB (for chart + diff baseline after refresh). */
  getLivePriceHistory(days = 7): Observable<LivePriceHistoryPoint[]> {
    return this.http.get<LivePriceHistoryPoint[]>(`${this.apiUrl}/live-price-history?days=${days}`);
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
}
