import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { DashboardResponse, HoldingRow, SoldRow } from '../models/dashboard.types';

@Injectable({ providedIn: 'root' })
export class DashboardService {
  private readonly apiUrl = '/api';

  constructor(private http: HttpClient) {}

  getDashboardData(refresh = false): Observable<DashboardResponse> {
    const url = refresh ? `${this.apiUrl}/dashboard?refresh=1` : `${this.apiUrl}/dashboard`;
    return this.http.get<DashboardResponse>(url);
  }

  getHoldings(): Observable<HoldingRow[]> {
    return this.http.get<HoldingRow[]>(`${this.apiUrl}/holdings`);
  }

  getSold(): Observable<SoldRow[]> {
    return this.http.get<SoldRow[]>(`${this.apiUrl}/sold`);
  }
}
