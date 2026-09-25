import { Injectable, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of, tap } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

export interface AuthSession {
  authenticated: boolean;
  username: string | null;
  /** False before the very first password has been chosen. */
  passwordSet: boolean;
  /** Whether this browser may run first-time setup (local network only). */
  canSetUpHere: boolean;
}

const SIGNED_OUT: AuthSession = { authenticated: false, username: null, passwordSet: true, canSetUpHere: false };

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly apiUrl = '/api/auth';

  /** Last known session, for the header to show who is signed in without re-fetching. */
  readonly session = signal<AuthSession>(SIGNED_OUT);

  constructor(private http: HttpClient) {}

  /** The session cookie is HttpOnly, so only the server can answer this. */
  refresh(): Observable<AuthSession> {
    return this.http.get<AuthSession>(`${this.apiUrl}/session`).pipe(
      // A backend that is down should land on the login page, not a blank dashboard.
      catchError(() => of(SIGNED_OUT)),
      tap((s) => this.session.set(s)),
    );
  }

  login(username: string, password: string): Observable<void> {
    return this.http
      .post<{ username: string }>(`${this.apiUrl}/login`, { username, password })
      .pipe(map(() => void 0));
  }

  setUp(username: string, password: string): Observable<void> {
    return this.http
      .post<{ username: string }>(`${this.apiUrl}/setup`, { username, password })
      .pipe(map(() => void 0));
  }

  logout(): Observable<void> {
    return this.http.post<{ ok: boolean }>(`${this.apiUrl}/logout`, {}).pipe(
      tap(() => this.session.set(SIGNED_OUT)),
      map(() => void 0),
    );
  }
}
