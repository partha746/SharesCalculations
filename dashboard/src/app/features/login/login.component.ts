import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { AuthService } from '../../core/services/auth.service';

@Component({
  selector: 'app-login',
  imports: [FormsModule],
  templateUrl: './login.component.html',
  styleUrl: './login.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class LoginComponent implements OnInit {
  /** True on first run: no password exists yet, so this browser creates the account. */
  setupMode = false;
  /** Password exists nowhere yet, but this browser is off the local network. */
  setupBlocked = false;

  username = 'psardar';
  password = '';
  confirmPassword = '';

  checking = true;
  submitting = false;
  error = '';

  readonly minPasswordLength = 8;

  constructor(
    private auth: AuthService,
    private router: Router,
    private route: ActivatedRoute,
    private cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.auth.refresh().subscribe((session) => {
      this.checking = false;
      if (session.authenticated) {
        this.router.navigateByUrl(this.nextUrl());
        return;
      }
      this.setupMode = !session.passwordSet && session.canSetUpHere;
      this.setupBlocked = !session.passwordSet && !session.canSetUpHere;
      if (session.username) this.username = session.username;
      this.cdr.markForCheck();
    });
  }

  get title(): string {
    return this.setupMode ? 'Choose a password' : 'Sign in';
  }

  get canSubmit(): boolean {
    if (this.submitting || !this.username.trim() || !this.password) return false;
    if (!this.setupMode) return true;
    return this.password.length >= this.minPasswordLength && this.password === this.confirmPassword;
  }

  submit(): void {
    if (!this.canSubmit) return;
    this.submitting = true;
    this.error = '';

    const request = this.setupMode
      ? this.auth.setUp(this.username.trim(), this.password)
      : this.auth.login(this.username.trim(), this.password);

    request.subscribe({
      next: () => this.auth.refresh().subscribe(() => this.router.navigateByUrl(this.nextUrl())),
      error: (err: HttpErrorResponse) => {
        this.submitting = false;
        this.password = '';
        this.confirmPassword = '';
        this.error = err?.error?.error || 'Could not sign in. Is the backend running?';
        this.cdr.markForCheck();
      },
    });
  }

  /** Where the guard wanted to go before it bounced us here. Rejects absolute URLs so
   *  a crafted ?next= cannot redirect off-site after a successful login. */
  private nextUrl(): string {
    const next = this.route.snapshot.queryParamMap.get('next') || '';
    return next.startsWith('/') && !next.startsWith('//') ? next : '/holdings';
  }
}
