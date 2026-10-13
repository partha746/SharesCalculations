import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnChanges, SimpleChanges } from '@angular/core';

import { DashboardService } from '../../../core/services/dashboard.service';
import { MfGainsYear, MfStatementSummary } from '../../../core/models/dashboard.types';

const STATEMENT_ISSUERS: Record<string, string> = { CAMS: 'CAMS', KFINTECH: 'KFintech' };

/** Realised capital gains per financial year, worked out from each account's detailed CAS statement.
 * Hidden until a statement has been read. */
@Component({
  selector: 'app-mf-gains',
  templateUrl: './mf-gains.component.html',
  styleUrls: ['../mf-tab/mf-tab.component.scss', './mf-gains.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class MfGainsComponent implements OnChanges {
  /** Account id -> display name. */
  @Input() accountNames: Record<string, string> = {};
  /** Bumped by the parent after a statement is read, to reload. */
  @Input() version = 0;

  statements: MfStatementSummary[] = [];
  error: string | null = null;
  selectedAccount = '';
  selectedFy = '';
  forgetting = false;

  private readonly moneyFmt = new Intl.NumberFormat('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  constructor(private dashboardService: DashboardService, private cdr: ChangeDetectorRef) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['version']) this.load();
  }

  load(): void {
    this.dashboardService.getMfStatements().subscribe({
      next: (res) => {
        this.statements = res.statements ?? [];
        if (!this.statements.some((s) => s.account === this.selectedAccount)) {
          this.selectedAccount = this.statements[0]?.account ?? '';
        }
        this.keepFyValid();
        this.error = null;
        this.cdr.markForCheck();
      },
      error: (err) => {
        this.error = err?.error?.error || err?.message || 'Could not load capital gains';
        this.cdr.markForCheck();
      },
    });
  }

  get current(): MfStatementSummary | null {
    return this.statements.find((s) => s.account === this.selectedAccount) ?? null;
  }

  get year(): MfGainsYear | null {
    return this.current?.gains.find((y) => y.fy === this.selectedFy) ?? null;
  }

  pickAccount(id: string): void {
    this.selectedAccount = id;
    this.keepFyValid();
  }

  pickFy(fy: string): void {
    this.selectedFy = fy;
  }

  /** Stays on the chosen year when the account has it, else shows the newest. */
  private keepFyValid(): void {
    const gains = this.current?.gains ?? [];
    if (!gains.some((y) => y.fy === this.selectedFy)) this.selectedFy = gains[0]?.fy ?? '';
  }

  forget(): void {
    const st = this.current;
    if (!st) return;
    if (!confirm(`Forget the statement for ${this.acctLabel(st.account)}? Its capital gains won't be shown until you read one again. Folio numbers stay.`)) return;
    this.forgetting = true;
    this.cdr.markForCheck();
    this.dashboardService.forgetMfStatement(st.account).subscribe({
      next: () => { this.forgetting = false; this.load(); },
      error: (err) => {
        this.forgetting = false;
        this.error = err?.error?.error || err?.message || 'Could not forget the statement';
        this.cdr.markForCheck();
      },
    });
  }

  acctLabel(id: string): string {
    return this.accountNames[id] || `Account ${id}`;
  }

  issuerLabel(st: MfStatementSummary): string {
    return STATEMENT_ISSUERS[st.issuer] ?? st.issuer;
  }

  readOn(st: MfStatementSummary): string {
    return new Date(st.importedAt * 1000).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', year: 'numeric' });
  }

  fyLabel(fy: string): string {
    return fy.replace(/^FY/, 'FY ');
  }

  money(n: number): string {
    return '\u20B9 ' + this.moneyFmt.format(n);
  }

  kind(n: number): string {
    return Math.abs(n) < 0.005 ? 'mf-muted' : n > 0 ? 'mf-pos' : 'mf-neg';
  }
}
