import { ChangeDetectionStrategy, ChangeDetectorRef, Component, EventEmitter, Input, Output } from '@angular/core';

import { FormsModule } from '@angular/forms';
import { DashboardService } from '../../../core/services/dashboard.service';
import { MfCsvImportResult, MfSearchResult, MfStatementImportResult } from '../../../core/models/dashboard.types';

const STATEMENT_ISSUERS: Record<string, string> = { CAMS: 'CAMS', KFINTECH: 'KFintech', NSDL: 'NSDL', CDSL: 'CDSL' };

/** MF management panel: add a fund (AMFI search), import an ICICI CSV, or read a CAS statement.
 * Emits `changed` after holdings writes so the parent can reload the combined holdings table, and
 * `statementChanged` after a statement is read so the capital gains panel reloads. */
@Component({
    selector: 'app-mf-tab',
    imports: [FormsModule],
    templateUrl: './mf-tab.component.html',
    styleUrl: './mf-tab.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class MfTabComponent {
  @Output() changed = new EventEmitter<void>();
  @Output() statementChanged = new EventEmitter<void>();
  /** Account id -> display name (holder name when connected). */
  @Input() accountNames: Record<string, string> = {};

  /** Default account for adds/imports. */
  addAccount = '1';

  /** All account ids to offer in the selector (numeric order). */
  get accountIds(): string[] {
    return Object.keys(this.accountNames).sort((a, b) => Number(a) - Number(b));
  }

  acctLabel(id: string): string {
    return this.accountNames[id] || `Account ${id}`;
  }

  // Add form
  searchQuery = '';
  searchResults: MfSearchResult[] = [];
  searching = false;
  selected: MfSearchResult | null = null;
  units: number | null = null;
  invested: number | null = null;
  adding = false;
  addError: string | null = null;
  private searchDebounce: ReturnType<typeof setTimeout> | null = null;

  // CSV import
  importing = false;
  importResult: MfCsvImportResult | null = null;
  /** Account the last CSV went into (the selector may have moved since). */
  importAccount = '1';
  importError: string | null = null;

  // CAS statement
  readonly camsUrl = 'https://www.camsonline.com/Investors/Statements/Consolidated-Account-Statement';
  casFile: File | null = null;
  casPassword = '';
  casReading = false;
  casResult: MfStatementImportResult | null = null;
  casAccount = '1';
  casError: string | null = null;

  constructor(private dashboardService: DashboardService, private cdr: ChangeDetectorRef) {}

  importCount(action: 'added' | 'updated' | 'combined'): number {
    return this.importResult?.imported.filter((x) => x.action === action).length ?? 0;
  }

  statementLabel(r: MfStatementImportResult): string {
    const kind = r.statementType === 'DETAILED' ? 'detailed' : r.statementType === 'SUMMARY' ? 'summary' : 'demat';
    let label = `${STATEMENT_ISSUERS[r.issuer] ?? r.issuer} ${kind} statement`;
    if (r.investorName) label += ` for ${r.investorName}`;
    if (r.periodFrom) label += ` (${r.periodFrom} to ${r.periodTo})`;
    return label;
  }

  onSearchInput(): void {
    this.selected = null;
    const q = this.searchQuery.trim();
    if (this.searchDebounce) clearTimeout(this.searchDebounce);
    if (q.length < 2) {
      this.searchResults = [];
      this.cdr.markForCheck();
      return;
    }
    this.searchDebounce = setTimeout(() => {
      this.searching = true;
      this.cdr.markForCheck();
      this.dashboardService.mfSearch(q).subscribe({
        next: (res) => { this.searchResults = res ?? []; this.searching = false; this.cdr.markForCheck(); },
        error: () => { this.searchResults = []; this.searching = false; this.cdr.markForCheck(); },
      });
    }, 350);
  }

  pickScheme(s: MfSearchResult): void {
    this.selected = s;
    this.searchQuery = s.schemeName;
    this.searchResults = [];
    this.cdr.markForCheck();
  }

  addHolding(): void {
    this.addError = null;
    if (!this.selected) { this.addError = 'Search and pick a scheme first.'; return; }
    if (!this.units || this.units <= 0) { this.addError = 'Enter the number of units.'; return; }
    this.adding = true;
    this.cdr.markForCheck();
    this.dashboardService.addMfHolding({
      schemeCode: this.selected.schemeCode,
      schemeName: this.selected.schemeName,
      units: this.units,
      invested: this.invested ?? null,
      account: this.addAccount,
    }).subscribe({
      next: () => {
        this.adding = false;
        this.selected = null;
        this.searchQuery = '';
        this.units = null;
        this.invested = null;
        this.searchResults = [];
        this.cdr.markForCheck();
        this.changed.emit();
      },
      error: (err) => {
        this.addError = err?.error?.error || err?.message || 'Failed to add';
        this.adding = false;
        this.cdr.markForCheck();
      },
    });
  }

  onImportFile(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) return;
    if (!confirm(`Make ${this.acctLabel(this.addAccount)}'s mutual funds match this CSV? Funds it doesn't list will be removed.`)) {
      input.value = '';
      return;
    }
    this.importing = true;
    this.importError = null;
    this.importResult = null;
    this.cdr.markForCheck();
    const account = this.addAccount;
    const reader = new FileReader();
    reader.onload = () => {
      const csv = String(reader.result || '');
      this.dashboardService.importMfCsv(account, csv).subscribe({
        next: (res) => {
          this.importResult = res;
          this.importAccount = account;
          this.importing = false;
          this.cdr.markForCheck();
          this.changed.emit();
        },
        error: (err) => { this.importError = err?.error?.error || err?.message || 'Import failed'; this.importing = false; this.cdr.markForCheck(); },
      });
    };
    reader.onerror = () => { this.importError = 'Could not read file'; this.importing = false; this.cdr.markForCheck(); };
    reader.readAsText(file);
    input.value = '';
  }

  onCasFile(event: Event): void {
    this.casFile = (event.target as HTMLInputElement).files?.[0] ?? null;
    this.casError = null;
    this.cdr.markForCheck();
  }

  readStatement(input: HTMLInputElement): void {
    if (!this.casFile) { this.casError = 'Choose the statement PDF.'; return; }
    const account = this.addAccount;
    this.casReading = true;
    this.casError = null;
    this.casResult = null;
    this.cdr.markForCheck();
    this.dashboardService.importMfStatement(account, this.casFile, this.casPassword).subscribe({
      next: (res) => {
        this.casResult = res;
        this.casAccount = account;
        this.casReading = false;
        this.casFile = null;
        this.casPassword = '';
        input.value = '';
        this.cdr.markForCheck();
        this.changed.emit();
        this.statementChanged.emit();
      },
      // The file stays chosen so a mistyped password can be retried.
      error: (err) => {
        this.casError = err?.error?.error || err?.message || 'Could not read the statement';
        this.casReading = false;
        this.cdr.markForCheck();
      },
    });
  }
}
