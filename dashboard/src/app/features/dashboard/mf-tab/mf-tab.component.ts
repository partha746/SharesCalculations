import { ChangeDetectionStrategy, ChangeDetectorRef, Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { DashboardService } from '../../../core/services/dashboard.service';
import { MfSearchResult } from '../../../core/models/dashboard.types';

/** MF management panel: add a fund (AMFI search) or import an ICICI CSV. Emits `changed` after writes
 * so the parent can reload the combined holdings table. */
@Component({
    selector: 'app-mf-tab',
    imports: [CommonModule, FormsModule],
    templateUrl: './mf-tab.component.html',
    styleUrl: './mf-tab.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class MfTabComponent {
  @Output() changed = new EventEmitter<void>();
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
  importResult: { imported: any[]; skipped: any[] } | null = null;
  importError: string | null = null;

  constructor(private dashboardService: DashboardService, private cdr: ChangeDetectorRef) {}

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
    this.importing = true;
    this.importError = null;
    this.importResult = null;
    this.cdr.markForCheck();
    const reader = new FileReader();
    reader.onload = () => {
      const csv = String(reader.result || '');
      this.dashboardService.importMfCsv(this.addAccount, csv).subscribe({
        next: (res) => { this.importResult = res; this.importing = false; this.cdr.markForCheck(); this.changed.emit(); },
        error: (err) => { this.importError = err?.error?.error || err?.message || 'Import failed'; this.importing = false; this.cdr.markForCheck(); },
      });
    };
    reader.onerror = () => { this.importError = 'Could not read file'; this.importing = false; this.cdr.markForCheck(); };
    reader.readAsText(file);
    input.value = '';
  }
}
