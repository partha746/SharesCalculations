import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnDestroy, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { formatCountdown, formatCountdownWithSeconds } from './countdown-utils';

@Component({
    selector: 'app-overview-time-card',
    imports: [CommonModule],
    templateUrl: './overview-time-card.component.html',
    styleUrl: './overview-time-card.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewTimeCardComponent implements OnInit, OnDestroy {
  @Input() marketOpen = false;
  @Input() isPreMarketSession = false;
  @Input() isPostMarketSession = false;
  @Input() marketNextOpenMs: number | null = null;
  @Input() marketNextCloseMs: number | null = null;
  @Input() marketNextPreMarketStartMs: number | null = null;
  @Input() lastRefreshedAt: Date | null = null;

  currentTime = new Date();
  private intervalId: ReturnType<typeof setInterval> | null = null;

  constructor(private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.intervalId = setInterval(() => {
      this.currentTime = new Date();
      this.cdr.markForCheck();
    }, 1000);
  }

  ngOnDestroy(): void {
    if (this.intervalId != null) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  formatLastRefreshed(d: Date | null): string {
    if (!d) return '';
    return d.toLocaleString('en-IN', { dateStyle: 'medium', timeStyle: 'short' });
  }

  formatClock(d: Date): string {
    return d.toLocaleTimeString('en-IN', { hour12: true, hour: 'numeric', minute: '2-digit', second: '2-digit' });
  }

  get marketCountdownText(): string {
    const now = Date.now();
    if (this.marketOpen && this.marketNextCloseMs != null) {
      const rem = this.marketNextCloseMs - now;
      if (rem <= 0) return '';
      return 'Closes in ' + formatCountdown(rem);
    }
    if (!this.marketOpen && this.marketNextOpenMs != null) {
      const rem = this.marketNextOpenMs - now;
      if (rem <= 0) return '';
      return 'Opens in ' + formatCountdown(rem);
    }
    return '';
  }

  get preMarketCountdownText(): string {
    if (this.isPreMarketSession || this.isPostMarketSession || this.marketOpen) return '';
    if (this.marketNextPreMarketStartMs == null) return '';
    const rem = this.marketNextPreMarketStartMs - Date.now();
    if (rem <= 0) return '';
    return 'Pre-market starts in ' + formatCountdownWithSeconds(rem);
  }

  get extendedHoursTimeCardLabel(): string {
    if (this.isPostMarketSession || (!this.isPreMarketSession && !this.marketOpen)) return 'Post-market';
    return 'Pre-market';
  }

  get extendedHoursTimeCardValue(): string {
    if (this.isPreMarketSession) return 'Open';
    if (this.isPostMarketSession) return 'Open';
    if (this.marketOpen) return 'Closed';
    return 'Closed';
  }

  get showPreMarketStartsCountdown(): boolean {
    return !this.isPreMarketSession && !this.isPostMarketSession && !this.marketOpen && this.preMarketCountdownText.length > 0;
  }
}
