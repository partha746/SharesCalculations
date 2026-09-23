import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnDestroy, OnInit } from '@angular/core';

import { formatCountdown, formatCountdownWithSeconds } from './countdown-utils';

@Component({
    selector: 'app-overview-time-card',
    imports: [],
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
  /** Next 8:00 PM ET. Post-market starts at marketNextCloseMs and ends here. */
  @Input() marketNextPostMarketEndMs: number | null = null;
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

  /** Time only when it was today, so the strip stays on one line; full date otherwise. */
  formatRefreshedShort(d: Date | null): string {
    if (!d) return '';
    const now = new Date();
    const sameDay = d.getDate() === now.getDate()
      && d.getMonth() === now.getMonth()
      && d.getFullYear() === now.getFullYear();
    return sameDay
      ? d.toLocaleTimeString('en-IN', { hour12: true, hour: 'numeric', minute: '2-digit' })
      : d.toLocaleString('en-IN', { day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit', hour12: true });
  }

  /** One phrase for the session, rather than separate Market and Pre-market rows. */
  get marketStripLabel(): string {
    if (this.marketOpen) return 'Market open';
    if (this.isPreMarketSession) return 'Pre-market';
    if (this.isPostMarketSession) return 'Post-market';
    return 'Market closed';
  }

  formatClock(d: Date): string {
    return d.toLocaleTimeString('en-IN', { hour12: true, hour: 'numeric', minute: '2-digit', second: '2-digit' });
  }

  /** Exchange-local (New York) clock, which is what the market's own hours are quoted in. */
  formatClockEt(d: Date): string {
    return d.toLocaleTimeString('en-US', {
      hour12: true, hour: 'numeric', minute: '2-digit', second: '2-digit',
      timeZone: 'America/New_York',
    });
  }

  formatDateEt(d: Date): string {
    return d.toLocaleDateString('en-US', {
      weekday: 'short', day: 'numeric', month: 'short', timeZone: 'America/New_York',
    });
  }

  /** Live exchange zone abbreviation: EDT while daylight saving is on, EST while off. */
  get etZoneAbbr(): string {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/New_York', timeZoneName: 'short',
    }).formatToParts(this.currentTime);
    return parts.find((p) => p.type === 'timeZoneName')?.value ?? 'ET';
  }

  get etIsDst(): boolean {
    return this.etZoneAbbr === 'EDT';
  }

  get etZoneTooltip(): string {
    return this.etIsDst
      ? 'Daylight saving is on in New York (EDT, UTC−4). India does not observe it, so ET is 9h 30m behind IST.'
      : 'Daylight saving is off in New York (EST, UTC−5). India does not observe it, so ET is 10h 30m behind IST.';
  }

  /** ET is usually a day behind IST here, so the date is only worth showing when it differs. */
  get etDateDiffers(): boolean {
    const ist = this.currentTime.toLocaleDateString('en-CA');
    const et = this.currentTime.toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
    return ist !== et;
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

  /** "Pre-market ends in 2h 10m" while running, otherwise when it next starts. */
  get preMarketStatusText(): string {
    const now = Date.now();
    if (this.isPreMarketSession) {
      const rem = (this.marketNextOpenMs ?? 0) - now;
      return rem > 0 ? 'Pre-market ends in ' + formatCountdown(rem) : 'Pre-market open';
    }
    const rem = (this.marketNextPreMarketStartMs ?? 0) - now;
    return rem > 0 ? 'Pre-market in ' + formatCountdown(rem) : '';
  }

  /** Post-market starts at the regular close and runs to 8 PM ET. */
  get postMarketStatusText(): string {
    const now = Date.now();
    if (this.isPostMarketSession) {
      const rem = (this.marketNextPostMarketEndMs ?? 0) - now;
      return rem > 0 ? 'Post-market ends in ' + formatCountdown(rem) : 'Post-market open';
    }
    const rem = (this.marketNextCloseMs ?? 0) - now;
    return rem > 0 ? 'Post-market in ' + formatCountdown(rem) : '';
  }
}
