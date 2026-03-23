import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnDestroy, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { formatCountdownWithSeconds } from './countdown-utils';

/**
 * Pre/post “closed” row in the live price card when no extended-hours quote exists.
 * Owns a 1s tick so the main dashboard is not change-detected every second.
 */
@Component({
  selector: 'app-live-price-extended-hint',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './live-price-extended-hint.component.html',
  styleUrl: './live-price-extended-hint.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class LivePriceExtendedHintComponent implements OnInit, OnDestroy {
  @Input() marketOpen = false;
  @Input() isPreMarketSession = false;
  @Input() isPostMarketSession = false;
  @Input() marketNextPreMarketStartMs: number | null = null;

  private intervalId: ReturnType<typeof setInterval> | null = null;

  constructor(private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.intervalId = setInterval(() => this.cdr.markForCheck(), 1000);
  }

  ngOnDestroy(): void {
    if (this.intervalId != null) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  get liveCardExtendedHoursClosedTitle(): string {
    if (this.isPreMarketSession) return 'Pre-market';
    if (this.isPostMarketSession) return 'Post-market';
    return 'Pre-market closed';
  }

  get liveCardExtendedHoursClosedText(): string {
    if (this.isPreMarketSession) return 'Open';
    if (this.isPostMarketSession) return 'Open';
    if (this.marketOpen) return '— Regular session';
    if (this.marketNextPreMarketStartMs != null) {
      const rem = this.marketNextPreMarketStartMs - Date.now();
      if (rem > 0) return 'Opens in ' + formatCountdownWithSeconds(rem);
    }
    return '— Market closed';
  }
}
