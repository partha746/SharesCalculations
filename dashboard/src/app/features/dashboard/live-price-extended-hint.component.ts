import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnDestroy, OnInit } from '@angular/core';

import { ExtendedSessionStats } from '../../core/models/dashboard.types';
import { formatCountdownWithSeconds } from './countdown-utils';

/**
 * Extended-hours row in the live price card: the pre/post session's open, high, low, last
 * and volume when we have them, otherwise a countdown to the next session.
 *
 * Owns a 1s tick so the main dashboard is not change-detected every second.
 */
@Component({
    selector: 'app-live-price-extended-hint',
    imports: [],
    templateUrl: './live-price-extended-hint.component.html',
    styleUrl: './live-price-extended-hint.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class LivePriceExtendedHintComponent implements OnInit, OnDestroy {
  @Input() marketOpen = false;
  @Input() isPreMarketSession = false;
  @Input() isPostMarketSession = false;
  @Input() marketNextPreMarketStartMs: number | null = null;
  @Input() preSession: ExtendedSessionStats | null = null;
  @Input() postSession: ExtendedSessionStats | null = null;
  /** Last price from /api/live-price. Early in a session Nasdaq's quote has a price before
   * its extended-trading table populates, so this keeps the card from looking empty. */
  @Input() fallbackPreUsd: number | null = null;
  @Input() fallbackPostUsd: number | null = null;

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

  /**
   * Which session to show: the one we are in, else whichever has data. Nasdaq keeps a
   * session's numbers readable until shortly before it next opens, so overnight this
   * surfaces the evening's post-market rather than an empty card.
   */
  get session(): ExtendedSessionStats | null {
    if (this.isPreMarketSession) {
      return this.preSession ?? this.priceOnly('pre', this.fallbackPreUsd);
    }
    if (this.isPostMarketSession) {
      return this.postSession ?? this.priceOnly('post', this.fallbackPostUsd);
    }
    // Outside both sessions, show whichever ran most recently. Nasdaq keeps each table
    // readable until shortly before it next opens, so during Monday's regular hours the
    // post table still holds Friday's numbers while pre holds this morning's — preferring
    // post unconditionally would hide the fresher session.
    const pre = this.preSession;
    const post = this.postSession;
    if (pre && post) {
      return (post.asOfMs ?? 0) >= (pre.asOfMs ?? 0) ? post : pre;
    }
    return pre ?? post ?? null;
  }

  /** Date of the session being shown, when it is not today's. */
  get staleDateLabel(): string | null {
    const s = this.session;
    if (!s?.asOfDate || !s.asOfMs) return null;
    const shown = new Date(s.asOfMs);
    const today = new Date();
    const sameDay =
      shown.getUTCFullYear() === today.getUTCFullYear() &&
      shown.getUTCMonth() === today.getUTCMonth() &&
      shown.getUTCDate() === today.getUTCDate();
    return sameDay ? null : s.asOfDate;
  }

  /** Minimal stats carrying just a last price, for the gap before the session table fills. */
  private priceOnly(session: 'pre' | 'post', last: number | null): ExtendedSessionStats | null {
    if (last == null) return null;
    return {
      session, open: null, openAtMs: null, openIsRecorded: false, recordedTicks: 0,
      last, change: null, changePct: null, high: null, highAt: null,
      low: null, lowAt: null, volume: null, prevClose: null,
      asOf: null, asOfMs: null, asOfDate: null,
    };
  }

  get hasSessionData(): boolean {
    const s = this.session;
    return !!s && (s.last != null || s.open != null);
  }

  /** True when showing a finished session rather than one in progress. */
  get isHistoric(): boolean {
    const s = this.session;
    if (!s) return false;
    return !(s.session === 'pre' ? this.isPreMarketSession : this.isPostMarketSession);
  }

  get sessionTitle(): string {
    const s = this.session;
    if (!s) return this.liveCardExtendedHoursClosedTitle;
    const name = s.session === 'pre' ? 'Pre-market' : 'Post-market';
    if (!this.isHistoric) return name;
    // Date it when it is not today's session, so Friday's numbers on a Monday are obvious.
    const stale = this.staleDateLabel;
    return stale ? `${name} · ${stale}` : `${name} (closed)`;
  }

  get changeClass(): string {
    const c = this.session?.change;
    if (c == null || c === 0) return '';
    return c > 0 ? 'is-up' : 'is-down';
  }

  usd(v: number | null | undefined): string {
    if (v == null || !Number.isFinite(v)) return '—';
    return '$' + v.toFixed(2);
  }

  signed(v: number | null | undefined, digits = 2): string {
    if (v == null || !Number.isFinite(v)) return '—';
    return (v > 0 ? '+' : '') + v.toFixed(digits);
  }

  /** 6,553,852 -> "6.55M": the card has no room for full grouping. */
  compactVolume(v: number | null | undefined): string {
    if (v == null || !Number.isFinite(v) || v <= 0) return '—';
    if (v >= 1_000_000_000) return (v / 1_000_000_000).toFixed(2) + 'B';
    if (v >= 1_000_000) return (v / 1_000_000).toFixed(2) + 'M';
    if (v >= 1_000) return (v / 1_000).toFixed(1) + 'K';
    return String(v);
  }

  /** Tooltip explaining that the open is ours, not the exchange's. */
  get openTitle(): string {
    const s = this.session;
    if (!s) return '';
    if (s.open == null) {
      return 'No open recorded: this dashboard captures the session open itself, and it was not running when the session started.';
    }
    const at = s.openAtMs ? new Date(s.openAtMs).toLocaleTimeString() : '';
    return `First price recorded by this dashboard${at ? ' at ' + at : ''} (${s.recordedTicks} ticks). ` +
      'Nasdaq does not publish an official extended-hours open, so this can trail the first print by up to one poll.';
  }

  get volumeTitle(): string {
    const s = this.session;
    if (!s || s.volume == null) return '';
    return s.volume.toLocaleString('en-US') + ' shares traded in this session';
  }

  get changeTitle(): string {
    const s = this.session;
    if (!s || s.prevClose == null) return '';
    const which = s.session === 'post' ? "the day's regular close" : 'the previous regular close';
    return `Change vs ${which}, ${this.usd(s.prevClose)}`;
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
