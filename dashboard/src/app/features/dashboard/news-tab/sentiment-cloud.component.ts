import {
  AfterViewInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnDestroy,
  ViewChild,
} from '@angular/core';

import { NewsCloudWord } from '../../../core/models/dashboard.types';

interface PlacedWord {
  text: string;
  count: number;
  valence: number;
  sentiment: 'positive' | 'negative';
  x: number;
  y: number;
  size: number;
}

interface Box {
  x: number;
  y: number;
  w: number;
  h: number;
}

const FONT_FAMILY = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";
const MIN_FONT = 11;
const MAX_FONT = 44;
/** Gap kept between neighbouring words, in px. */
const PADDING = 3;

/**
 * Sentiment word cloud: positive terms settle toward the top, negative toward the
 * bottom, sized by how many articles used them.
 *
 * Layout is an Archimedean spiral with axis-aligned collision tests — the same idea as
 * d3-cloud, minus the dependency. Text is measured on an offscreen canvas so the boxes
 * match what the browser will actually paint.
 */
@Component({
  selector: 'app-sentiment-cloud',
  imports: [],
  templateUrl: './sentiment-cloud.component.html',
  styleUrl: './sentiment-cloud.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SentimentCloudComponent implements AfterViewInit, OnChanges, OnDestroy {
  @Input() words: NewsCloudWord[] = [];
  @Input() height = 420;

  @ViewChild('stage') stage?: ElementRef<HTMLElement>;

  placed: PlacedWord[] = [];
  /** Words that could not be fitted, reported rather than silently dropped. */
  dropped = 0;
  /** Stage height after trimming to the words actually placed. */
  renderedHeight = 0;
  /** Shifts the cloud up so the trimmed stage starts at the topmost word. */
  offsetY = 0;

  private measureCtx: CanvasRenderingContext2D | null = null;
  private resizeObserver: ResizeObserver | null = null;
  private relayoutTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(private cdr: ChangeDetectorRef) {}

  ngAfterViewInit(): void {
    this.layout();
    const host = this.stage?.nativeElement;
    if (host && typeof ResizeObserver !== 'undefined') {
      // Debounced: a drag-resize fires continuously and the layout is O(words x spiral).
      this.resizeObserver = new ResizeObserver(() => {
        if (this.relayoutTimer) clearTimeout(this.relayoutTimer);
        this.relayoutTimer = setTimeout(() => this.layout(), 120);
      });
      this.resizeObserver.observe(host);
    }
  }

  ngOnChanges(): void {
    if (this.stage) this.layout();
  }

  ngOnDestroy(): void {
    this.resizeObserver?.disconnect();
    if (this.relayoutTimer) clearTimeout(this.relayoutTimer);
  }

  get hasWords(): boolean {
    return this.placed.length > 0;
  }

  title(w: PlacedWord): string {
    const plural = w.count === 1 ? 'article' : 'articles';
    return `"${w.text}" — in ${w.count} ${plural} (VADER valence ${w.valence > 0 ? '+' : ''}${w.valence})`;
  }

  // --- Layout ---

  private layout(): void {
    const host = this.stage?.nativeElement;
    if (!host) return;
    const width = host.clientWidth || 600;
    const height = this.height;
    const ctx = this.ctx();
    if (!ctx) return;

    // Biggest first: large words need the free space at the centre, and small ones can
    // tuck into whatever gaps are left.
    const words = [...(this.words ?? [])].sort((a, b) => b.count - a.count || Math.abs(b.valence) - Math.abs(a.valence));
    const counts = words.map((w) => w.count);
    const maxCount = Math.max(1, ...counts);
    const minCount = Math.min(...(counts.length ? counts : [1]));

    const boxes: Box[] = [];
    const placed: PlacedWord[] = [];
    let dropped = 0;

    for (const w of words) {
      const size = this.fontSize(w.count, minCount, maxCount);
      ctx.font = `${this.fontWeight(size)} ${size}px ${FONT_FAMILY}`;
      const tw = ctx.measureText(w.text).width;
      const th = size * 1.05;

      // Positive words aim high, negative low; the spiral pulls them together from there.
      const anchorY = w.sentiment === 'positive' ? height * 0.33 : height * 0.67;
      const spot = this.findSpot(tw, th, width, height, width / 2, anchorY, boxes);
      if (!spot) {
        dropped++;
        continue;
      }
      boxes.push({ x: spot.x, y: spot.y, w: tw, h: th });
      placed.push({ ...w, size, x: spot.x, y: spot.y });
    }

    this.placed = placed;
    this.dropped = dropped;
    // Shrink to what was actually used. The two polarities are rarely balanced, and a
    // fixed height leaves an empty band under the smaller side that reads as a bug.
    const bottom = placed.reduce((m, p) => Math.max(m, p.y + p.size * 1.05), 0);
    const top = placed.reduce((m, p) => Math.min(m, p.y), height);
    this.renderedHeight = placed.length ? Math.max(160, Math.ceil(bottom - top) + 10) : height;
    this.offsetY = placed.length ? -Math.floor(top) + 5 : 0;
    this.cdr.markForCheck();
  }

  /** Square-root scale: with counts spanning a small range, a linear map leaves every
   *  word the same size, and area (not height) is what the eye compares. */
  private fontSize(count: number, minCount: number, maxCount: number): number {
    if (maxCount <= minCount) return (MIN_FONT + MAX_FONT) / 2.4;
    const t = (Math.sqrt(count) - Math.sqrt(minCount)) / (Math.sqrt(maxCount) - Math.sqrt(minCount));
    return Math.round(MIN_FONT + t * (MAX_FONT - MIN_FONT));
  }

  private fontWeight(size: number): number {
    return size >= 26 ? 700 : size >= 17 ? 600 : 500;
  }

  /** Walks an Archimedean spiral out from (cx, cy) until the box fits without touching
   *  anything already placed and stays inside the stage. */
  private findSpot(
    w: number,
    h: number,
    stageW: number,
    stageH: number,
    cx: number,
    cy: number,
    boxes: Box[],
  ): { x: number; y: number } | null {
    // Reach the far corner, or words simply stop being placed once the middle fills up.
    const maxRadius = Math.hypot(stageW, stageH);
    // Flattened vertically so the cloud grows wider than tall, which suits a dashboard panel.
    const yScale = 0.6;
    const growth = 1.7;

    let angle = 0;
    for (let i = 0; i < 8000; i++) {
      const radius = growth * angle;
      if (radius > maxRadius) break;
      const x = cx + radius * Math.cos(angle) - w / 2;
      const y = cy + radius * Math.sin(angle) * yScale - h / 2;
      // Constant arc length per step: a fixed angular step samples coarsely far out and
      // would stride straight past gaps that a word would have fitted into.
      angle += 5 / Math.max(radius, 8);

      if (x < 0 || y < 0 || x + w > stageW || y + h > stageH) continue;
      const candidate: Box = { x, y, w, h };
      if (!boxes.some((b) => this.overlaps(candidate, b))) return { x, y };
    }
    return null;
  }

  private overlaps(a: Box, b: Box): boolean {
    return (
      a.x < b.x + b.w + PADDING &&
      a.x + a.w + PADDING > b.x &&
      a.y < b.y + b.h + PADDING &&
      a.y + a.h + PADDING > b.y
    );
  }

  private ctx(): CanvasRenderingContext2D | null {
    if (!this.measureCtx) {
      this.measureCtx = document.createElement('canvas').getContext('2d');
    }
    return this.measureCtx;
  }
}
