import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { DashboardService } from '../../../core/services/dashboard.service';
import { NewsArticle, NewsSummary } from '../../../core/models/dashboard.types';

/** News tab: latest NVIDIA headlines (Finnhub) with VADER sentiment. */
@Component({
    selector: 'app-news-tab',
    imports: [CommonModule, FormsModule],
    templateUrl: './news-tab.component.html',
    styleUrl: './news-tab.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class NewsTabComponent implements OnInit {
  articles: NewsArticle[] = [];
  summary: NewsSummary | null = null;
  fetchedAt: number | null = null;
  loading = false;
  error: string | null = null;
  rangeDays = 7;
  /** Filter chips: all | positive | negative | neutral */
  filter: 'all' | 'positive' | 'negative' | 'neutral' = 'all';

  constructor(private dashboardService: DashboardService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.load();
  }

  load(): void {
    this.loading = true;
    this.error = null;
    this.cdr.markForCheck();
    this.dashboardService.getNews(this.rangeDays).subscribe({
      next: (res) => {
        this.articles = res.articles ?? [];
        this.summary = res.summary ?? null;
        this.fetchedAt = res.fetchedAt ?? null;
        this.loading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        this.error = err?.error?.error || err?.message || 'Failed to load news';
        this.loading = false;
        this.cdr.markForCheck();
      },
    });
  }

  setRange(days: number): void {
    if (this.rangeDays === days) return;
    this.rangeDays = days;
    this.load();
  }

  setFilter(f: 'all' | 'positive' | 'negative' | 'neutral'): void {
    this.filter = f;
  }

  get filteredArticles(): NewsArticle[] {
    if (this.filter === 'all') return this.articles;
    return this.articles.filter((a) => a.sentiment === this.filter);
  }

  /** Gauge needle position 0-100 from score. */
  get gaugePct(): number {
    return this.summary ? this.summary.score100 : 50;
  }

  overallClass(): string {
    return this.summary ? 'news-sentiment--' + this.summary.overall : '';
  }

  relativeTime(ms: number): string {
    if (!ms) return '';
    const diff = Date.now() - ms;
    const m = Math.floor(diff / 60000);
    if (m < 1) return 'just now';
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    const d = Math.floor(h / 24);
    return `${d}d ago`;
  }
}
