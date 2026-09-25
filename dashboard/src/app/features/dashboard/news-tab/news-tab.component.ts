import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { DashboardService } from '../../../core/services/dashboard.service';
import { NewsArticle, NewsCloudWord, NewsSummary } from '../../../core/models/dashboard.types';
import { SentimentCloudComponent } from './sentiment-cloud.component';

/** News tab: latest NVIDIA headlines (Finnhub) with VADER sentiment. */
@Component({
    selector: 'app-news-tab',
    imports: [CommonModule, FormsModule, SentimentCloudComponent],
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
  words: NewsCloudWord[] = [];
  /** Off by default: the raw NVDA feed is mostly about other companies. */
  showEverything = false;
  /** Narrows the list to one subject, e.g. "China / export". */
  topicFilter = '';

  constructor(private dashboardService: DashboardService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.load();
  }

  load(): void {
    this.loading = true;
    this.error = null;
    this.cdr.markForCheck();
    this.dashboardService.getNews(this.rangeDays, this.showEverything ? 0 : undefined).subscribe({
      next: (res) => {
        this.articles = res.articles ?? [];
        this.summary = res.summary ?? null;
        this.words = res.wordCloud ?? [];
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

  toggleEverything(): void {
    this.showEverything = !this.showEverything;
    this.topicFilter = '';
    this.load(); // the relevance floor is applied server-side, so this needs a refetch
  }

  setTopic(topic: string): void {
    this.topicFilter = this.topicFilter === topic ? '' : topic;
  }

  /** Subjects present in the current set, most common first, for the chip row. */
  get topics(): Array<{ name: string; count: number }> {
    const counts = new Map<string, number>();
    for (const a of this.articles) {
      for (const t of a.topics ?? []) counts.set(t, (counts.get(t) ?? 0) + 1);
    }
    return [...counts.entries()]
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count);
  }

  get filteredArticles(): NewsArticle[] {
    let rows = this.articles;
    if (this.filter !== 'all') rows = rows.filter((a) => a.sentiment === this.filter);
    if (this.topicFilter) rows = rows.filter((a) => (a.topics ?? []).includes(this.topicFilter));
    return rows;
  }

  /** Coarse band for the badge: a 0.9 headline is squarely about NVIDIA, 0.5 is a mention. */
  relevanceLabel(r: number): string {
    if (r >= 0.8) return 'High';
    if (r >= 0.6) return 'Medium';
    return 'Low';
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
