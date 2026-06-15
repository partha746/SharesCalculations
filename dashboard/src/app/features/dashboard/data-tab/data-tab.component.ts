import { ChangeDetectionStrategy, Component } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';

/** Data tab: embeds the nvShares.db Data Manager (static page served from assets). */
@Component({
  selector: 'app-data-tab',
  standalone: true,
  templateUrl: './data-tab.component.html',
  styleUrl: './data-tab.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DataTabComponent {
  readonly iframeSrc: SafeResourceUrl;

  constructor(sanitizer: DomSanitizer) {
    this.iframeSrc = sanitizer.bypassSecurityTrustResourceUrl('assets/web-app/index.html');
  }
}
