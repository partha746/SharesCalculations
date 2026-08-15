import { Component, Input } from '@angular/core';


@Component({
    selector: 'app-stat-card',
    imports: [],
    templateUrl: './stat-card.component.html',
    styleUrl: './stat-card.component.scss'
})
export class StatCardComponent {
  @Input() title = '';
  @Input() value: string | number = '';
  @Input() sublabel = '';
  @Input() currency: 'USD' | 'INR' | 'none' = 'none';
}
