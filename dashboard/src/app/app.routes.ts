import { Routes } from '@angular/router';

// Each dashboard tab has its own URL (e.g. /holdings, /sold). The DashboardComponent reads the
// :tab route param to select the active tab. Unknown paths fall back to /holdings.
export const routes: Routes = [
  { path: '', redirectTo: 'holdings', pathMatch: 'full' },
  { path: ':tab', loadComponent: () => import('./features/dashboard/dashboard.component').then(m => m.DashboardComponent) },
  { path: '**', redirectTo: 'holdings' },
];
