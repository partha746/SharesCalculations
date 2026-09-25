import { Routes } from '@angular/router';

import { authGuard, loginPageGuard } from './core/guards/auth.guard';

// Each dashboard tab has its own URL (e.g. /holdings, /sold). The DashboardComponent reads the
// :tab route param to select the active tab. Unknown paths fall back to /holdings.
// Everything except /login sits behind authGuard, which asks the server whether the
// session cookie is still good.
export const routes: Routes = [
  {
    path: 'login',
    canActivate: [loginPageGuard],
    loadComponent: () => import('./features/login/login.component').then((m) => m.LoginComponent),
  },
  { path: '', redirectTo: 'holdings', pathMatch: 'full' },
  {
    path: ':tab',
    canActivate: [authGuard],
    loadComponent: () => import('./features/dashboard/dashboard.component').then((m) => m.DashboardComponent),
  },
  { path: '**', redirectTo: 'holdings' },
];
