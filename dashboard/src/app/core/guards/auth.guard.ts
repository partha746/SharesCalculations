import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { map } from 'rxjs/operators';

import { AuthService } from '../services/auth.service';

/** Blocks the dashboard until the server confirms a live session. Asking the server
 *  rather than trusting anything client-side is what makes the HttpOnly cookie worth
 *  having — there is no local flag an attacker could flip. */
export const authGuard: CanActivateFn = (_route, state) => {
  const auth = inject(AuthService);
  const router = inject(Router);

  return auth.refresh().pipe(
    map((session) => {
      if (session.authenticated) return true;
      // Remember where they were headed so login can send them back there.
      return router.createUrlTree(['/login'], {
        queryParams: state.url && state.url !== '/login' ? { next: state.url } : undefined,
      });
    }),
  );
};

/** Keeps a signed-in user off the login page if they navigate to it directly. */
export const loginPageGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);

  return auth.refresh().pipe(map((session) => (session.authenticated ? router.createUrlTree(['/holdings']) : true)));
};
