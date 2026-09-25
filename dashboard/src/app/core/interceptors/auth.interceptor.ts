import { inject } from '@angular/core';
import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { Router } from '@angular/router';
import { catchError, throwError } from 'rxjs';

/** Sends the user to the login page when a session expires mid-visit, instead of
 *  letting every panel quietly fill with error text. */
export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const router = inject(Router);

  return next(req).pipe(
    catchError((err: HttpErrorResponse) => {
      // The auth endpoints answer 401 as a normal outcome (wrong password); the login
      // form shows that itself and must not be bounced.
      const isAuthCall = req.url.startsWith('/api/auth/');
      if (err.status === 401 && !isAuthCall && !router.url.startsWith('/login')) {
        router.navigate(['/login'], { queryParams: { next: router.url } });
      }
      return throwError(() => err);
    }),
  );
};
