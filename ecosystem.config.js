module.exports = {
  apps: [
    {
      name: 'shares-backend',
      script: 'python3',
      args: 'dashboard/backend/server.py',
      cwd: '/home/psardar/Scripts/SharesCalculations',
      interpreter: 'none',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 3000,
      out_file: './logs/backend-out.log',
      error_file: './logs/backend-err.log',
      merge_logs: true,
      env: {
        PORT: 8080,
      },
    },
    {
      name: 'shares-dashboard',
      // Serves the prebuilt Angular app (dist/dashboard) + proxies /api to the backend.
      // Deploy frontend changes with: cd dashboard && npm run build  (then `pm2 restart shares-dashboard` is optional).
      script: 'serve-prod.js',
      cwd: '/home/psardar/Scripts/SharesCalculations/dashboard',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 3000,
      out_file: '/home/psardar/Scripts/SharesCalculations/logs/dashboard-out.log',
      error_file: '/home/psardar/Scripts/SharesCalculations/logs/dashboard-err.log',
      merge_logs: true,
      env: {
        PORT: 4201,
        API_TARGET: 'http://127.0.0.1:8080',
      },
    },
  ],
};
