"""Flask application factory + background price-recorder startup.

The route handlers live in routes/* (blueprints); business logic in services/*;
shared DB access in db.py; secrets/paths in helpers/config.py.
"""
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix


def create_app():
    app = Flask(__name__)
    # We always sit behind serve-prod.js, which forwards /api. Trust its X-Forwarded-* headers so
    # request.url_root reflects the browser's origin (scheme + host) rather than the internal
    # backend address — absolute URLs such as the ICICI OAuth callback are derived from it.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    from routes.tables import bp as tables_bp
    from routes.market import bp as market_bp
    from routes.live_price import bp as live_price_bp
    from routes.portfolio import bp as portfolio_bp
    from routes.tax import bp as tax_bp
    from routes.breeze import bp as breeze_bp
    from routes.news import bp as news_bp
    from routes.mf import bp as mf_bp
    from routes.earmarks import bp as earmarks_bp
    from routes.finance_plan import bp as finance_plan_bp
    from routes.networth import bp as networth_bp
    from routes.income import bp as income_bp
    from routes.advance_tax import bp as advance_tax_bp
    from routes.metals import bp as metals_bp

    for bp in (tables_bp, market_bp, live_price_bp, portfolio_bp, tax_bp, breeze_bp, news_bp, mf_bp, earmarks_bp, finance_plan_bp, networth_bp, income_bp, advance_tax_bp, metals_bp):
        app.register_blueprint(bp)
    return app


def start_recorder():
    """Launch the background live-price recorder thread (records every 14s when market open)."""
    import threading
    from services.market import _live_price_recorder_loop

    rec = threading.Thread(target=_live_price_recorder_loop, daemon=True)
    rec.start()
    return rec


def _metal_recorder_loop():
    """Snapshot Pune gold/silver every 15 minutes.

    Separate from the price recorder, which sleeps outside US market hours: bullion rates
    are published on their own schedule. The service skips writes when nothing moved, so an
    unchanged rate does not accumulate rows.
    """
    import time
    from services.metals import record_rates

    INTERVAL_SEC = 15 * 60
    while True:
        try:
            written = record_rates()
            if written:
                print("[metal-recorder] recorded {} series".format(written), flush=True)
        except Exception as e:
            print("[metal-recorder] {}".format(e), flush=True)
        time.sleep(INTERVAL_SEC)


def start_metal_recorder():
    """Launch the background gold/silver rate recorder thread."""
    import threading

    rec = threading.Thread(target=_metal_recorder_loop, daemon=True)
    rec.start()
    return rec
