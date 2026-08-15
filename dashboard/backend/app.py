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

    for bp in (tables_bp, market_bp, live_price_bp, portfolio_bp, tax_bp, breeze_bp, news_bp, mf_bp, earmarks_bp, finance_plan_bp, networth_bp, income_bp):
        app.register_blueprint(bp)
    return app


def start_recorder():
    """Launch the background live-price recorder thread (records every 14s when market open)."""
    import threading
    from services.market import _live_price_recorder_loop

    rec = threading.Thread(target=_live_price_recorder_loop, daemon=True)
    rec.start()
    return rec
