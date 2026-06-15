"""Flask application factory + background price-recorder startup.

The route handlers live in routes/* (blueprints); business logic in services/*;
shared DB access in db.py; secrets/paths in helpers/config.py.
"""
from flask import Flask


def create_app():
    app = Flask(__name__)
    from routes.tables import bp as tables_bp
    from routes.market import bp as market_bp
    from routes.live_price import bp as live_price_bp
    from routes.portfolio import bp as portfolio_bp
    from routes.tax import bp as tax_bp
    from routes.breeze import bp as breeze_bp
    from routes.news import bp as news_bp

    for bp in (tables_bp, market_bp, live_price_bp, portfolio_bp, tax_bp, breeze_bp, news_bp):
        app.register_blueprint(bp)
    return app


def start_recorder():
    """Launch the background live-price recorder thread (records every 14s when market open)."""
    import threading
    from services.market import _live_price_recorder_loop

    rec = threading.Thread(target=_live_price_recorder_loop, daemon=True)
    rec.start()
    return rec
