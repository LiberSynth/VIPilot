from flask import Flask

def register_blueprints(app: Flask) -> None:
    from routes.web import bp as web_bp
    from routes.api import bp as api_bp, production_bp
    from routes.browser_widget import bp as browser_widget_bp

    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(production_bp)
    app.register_blueprint(browser_widget_bp)
