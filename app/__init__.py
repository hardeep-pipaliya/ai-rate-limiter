# app/__init__.py
import os
from flask import Flask, jsonify
from flask_swagger_ui import get_swaggerui_blueprint
from .connection.orm_postgres_connection import init_orm, db

def create_app():
    app = Flask(__name__)

    # — Load your Config class which itself can read os.environ or .env/.env.docker
    app.config.from_object("app.config.config.Config")

    # — Initialize DB/Migrations
    init_orm(app)

    # — Root + health routes
    @app.route("/")
    def index():
        return jsonify({
            "success": True,
            "message": "New AI Rate Limiter API Running"
        }), 200

    @app.route('/health')
    def health_check():
        return jsonify({"status": "healthy"}), 200




    # — Swagger UI
    SWAGGER_URL = "/swagger"
    API_URL     = "/static/swagger.yaml"
    swagger_bp  = get_swaggerui_blueprint(
        SWAGGER_URL, API_URL, config={"app_name": "AI Rate Limiter API"}
    )
    app.register_blueprint(swagger_bp, url_prefix=SWAGGER_URL)

    # — Your domain models (so Alembic sees them)
    from app.models.workspaces     import WorkSpace
    from app.models.messages       import Message
    from app.models.provider_keys  import ProviderKey
    from app.models.workers        import Worker

    # — Register all the blueprints
    from app.routes.api            import bp as api_bp
    from app.routes.workspaces     import workspace_bp, workspaces_bp
    from app.routes.provider_keys  import provider_keys_bp
    from app.routes.workers        import workers_bp
    from app.routes.message        import message_bp

    app.register_blueprint(api_bp)            
    app.register_blueprint(workspace_bp)      # e.g. /workspace/…
    app.register_blueprint(workspaces_bp)     # e.g. /workspaces…
    app.register_blueprint(provider_keys_bp)  # /provider-keys…
    app.register_blueprint(workers_bp)        # /workers…
    app.register_blueprint(message_bp)        # /message/publish…

    return app
