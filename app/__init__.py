# app/__init__.py
# Version 1.0 — 2025-01-01
#
# Flask application factory for The Queue

import os
from flask import Flask

def create_app():
    """Create and configure the Flask application"""
    app = Flask(__name__)
    
    # Configuration
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'thequeue-dev-key')
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    
    # Register blueprints
    from app.routes.views import views
    app.register_blueprint(views)
    
    return app
