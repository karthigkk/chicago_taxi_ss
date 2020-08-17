from flask import Flask
from .config import get_logger

_logger = get_logger(logger_name=__name__)

def create_app(*, config_object) -> Flask:
    # Create flask app

    flask_app = Flask('ml_api')
    flask_app.config.from_object(config_object)

    # import blueprints
    from api.controller import ml_api_blueprint
    from ui_components.fare_predict_form import forms_blueprints
    flask_app.register_blueprint(ml_api_blueprint)
    flask_app.register_blueprint(forms_blueprints)
    _logger.debug('Application instance created')

    return flask_app
