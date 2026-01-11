from flask import Flask, jsonify
from flask_cors import CORS
from flasgger import Swagger
import logging
from modules.auth import auth_bp
from modules.dashboard_operational import dashboard_ops_bp
from modules.dashboard_sla import dashboard_sla_bp
from modules.dashboard_jobs import dashboard_jobs_bp
from modules.configuration import config_bp

# Setup app and logging
app = Flask(__name__)
CORS(app) # Enable CORS for all routes

# Swagger Configuration
app.config['SWAGGER'] = {
    'title': 'UiPath ETL Dashboard API',
    'uiversion': 3,
    'description': 'API for RPA Analytics Dashboards (Queues, SLA, Jobs)',
    'version': '1.0.0',
    'securityDefinitions': {
        'Bearer': {
            'type': 'apiKey',
            'name': 'Authorization',
            'in': 'header',
            'description': 'JWT Authorization header using the Bearer scheme. Example: "Bearer {token}"'
        }
    }
}
swagger = Swagger(app)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Register Blueprints
app.register_blueprint(auth_bp, url_prefix='/api/auth')
app.register_blueprint(dashboard_ops_bp, url_prefix='/api/dashboard')
app.register_blueprint(dashboard_sla_bp, url_prefix='/api/dashboard')
app.register_blueprint(dashboard_jobs_bp, url_prefix='/api/dashboard')
app.register_blueprint(config_bp, url_prefix='/api/admin')

@app.route('/api/health', methods=['GET'])
def health_check():
    """
    System Health Check
    ---
    tags:
      - System
    responses:
      200:
        description: Service is healthy
        schema:
          type: object
          properties:
            status:
              type: string
              example: healthy
            service:
              type: string
              example: uipath-etl-api
    """
    return jsonify({"status": "healthy", "service": "uipath-etl-api"}), 200

if __name__ == '__main__':
    logger.info("Starting Modular API Server on port 5000...")
    app.run(host='0.0.0.0', port=5000, debug=True)
