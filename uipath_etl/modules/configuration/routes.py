from flask import Blueprint, request, jsonify
from .service import ConfigurationService
from modules.auth.routes import token_required

config_bp = Blueprint('configuration', __name__)
service = ConfigurationService()

@config_bp.route('/config', methods=['GET'])
@token_required # Secure this endpoint
def get_configuration(current_user):
    """
    Get UiPath Configuration
    ---
    tags:
      - Configuration
    security:
      - Bearer: []
    responses:
      200:
        description: Current configuration
        schema:
          type: object
          properties:
            base_url:
              type: string
            client_id:
              type: string
            org:
              type: string
            tenant:
              type: string
            scope:
              type: string
            # Exclude client_secret for security in generic GET? 
            # Often admin needs to see it, or amasked version.
            # Returning all for now as per user request.
      500:
        description: Server Error
    """
    config = service.get_config()
    if config:
        return jsonify(config), 200
    return jsonify({"error": "Failed to fetch configuration"}), 500

@config_bp.route('/config', methods=['PUT'])
@token_required
def update_configuration(current_user):
    """
    Update UiPath Configuration
    ---
    tags:
      - Configuration
    security:
      - Bearer: []
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - base_url
            - client_id
            - client_secret
            - org
            - tenant
            - scope
          properties:
            base_url:
              type: string
            client_id:
              type: string
            client_secret:
              type: string
            org:
              type: string
            tenant:
              type: string
            scope:
              type: string
    responses:
      200:
        description: Configuration updated successfully
      400:
        description: Invalid input
      500:
        description: Update failed
    """
    data = request.json
    required_fields = ['base_url', 'client_id', 'client_secret', 'org', 'tenant', 'scope']
    
    if not data or not all(k in data for k in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    success = service.update_config(data)
    if success:
        return jsonify({"message": "Configuration updated successfully"}), 200
    return jsonify({"error": "Failed to update configuration"}), 500
