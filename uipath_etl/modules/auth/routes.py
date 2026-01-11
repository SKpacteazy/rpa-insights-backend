from flask import Blueprint, request, jsonify
from functools import wraps
from .service import AuthManager

auth_bp = Blueprint('auth', __name__)
auth_manager = AuthManager()

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header.split(" ")[1]
        
        if not token:
            return jsonify({'message': 'Token is missing!'}), 401
        
        current_user = auth_manager.validate_token(token)
        if not current_user:
            return jsonify({'message': 'Token is invalid or expired!'}), 401
        
        return f(current_user=current_user, *args, **kwargs)
    
    return decorated

@auth_bp.route('/signup', methods=['POST'])
def signup():
    """
    User Registration
    ---
    tags:
      - Authentication
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - username
            - email
            - password
          properties:
            username:
              type: string
            email:
              type: string
            password:
              type: string
    responses:
      201:
        description: User created successfully
      400:
        description: Invalid input or user exists
    """
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "No input data provided"}), 400
    
    username = data.get('username')
    email = data.get('email')
    password = data.get('password')

    if not all([username, email, password]):
        return jsonify({"success": False, "message": "Missing required fields: username, email, password"}), 400

    result = auth_manager.signup(username, email, password)
    
    if result.get("success"):
        return jsonify(result), 201
    else:
        return jsonify(result), 400

@auth_bp.route('/login', methods=['POST'])
def login():
    """
    User Login
    ---
    tags:
      - Authentication
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - password
          properties:
            username:
              type: string
              description: Username or Email
            password:
              type: string
    responses:
      200:
        description: Login successful
        schema:
          type: object
          properties:
            success:
              type: boolean
            token:
              type: string
              description: JWT Token
      401:
        description: Invalid credentials
    """
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "No input data provided"}), 400
    
    username = data.get('username') or data.get('email')
    password = data.get('password')

    if not all([username, password]):
        return jsonify({"success": False, "message": "Missing required fields: username (or email), password"}), 400

    result = auth_manager.login(username, password)
    
    if result.get("success"):
        return jsonify(result), 200
    else:
        return jsonify(result), 401
