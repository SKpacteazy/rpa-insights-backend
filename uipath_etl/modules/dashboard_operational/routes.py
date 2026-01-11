from flask import Blueprint, request, jsonify
from modules.auth import token_required
from shared.utils import get_dates_from_request
from .service import DashboardOpsService

dashboard_ops_bp = Blueprint('dashboard_ops', __name__)
dashboard_service = DashboardOpsService()

@dashboard_ops_bp.route('/kpi/volume', methods=['GET'])
@token_required
def get_volume_snapshot(current_user):
    """
    Dashboard 1: Queue Volume Snapshot
    ---
    tags:
      - Dashboard 1 (Operations)
    security:
      - Bearer: []
    parameters:
      - name: startDate
        in: query
        type: string
        description: Filter start date (YYYY-MM-DD HH:MM:SS)
      - name: endDate
        in: query
        type: string
        description: Filter end date (YYYY-MM-DD HH:MM:SS)
    responses:
      200:
        description: Volume metrics (Total, New, Successful, Failed, etc.)
      401:
        description: Unauthorized
    """
    start, end = get_dates_from_request()
    data = dashboard_service.get_queue_volume_snapshot(start, end)
    if data:
        return jsonify(data), 200
    return jsonify({"message": "Error fetching volume snapshot"}), 500

@dashboard_ops_bp.route('/kpi/trend', methods=['GET'])
@token_required
def get_trend(current_user):
    """
    Dashboard 1: Demand vs Completion Trend
    ---
    tags:
      - Dashboard 1 (Operations)
    security:
      - Bearer: []
    parameters:
      - name: startDate
        in: query
        type: string
      - name: endDate
        in: query
        type: string
      - name: interval
        in: query
        type: string
        enum: [HOUR, DAY]
        default: HOUR
    responses:
      200:
        description: Time-series data for Created vs Completed vs Backlog
    """
    start, end = get_dates_from_request()
    interval = request.args.get('interval', 'HOUR')
    data = dashboard_service.get_trend_analysis(start, end, interval)
    if data:
        return jsonify(data), 200
    return jsonify({"message": "Error fetching trend analysis"}), 500

@dashboard_ops_bp.route('/kpi/status', methods=['GET'])
@token_required
def get_status_dist(current_user):
    """
    Dashboard 1: Queue Status Distribution
    ---
    tags:
      - Dashboard 1 (Operations)
    security:
      - Bearer: []
    parameters:
      - name: startDate
        in: query
        type: string
      - name: endDate
        in: query
        type: string
    responses:
      200:
        description: Percentage distribution of queue items by status
    """
    start, end = get_dates_from_request()
    data = dashboard_service.get_status_distribution(start, end)
    if data:
        return jsonify(data), 200
    return jsonify({"message": "Error fetching status distribution"}), 500

@dashboard_ops_bp.route('/kpi/aging', methods=['GET'])
@token_required
def get_aging(current_user):
    """
    Dashboard 1: Aging & Wait Time
    ---
    tags:
      - Dashboard 1 (Operations)
    security:
      - Bearer: []
    parameters:
      - name: startDate
        in: query
        type: string
      - name: endDate
        in: query
        type: string
      - name: threshold_hours
        in: query
        type: integer
        default: 24
    responses:
      200:
        description: Avg wait time, max age, and aging buckets
    """
    start, end = get_dates_from_request()
    threshold = int(request.args.get('threshold_hours', 24))
    data = dashboard_service.get_aging_metrics(start, end, threshold)
    if data:
        return jsonify(data), 200
    return jsonify({"message": "Error fetching aging metrics"}), 500

@dashboard_ops_bp.route('/kpi/performance', methods=['GET'])
@token_required
def get_performance(current_user):
    """
    Dashboard 1: Processing Performance
    ---
    tags:
      - Dashboard 1 (Operations)
    security:
      - Bearer: []
    parameters:
      - name: startDate
        in: query
        type: string
      - name: endDate
        in: query
        type: string
    responses:
      200:
        description: Avg/Median processing time and trends
    """
    start, end = get_dates_from_request()
    data = dashboard_service.get_processing_performance(start, end)
    if data:
        return jsonify(data), 200
    return jsonify({"message": "Error fetching performance metrics"}), 500

@dashboard_ops_bp.route('/kpi/benchmarking', methods=['GET'])
@token_required
def get_benchmarking(current_user):
    """
    Dashboard 1: Queue Benchmarking
    ---
    tags:
      - Dashboard 1 (Operations)
    security:
      - Bearer: []
    parameters:
      - name: startDate
        in: query
        type: string
      - name: endDate
        in: query
        type: string
    responses:
      200:
        description: Performance metrics per Queue Definition
    """
    start, end = get_dates_from_request()
    data = dashboard_service.get_benchmarking(start, end)
    if data:
        return jsonify(data), 200
    return jsonify({"message": "Error fetching benchmarking"}), 500
