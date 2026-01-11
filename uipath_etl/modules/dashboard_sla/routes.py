from flask import Blueprint, request, jsonify
from modules.auth import token_required
from shared.utils import get_dates_from_request
from .service import DashboardSlaService

dashboard_sla_bp = Blueprint('dashboard_sla', __name__)
dashboard_service = DashboardSlaService()

@dashboard_sla_bp.route('/sla/compliance', methods=['GET'])
@token_required
def get_sla_compliance(current_user):
    """
    Dashboard 2: SLA Compliance Overview
    ---
    tags:
      - Dashboard 2 (SLA & Risk)
    security:
      - Bearer: []
    parameters:
      - name: startDate
        in: query
        type: string
      - name: endDate
        in: query
        type: string
      - name: sla_hours
        in: query
        type: integer
        default: 24
        description: SLA threshold in hours
    responses:
      200:
        description: SLA Compliance %, Breach Count, Avg Breach Duration
    """
    start, end = get_dates_from_request()
    sla = int(request.args.get('sla_hours', 24))
    data = dashboard_service.get_sla_compliance(start, end, sla)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching SLA compliance"}), 500

@dashboard_sla_bp.route('/sla/risk', methods=['GET'])
@token_required
def get_sla_risk(current_user):
    """
    Dashboard 2: SLA Risk Assessment
    ---
    tags:
      - Dashboard 2 (SLA & Risk)
    security:
      - Bearer: []
    parameters:
      - name: startDate
        in: query
        type: string
      - name: endDate
        in: query
        type: string
      - name: sla_hours
        in: query
        type: integer
        default: 24
    responses:
      200:
        description: Risk metrics (Items close to breach, current breaches, aging breach rate)
    """
    # This endpoint mixes live snapshot (risk) and historical (aging breach rate)
    start, end = get_dates_from_request()
    sla = int(request.args.get('sla_hours', 24))
    data = dashboard_service.get_sla_risk(start, end, sla)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching SLA risk"}), 500

@dashboard_sla_bp.route('/quality/exceptions', methods=['GET'])
@token_required
def get_exceptions(current_user):
    """
    Dashboard 2: Exception Analysis
    ---
    tags:
      - Dashboard 2 (SLA & Risk)
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
        description: Failure rate and exception breakdown (Business vs System)
    """
    start, end = get_dates_from_request()
    data = dashboard_service.get_exception_analysis(start, end)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching exception analysis"}), 500

@dashboard_sla_bp.route('/quality/retries', methods=['GET'])
@token_required
def get_retries(current_user):
    """
    Dashboard 2: Retries & Rework
    ---
    tags:
      - Dashboard 2 (SLA & Risk)
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
        description: Retry rate, avg retries, and retry success rate
    """
    start, end = get_dates_from_request()
    data = dashboard_service.get_retry_metrics(start, end)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching retry metrics"}), 500

@dashboard_sla_bp.route('/risk/operational', methods=['GET'])
@token_required
def get_operational_risk(current_user):
    """
    Dashboard 2: Operational Risk Flags
    ---
    tags:
      - Dashboard 2 (SLA & Risk)
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
        description: Orphan/Zombie items and abandoned rate
    """
    start, end = get_dates_from_request()
    data = dashboard_service.get_operational_risk(start, end)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching operational risk"}), 500

@dashboard_sla_bp.route('/quality/failures/queue', methods=['GET'])
@token_required
def get_failures_queue(current_user):
    """
    Dashboard 2: Failures by Queue
    ---
    tags:
      - Dashboard 2 (SLA & Risk)
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
        description: List of queues with failure counts and rates
    """
    start, end = get_dates_from_request()
    data = dashboard_service.get_failures_by_queue(start, end)
    if data is not None: return jsonify(data), 200
    return jsonify({"message": "Error fetching failures by queue"}), 500

@dashboard_sla_bp.route('/quality/failures/recent', methods=['GET'])
@token_required
def get_recent_failures(current_user):
    """
    Dashboard 2: Recent Failures
    ---
    tags:
      - Dashboard 2 (SLA & Risk)
    security:
      - Bearer: []
    parameters:
      - name: limit
        in: query
        type: integer
        default: 10
    responses:
      200:
        description: List of last N failed queue items
    """
    limit = int(request.args.get('limit', 10))
    data = dashboard_service.get_recent_failures(limit)
    if data is not None: return jsonify(data), 200
    return jsonify({"message": "Error fetching recent failures"}), 500

@dashboard_sla_bp.route('/quality/failures/reasons', methods=['GET'])
@token_required
def get_failure_reasons(current_user):
    """
    Dashboard 2: Top Failure Reasons
    ---
    tags:
      - Dashboard 2 (SLA & Risk)
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
        description: Top failure reasons (Placeholder if DB data missing)
    """
    start, end = get_dates_from_request()
    data = dashboard_service.get_top_failure_reasons(start, end)
    if data is not None: return jsonify(data), 200
    return jsonify({"message": "Error fetching failure reasons"}), 500

@dashboard_sla_bp.route('/quality/failures/trend', methods=['GET'])
@token_required
def get_failure_trend(current_user):
    """
    Dashboard 2: Failure Trend
    ---
    tags:
      - Dashboard 2 (SLA & Risk)
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
        description: Failure counts over time
    """
    start, end = get_dates_from_request()
    interval = request.args.get('interval', 'HOUR')
    data = dashboard_service.get_failure_trend(start, end, interval)
    if data is not None: return jsonify(data), 200
    return jsonify({"message": "Error fetching failure trend"}), 500
