from flask import Blueprint, request, jsonify
from modules.auth import token_required
from shared.utils import get_dates_from_request
from .service import DashboardJobsService

dashboard_jobs_bp = Blueprint('dashboard_jobs', __name__)
jobs_service = DashboardJobsService()

@dashboard_jobs_bp.route('/jobs/snapshot', methods=['GET'])
@token_required
def get_jobs_snapshot(current_user):
    """
    Dashboard 3: Jobs Snapshot
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
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
        description: Count of jobs by state (Total, Pending, Running, etc.)
    """
    start, end = get_dates_from_request()
    data = jobs_service.get_jobs_snapshot(start, end)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching jobs snapshot"}), 500

@dashboard_jobs_bp.route('/jobs/distribution', methods=['GET'])
@token_required
def get_jobs_distribution(current_user):
    """
    Dashboard 3: Jobs Distribution
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
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
        description: Percentage distribution of jobs by state
    """
    start, end = get_dates_from_request()
    data = jobs_service.get_jobs_distribution(start, end)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching jobs distribution"}), 500

@dashboard_jobs_bp.route('/jobs/trend', methods=['GET'])
@token_required
def get_jobs_trend(current_user):
    """
    Dashboard 3: Jobs Volume Trend
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
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
        description: Jobs created/started/completed/failed over time
    """
    start, end = get_dates_from_request()
    interval = request.args.get('interval', 'HOUR')
    data = jobs_service.get_jobs_volume_trend(start, end, interval)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching jobs trend"}), 500

@dashboard_jobs_bp.route('/jobs/performance', methods=['GET'])
@token_required
def get_jobs_performance(current_user):
    """
    Dashboard 3: Jobs Performance
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
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
        description: Avg/Median/Max execution time and trend
    """
    start, end = get_dates_from_request()
    data = jobs_service.get_jobs_performance(start, end)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching jobs performance"}), 500

@dashboard_jobs_bp.route('/jobs/reliability', methods=['GET'])
@token_required
def get_jobs_reliability(current_user):
    """
    Dashboard 3: Jobs Reliability
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
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
        description: Job failure rate and count
    """
    start, end = get_dates_from_request()
    data = jobs_service.get_jobs_reliability(start, end)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching jobs reliability"}), 500

@dashboard_jobs_bp.route('/jobs/failures/reasons', methods=['GET'])
@token_required
def get_jobs_failure_reasons(current_user):
    """
    Dashboard 3: Job Failure Reasons
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
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
        description: Top reasons for job failures
    """
    start, end = get_dates_from_request()
    data = jobs_service.get_jobs_failure_reasons(start, end)
    if data is not None: return jsonify(data), 200
    return jsonify({"message": "Error fetching jobs failure reasons"}), 500

@dashboard_jobs_bp.route('/jobs/release', methods=['GET'])
@token_required
def get_jobs_by_release(current_user):
    """
    Dashboard 3: Jobs By Release
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
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
        description: Performance and reliability per release
    """
    start, end = get_dates_from_request()
    data = jobs_service.get_jobs_by_release(start, end)
    if data is not None: return jsonify(data), 200
    return jsonify({"message": "Error fetching jobs by release"}), 500

@dashboard_jobs_bp.route('/jobs/triggers', methods=['GET'])
@token_required
def get_jobs_triggers(current_user):
    """
    Dashboard 3: Trigger Analysis
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
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
        description: Breakdown by Source (Manual/Trigger) and Type (Attended/Unattended)
    """
    start, end = get_dates_from_request()
    data = jobs_service.get_jobs_trigger_analysis(start, end)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching jobs triggers"}), 500

@dashboard_jobs_bp.route('/jobs/risk', methods=['GET'])
@token_required
def get_jobs_risk(current_user):
    """
    Dashboard 3: Risk Flags
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
    security:
      - Bearer: []
    parameters:
      - name: threshold_hours
        in: query
        type: integer
        default: 24
    responses:
      200:
        description: Long running, stuck pending, and zombie jobs
    """
    threshold = int(request.args.get('threshold_hours', 24))
    data = jobs_service.get_jobs_risk_flags(threshold)
    if data: return jsonify(data), 200
    return jsonify({"message": "Error fetching jobs risk"}), 500

@dashboard_jobs_bp.route('/jobs/failures/recent', methods=['GET'])
@token_required
def get_recent_failed_jobs(current_user):
    """
    Dashboard 3: Recent Failed Jobs
    ---
    tags:
      - Dashboard 3 (Jobs Execution)
    security:
      - Bearer: []
    parameters:
      - name: limit
        in: query
        type: integer
        default: 10
    responses:
      200:
        description: List of recently failed jobs
    """
    limit = int(request.args.get('limit', 10))
    data = jobs_service.get_recent_failed_jobs(limit)
    if data is not None: return jsonify(data), 200
    return jsonify({"message": "Error fetching recent failed jobs"}), 500
