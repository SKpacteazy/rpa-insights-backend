from flask import request

def get_dates_from_request():
    """Helper to extract optional start/end params"""
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    return start_date, end_date
