import logging
from datetime import datetime, timedelta
from shared.db import db
from shared.utils import get_dates_from_request
from mysql.connector import Error

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DashboardSlaService:
    def __init__(self):
        pass

    def get_connection(self):
        return db.get_connection()

    def _get_date_range(self, start_date, end_date):
        """Helper to set default dates to last 24h if None"""
        if not start_date:
            start_date = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return start_date, end_date

    def get_sla_compliance(self, start_date=None, end_date=None, sla_hours=24):
        """
        A. SLA Compliance Overview
        KPIs: SLA Compliance %, SLA Breach Count, Avg SLA Breach Duration, Trend
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)

            sla_seconds = sla_hours * 3600

            query = f"""
                SELECT 
                    COUNT(*) as total_completed,
                    SUM(CASE WHEN TIMESTAMPDIFF(SECOND, creation_time, end_processing) > {sla_seconds} THEN 1 ELSE 0 END) as breach_count,
                    AVG(CASE WHEN TIMESTAMPDIFF(SECOND, creation_time, end_processing) > {sla_seconds} 
                             THEN TIMESTAMPDIFF(SECOND, creation_time, end_processing) - {sla_seconds} 
                             ELSE NULL END) as avg_breach_amount_sec
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s AND status = 'Successful'
            """
            
            trend_query = f"""
                SELECT DATE_FORMAT(end_processing, '%Y-%m-%d %H:00:00') as time_bucket,
                       SUM(CASE WHEN TIMESTAMPDIFF(SECOND, creation_time, end_processing) > {sla_seconds} THEN 1 ELSE 0 END) as breach_count
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s AND status = 'Successful'
                GROUP BY time_bucket
                ORDER BY time_bucket
            """

            cursor.execute(query, (start_date, end_date))
            summary = cursor.fetchone()
            
            cursor.execute(trend_query, (start_date, end_date))
            trend_rows = cursor.fetchall()
            
            total = summary['total_completed'] if summary and summary['total_completed'] else 0
            breaches = int(summary['breach_count']) if summary and summary['breach_count'] else 0
            avg_breach_sec = float(summary['avg_breach_amount_sec']) if summary and summary['avg_breach_amount_sec'] else 0
            
            compliance_pct = 100.0
            if total > 0:
                compliance_pct = round(((total - breaches) / total) * 100, 2)
                
            return {
                "sla_compliance_percent": compliance_pct,
                "sla_breach_count": breaches,
                "avg_sla_breach_duration_seconds": avg_breach_sec,
                "sla_breach_trend": trend_rows
            }

        except Error as e:
            logger.error(f"Error fetching SLA compliance: {e}")
            return None
        finally:
            conn.close()

    def get_sla_risk(self, start_date=None, end_date=None, sla_hours=24):
        """
        B. SLA Risk & Aging
        Risk Tiles: Close to Breach (80% of SLA), Currently Breached (but still running)
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            sla_seconds = sla_hours * 3600
            warning_threshold = sla_seconds * 0.8
            
            query = f"""
                SELECT 
                    SUM(CASE WHEN 
                        TIMESTAMPDIFF(SECOND, creation_time, NOW()) >= {warning_threshold} 
                        AND TIMESTAMPDIFF(SECOND, creation_time, NOW()) < {sla_seconds} 
                        THEN 1 ELSE 0 END) as close_to_breach,
                    SUM(CASE WHEN 
                        TIMESTAMPDIFF(SECOND, creation_time, NOW()) >= {sla_seconds} 
                        THEN 1 ELSE 0 END) as already_breached
                FROM queue_items
                WHERE status IN ('New', 'In Progress')
            """
            
            breach_rate_query = f"""
                 SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN TIMESTAMPDIFF(SECOND, creation_time, end_processing) > {sla_seconds} THEN 1 ELSE 0 END) as breaches
                 FROM queue_items
                 WHERE end_processing BETWEEN %s AND %s
            """

            cursor.execute(query) # Snapshot
            risk_res = cursor.fetchone()
            
            cursor.execute(breach_rate_query, (start_date, end_date))
            rate_res = cursor.fetchone()
            
            total_hist = rate_res['total'] if rate_res else 0
            breaches_hist = rate_res['breaches'] if rate_res else 0
            aging_breach_rate = round((breaches_hist / total_hist * 100), 2) if total_hist > 0 else 0.0

            return {
                "items_close_to_sla_breach": int(risk_res['close_to_breach']) if risk_res['close_to_breach'] else 0,
                "items_breached_sla_current": int(risk_res['already_breached']) if risk_res['already_breached'] else 0,
                "aging_breach_rate_percent": aging_breach_rate
            }

        except Error as e:
            logger.error(f"Error fetching SLA risk: {e}")
            return None
        finally:
            conn.close()

    def get_exception_analysis(self, start_date=None, end_date=None):
        """
        C. Failure & Exception Analysis
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)

            query = """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) as failed
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s
            """
            cursor.execute(query, (start_date, end_date))
            res = cursor.fetchone()
            
            total = res['total'] if res else 0
            failed = res['failed'] if res else 0
            failure_rate = round((failed / total * 100), 2) if total > 0 else 0.0
            
            return {
                "failure_rate_percent": failure_rate,
                "business_exception_rate_percent": 0.0,
                "system_exception_rate_percent": 0.0,
                "note": "Complete exception analysis requires data not currently in database."
            }
        except Error as e:
             logger.error(f"Error fetching exception analysis: {e}")
             return None
        finally:
            conn.close()

    def get_retry_metrics(self, start_date=None, end_date=None):
        """
        D. Retry & Rework Indicators
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)
            
            query = """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN retry_number > 0 THEN 1 ELSE 0 END) as retried_items,
                    AVG(retry_number) as avg_retries,
                    SUM(CASE WHEN retry_number > 0 AND status = 'Successful' THEN 1 ELSE 0 END) as successful_retries
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s
            """
            cursor.execute(query, (start_date, end_date))
            res = cursor.fetchone()
            
            total = res['total'] if res else 0
            retried = res['retried_items'] if res else 0
            avg_retry = float(res['avg_retries']) if res and res['avg_retries'] else 0.0
            success_retries = res['successful_retries'] if res else 0
            
            retry_rate = round((retried / total * 100), 2) if total > 0 else 0.0
            retry_success_rate = round((success_retries / retried * 100), 2) if retried > 0 else 0.0

            return {
                "retry_rate_percent": retry_rate,
                "avg_retry_count": avg_retry,
                "retry_success_rate_percent": retry_success_rate
            }
        except Error as e:
             logger.error(f"Error fetching retry metrics: {e}")
             return None
        finally:
            conn.close()

    def get_operational_risk(self, start_date=None, end_date=None):
        """
        E. Operational Risk Flags
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            
            orphan_query = """SELECT COUNT(*) as count FROM queue_items WHERE status = 'New' AND creation_time < NOW() - INTERVAL 48 HOUR"""
            
            zombie_query = """SELECT COUNT(*) as count FROM queue_items WHERE status = 'In Progress' AND start_processing < NOW() - INTERVAL 24 HOUR"""
            
            start_date, end_date = self._get_date_range(start_date, end_date)

            abandoned_query = """
                SELECT COUNT(*) as total, SUM(CASE WHEN status = 'Abandoned' THEN 1 ELSE 0 END) as abandoned
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s
            """
            
            cursor.execute(orphan_query)
            orphan_res = cursor.fetchone()
            
            cursor.execute(zombie_query)
            zombie_res = cursor.fetchone()
            
            cursor.execute(abandoned_query, (start_date, end_date))
            ab_res = cursor.fetchone()
            
            total = ab_res['total'] if ab_res else 0
            ab_count = ab_res['abandoned'] if ab_res else 0
            ab_rate = round((ab_count / total * 100), 2) if total > 0 else 0.0
            
            return {
                "orphan_items_count": orphan_res['count'] if orphan_res else 0,
                "zombie_items_count": zombie_res['count'] if zombie_res else 0,
                "abandoned_item_rate_percent": ab_rate
            }
            
        except Error as e:
             logger.error(f"Error fetching operational risk: {e}")
             return None
        finally:
            conn.close()

    def get_failures_by_queue(self, start_date=None, end_date=None):
        """
        G. Failures by Queue
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)
                
            query = """
                SELECT 
                    queue_definition_id, 
                    COUNT(*) as total_processed,
                    SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) as failure_count
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s
                GROUP BY queue_definition_id
                HAVING failure_count > 0
                ORDER BY failure_count DESC
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            cleaned = []
            for row in results:
                total = row['total_processed']
                fail_count = row['failure_count'] 
                fail_val = int(fail_count) if fail_count else 0
                rate = 0.0
                if total > 0:
                    rate = round((fail_val / total) * 100, 2)
                    
                cleaned.append({
                    "queue_definition_id": row['queue_definition_id'],
                    "failure_count": fail_val,
                    "failure_rate_percent": rate
                })
                
            return cleaned
        except Error as e:
             logger.error(f"Error fetching failures by queue: {e}")
             return None
        finally:
            conn.close()

    def get_recent_failures(self, limit=10):
        """
        H. Last 10 Failed Queue Items
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            query = f"""
                SELECT id, queue_definition_id, creation_time, end_processing as failure_time, retry_number, status
                FROM queue_items
                WHERE status = 'Failed'
                ORDER BY end_processing DESC
                LIMIT {limit}
            """
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                row['exception_type'] = 'Unknown'
                row['failure_reason'] = 'See raw output'
                
            return results
        except Error as e:
             logger.error(f"Error fetching recent failures: {e}")
             return None
        finally:
            conn.close()

    def get_top_failure_reasons(self, start_date=None, end_date=None):
        """
        F. Top Failure Reasons (Top 10)
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)

            query = """
                SELECT COUNT(*) as failure_count
                FROM queue_items
                WHERE status = 'Failed' AND end_processing BETWEEN %s AND %s
            """
            cursor.execute(query, (start_date, end_date))
            res = cursor.fetchone()
            count = res['failure_count'] if res else 0
            
            if count > 0:
                return [{
                    "failure_reason": "Generic Failure (Reason Not Logged)",
                    "failure_count": count,
                    "failure_percent": 100.0
                }]
            return []

        except Error as e:
             logger.error(f"Error fetching failure reasons: {e}")
             return None
        finally:
            conn.close()

    def get_failure_trend(self, start_date=None, end_date=None, interval='HOUR'):
        """
        Failure Trend
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)

            date_format = "%Y-%m-%d %H:00:00" 
            if interval.upper() == "DAY":
                date_format = "%Y-%m-%d"

            query = f"""
                SELECT DATE_FORMAT(end_processing, '{date_format}') as time_bucket, COUNT(*) as count
                FROM queue_items
                WHERE status = 'Failed' AND end_processing BETWEEN %s AND %s
                GROUP BY time_bucket
                ORDER BY time_bucket
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            trend = []
            for row in results:
                trend.append({
                    "time_bucket": row['time_bucket'],
                    "failure_count": row['count']
                })
            return trend
        except Error as e:
             logger.error(f"Error fetching failure trend: {e}")
             return None
        finally:
            conn.close()
