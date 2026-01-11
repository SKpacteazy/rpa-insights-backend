import logging
from datetime import datetime, timedelta
from shared.db import db
from mysql.connector import Error

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DashboardJobsService:
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

    def get_jobs_snapshot(self, start_date=None, end_date=None):
        """
        A. Job Execution Snapshot (At-a-Glance Health)
        KPIs: Total, Pending, Running, Successful, Failed, Stopped
        """
        conn = self.get_connection()
        if not conn: return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)

            query = """
                SELECT state, COUNT(*) as count
                FROM jobs
                WHERE creation_time BETWEEN %s AND %s
                GROUP BY state
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            stats = {
                "total_jobs": 0, "pending": 0, "running": 0,
                "successful": 0, "failed": 0, "stopped": 0
            }
            
            total = 0
            for row in results:
                state = row['state']
                count = row['count']
                total += count
                
                if state == 'Pending': stats['pending'] += count
                elif state == 'Running': stats['running'] += count
                elif state == 'Successful': stats['successful'] += count
                elif state == 'Faulted': stats['failed'] += count
                elif state == 'Stopped': stats['stopped'] += count
            
            stats['total_jobs'] = total
            return stats

        except Error as e:
            logger.error(f"Error fetching jobs snapshot: {e}")
            return None
        finally:
            conn.close()

    def get_jobs_distribution(self, start_date=None, end_date=None):
        """
        B. Job State Distribution (Flow Health)
        Returns % by State
        """
        snapshot = self.get_jobs_snapshot(start_date, end_date)
        if not snapshot: return None
        
        total = snapshot['total_jobs']
        distribution = {}
        
        for key in ['pending', 'running', 'successful', 'failed', 'stopped']:
            count = snapshot.get(key, 0)
            pct = 0.0
            if total > 0:
                pct = round((count / total) * 100, 2)
            distribution[f"{key}_percent"] = pct
            
        return distribution

    def get_jobs_volume_trend(self, start_date=None, end_date=None, interval="HOUR"):
        """
        C. Job Throughput & Volume Trend
        KPIs: Created, Started, Completed, Failed over time
        """
        conn = self.get_connection()
        if not conn: return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)
            
            date_format = "%Y-%m-%d %H:00:00" 
            if interval.upper() == "DAY": date_format = "%Y-%m-%d"

            created_query = f"""
                SELECT DATE_FORMAT(creation_time, '{date_format}') as time_bucket, COUNT(*) as count
                FROM jobs WHERE creation_time BETWEEN %s AND %s GROUP BY time_bucket
            """
            started_query = f"""
                SELECT DATE_FORMAT(start_time, '{date_format}') as time_bucket, COUNT(*) as count
                FROM jobs WHERE start_time BETWEEN %s AND %s GROUP BY time_bucket
            """
            completed_query = f"""
                SELECT DATE_FORMAT(end_time, '{date_format}') as time_bucket, COUNT(*) as count
                FROM jobs WHERE end_time BETWEEN %s AND %s AND state = 'Successful' GROUP BY time_bucket
            """
            failed_query = f"""
                SELECT DATE_FORMAT(end_time, '{date_format}') as time_bucket, COUNT(*) as count
                FROM jobs WHERE end_time BETWEEN %s AND %s AND state = 'Faulted' GROUP BY time_bucket
            """
            
            cursor.execute(created_query, (start_date, end_date))
            created_res = cursor.fetchall()
            cursor.execute(started_query, (start_date, end_date))
            started_res = cursor.fetchall()
            cursor.execute(completed_query, (start_date, end_date))
            completed_res = cursor.fetchall()
            cursor.execute(failed_query, (start_date, end_date))
            failed_res = cursor.fetchall()
            
            return {
                "jobs_created": created_res,
                "jobs_started": started_res,
                "jobs_completed": completed_res,
                "jobs_failed": failed_res
            }

        except Error as e:
            logger.error(f"Error fetching jobs trend: {e}")
            return None
        finally:
            conn.close()

    def get_jobs_performance(self, start_date=None, end_date=None):
        """
        D. Job Execution Time Performance
        KPIs: Avg, Median, Max Execution Time, Trend
        """
        conn = self.get_connection()
        if not conn: return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)

            avg_query = """
                SELECT 
                    AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) as avg_exec_seconds,
                    MAX(TIMESTAMPDIFF(SECOND, start_time, end_time)) as max_exec_seconds
                FROM jobs
                WHERE end_time BETWEEN %s AND %s AND state IN ('Successful', 'Faulted')
            """
            
            median_query = """
                SELECT TIMESTAMPDIFF(SECOND, start_time, end_time) as duration
                FROM jobs
                WHERE end_time BETWEEN %s AND %s AND state IN ('Successful', 'Faulted')
            """
            
            trend_query = """
                SELECT DATE_FORMAT(end_time, '%Y-%m-%d %H:00:00') as time_bucket,
                       AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) as avg_exec_seconds
                FROM jobs
                WHERE end_time BETWEEN %s AND %s AND state IN ('Successful', 'Faulted')
                GROUP BY time_bucket ORDER BY time_bucket
            """

            cursor.execute(avg_query, (start_date, end_date))
            avg_res = cursor.fetchone()
            
            cursor.execute(median_query, (start_date, end_date))
            median_rows = cursor.fetchall()
            
            cursor.execute(trend_query, (start_date, end_date))
            trend_res = cursor.fetchall()
            
            import statistics
            durations = [r['duration'] for r in median_rows if r['duration'] is not None]
            median_val = statistics.median(durations) if durations else 0

            return {
                "avg_execution_seconds": float(avg_res['avg_exec_seconds']) if avg_res and avg_res['avg_exec_seconds'] else 0,
                "max_execution_seconds": int(avg_res['max_exec_seconds']) if avg_res and avg_res['max_exec_seconds'] else 0,
                "median_execution_seconds": median_val,
                "execution_trend": trend_res
            }

        except Error as e:
            logger.error(f"Error fetching jobs performance: {e}")
            return None
        finally:
            conn.close()

    def get_jobs_reliability(self, start_date=None, end_date=None):
        """
        E. Failure & Stability Analysis
        KPIs: Failure Rate %, Failure Count
        """
        conn = self.get_connection()
        if not conn: return None
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)
            
            query = """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN state = 'Faulted' THEN 1 ELSE 0 END) as failed_count
                FROM jobs
                WHERE end_time BETWEEN %s AND %s
            """
            cursor.execute(query, (start_date, end_date))
            res = cursor.fetchone()
            
            total = res['total'] if res else 0
            failed = res['failed_count'] if res else 0
            rate = round((failed / total * 100), 2) if total > 0 else 0.0
            
            return {
                "job_failure_rate_percent": rate,
                "job_failure_count": int(failed)
            }
        except Error as e:
            logger.error(f"Error fetching reliability: {e}")
            return None
        finally:
            conn.close()

    def get_jobs_failure_reasons(self, start_date=None, end_date=None):
        """
        F. Failure Reasons & Error Intelligence
        """
        conn = self.get_connection()
        if not conn: return None
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)
            
            query = """
                SELECT info, error_code, COUNT(*) as count
                FROM jobs
                WHERE state = 'Faulted' AND end_time BETWEEN %s AND %s
                GROUP BY info, error_code
                ORDER BY count DESC
                LIMIT 10
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            cleaned = []
            for row in results:
                reason = "Unknown"
                if row['error_code']: reason = str(row['error_code'])
                elif row['info']: reason = str(row['info'])[:50] 
                
                cleaned.append({
                    "failure_reason": reason,
                    "count": row['count']
                })
            return cleaned

        except Error as e:
            logger.error(f"Error fetching failure reasons: {e}")
            return None
        finally:
            conn.close()

    def get_jobs_by_release(self, start_date=None, end_date=None):
        """
        G. Jobs by Release (Process Health)
        """
        conn = self.get_connection()
        if not conn: return None
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)
            
            query = """
                SELECT 
                    release_name,
                    COUNT(*) as job_count,
                    SUM(CASE WHEN state = 'Faulted' THEN 1 ELSE 0 END) as fail_count,
                    AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) as avg_duration_sec,
                    MAX(CASE WHEN state = 'Faulted' THEN end_time ELSE NULL END) as last_failure
                FROM jobs
                WHERE creation_time BETWEEN %s AND %s
                GROUP BY release_name
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            cleaned = []
            for row in results:
                total = row['job_count']
                fails = row['fail_count']
                rate = round((fails / total * 100), 2) if total > 0 else 0.0
                
                cleaned.append({
                    "release_name": row['release_name'],
                    "job_count": total,
                    "failure_rate_percent": rate,
                    "avg_execution_seconds": float(row['avg_duration_sec']) if row['avg_duration_sec'] else 0,
                    "last_failure_time": str(row['last_failure']) if row['last_failure'] else None
                })
            return cleaned

        except Error as e:
            logger.error(f"Error fetching jobs by release: {e}")
            return None
        finally:
            conn.close()

    def get_jobs_trigger_analysis(self, start_date=None, end_date=None):
        """
        H. Job Trigger & Source Analysis
        """
        conn = self.get_connection()
        if not conn: return None
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)
            
            source_query = """
                SELECT source, COUNT(*) as count 
                FROM jobs WHERE creation_time BETWEEN %s AND %s GROUP BY source
            """
            type_query = """
                SELECT type, COUNT(*) as count
                FROM jobs WHERE creation_time BETWEEN %s AND %s GROUP BY type
            """
            
            cursor.execute(source_query, (start_date, end_date))
            source_res = cursor.fetchall()
            
            cursor.execute(type_query, (start_date, end_date))
            type_res = cursor.fetchall()
            
            return {
                "by_source": source_res,
                "by_type": type_res
            }

        except Error as e:
            logger.error(f"Error fetching trigger analysis: {e}")
            return None
        finally:
            conn.close()

    def get_jobs_risk_flags(self, threshold_hours=24):
        """
        I. Long-Running & Stuck Jobs
        """
        conn = self.get_connection()
        if not conn: return None
        try:
            cursor = conn.cursor(dictionary=True)
            
            threshold_sec = threshold_hours * 3600
            
            long_running_query = f"""
                SELECT COUNT(*) as count FROM jobs 
                WHERE state = 'Running' AND TIMESTAMPDIFF(SECOND, start_time, NOW()) > {threshold_sec}
            """
            
            pending_long_query = f"""
                SELECT COUNT(*) as count FROM jobs 
                WHERE state = 'Pending' AND TIMESTAMPDIFF(SECOND, creation_time, NOW()) > {threshold_sec}
            """
            
            cursor.execute(long_running_query)
            long_res = cursor.fetchone()
            
            cursor.execute(pending_long_query)
            pending_res = cursor.fetchone()
            
            return {
                "jobs_running_beyond_threshold": long_res['count'] if long_res else 0,
                "jobs_pending_beyond_threshold": pending_res['count'] if pending_res else 0,
                "zombie_jobs_count": long_res['count'] if long_res else 0 
            }

        except Error as e:
            logger.error(f"Error fetching job risks: {e}")
            return None
        finally:
            conn.close()

    def get_recent_failed_jobs(self, limit=10):
        """
        J. Last 10 Failed Jobs
        """
        conn = self.get_connection()
        if not conn: return None
        try:
            cursor = conn.cursor(dictionary=True)
            
            query = f"""
                SELECT id, release_name, state, start_time, end_time, 
                       TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_sec,
                       info, error_code, source, host_machine_name
                FROM jobs
                WHERE state = 'Faulted'
                ORDER BY end_time DESC
                LIMIT {limit}
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            cleaned = []
            for row in results:
                row['formatted_duration'] = str(timedelta(seconds=row['duration_sec'])) if row['duration_sec'] else None
                cleaned.append(row)
                
            return cleaned

        except Error as e:
            logger.error(f"Error fetching recent failed jobs: {e}")
            return None
        finally:
            conn.close()
