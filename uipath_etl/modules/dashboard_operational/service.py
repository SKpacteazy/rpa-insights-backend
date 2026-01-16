import logging
from datetime import datetime, timedelta
from shared.db import db
from shared.utils import get_dates_from_request
from mysql.connector import Error

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DashboardOpsService:
    def __init__(self):
        pass

    def get_connection(self):
        return db.get_connection()

    def _get_date_range(self, start_date, end_date):
        """Helper to set default dates to last 24h if None"""
        if not start_date:
            start_date = '2000-01-01 00:00:00'
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return start_date, end_date

    def get_queue_volume_snapshot(self, start_date=None, end_date=None):
        """
        A. Queue Volume Snapshot
        KPIs: Total, New, In Progress, Successful, Failed, Abandoned, Retried, Deleted
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)

            query = """
                SELECT status, COUNT(*) as count 
                FROM queue_items 
                WHERE creation_time BETWEEN %s AND %s 
                GROUP BY status
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            stats = {
                "Total Queue Items": 0, "New": 0, "In Progress": 0,
                "Successful": 0, "Failed": 0, "Abandoned": 0,
                "Retried": 0, "Deleted": 0
            }
            
            total = 0
            for row in results:
                status = row['status']
                count = row['count']
                if status == "InProgress":
                    status = "In Progress"
                stats[status] = count
                total += count
            
            stats["Total Queue Items"] = total
            return stats

        except Error as e:
            logger.error(f"Error fetching volume snapshot: {e}")
            return None
        finally:
            conn.close()

    def get_trend_analysis(self, start_date=None, end_date=None, interval="HOUR"):
        """
        B. Demand vs Completion Trend
        KPIs: Items Created, Items Completed over time
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

            created_query = f"""
                SELECT DATE_FORMAT(creation_time, '{date_format}') as time_bucket, COUNT(*) as count
                FROM queue_items
                WHERE creation_time BETWEEN %s AND %s
                GROUP BY time_bucket
                ORDER BY time_bucket
            """
            
            completed_query = f"""
                SELECT DATE_FORMAT(end_processing, '{date_format}') as time_bucket, COUNT(*) as count
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s
                GROUP BY time_bucket
                ORDER BY time_bucket
            """

            cursor.execute(created_query, (start_date, end_date))
            created_data = cursor.fetchall()

            cursor.execute(completed_query, (start_date, end_date))
            completed_data = cursor.fetchall()

            created_map = {row['time_bucket']: row['count'] for row in created_data}
            completed_map = {row['time_bucket']: row['count'] for row in completed_data}

            all_buckets = sorted(list(set(created_map.keys()) | set(completed_map.keys())))

            created_list = []
            completed_list = []
            backlog_list = []

            for bucket in all_buckets:
                c_val = created_map.get(bucket, 0)
                comp_val = completed_map.get(bucket, 0)
                
                created_list.append({"time_bucket": bucket, "count": c_val})
                completed_list.append({"time_bucket": bucket, "count": comp_val})
                backlog_list.append({"time_bucket": bucket, "count": c_val - comp_val})

            return {
                "created": created_list,
                "completed": completed_list,
                "backlog_trend": backlog_list
            }

        except Error as e:
            logger.error(f"Error fetching trend analysis: {e}")
            return None
        finally:
            conn.close()

    def get_status_distribution(self, start_date=None, end_date=None):
        """
        C. Queue Status Distribution
        Returns % by Status (New / In Progress / Success / Failed / Abandoned)
        """
        data = self.get_queue_volume_snapshot(start_date, end_date)
        if not data:
            return None
        
        if "InProgress" in data:
            data["In Progress"] = data.get("In Progress", 0) + data.pop("InProgress")

        if "Successful" in data:
            data["Success"] = data.pop("Successful")

        target_statuses = ["New", "In Progress", "Success", "Failed", "Abandoned"]
        total = data.get("Total Queue Items", 0)
        
        distribution = {}
        for status in target_statuses:
            count = data.get(status, 0)
            percentage = 0
            if total > 0:
                percentage = round((count / total) * 100, 2)
            distribution[status] = percentage

        return distribution

    def get_aging_metrics(self, start_date=None, end_date=None, threshold_hours=24):
        """
        D. Queue Aging & Wait Time
        KPIs: Avg Wait Time, Max Age, Items > Threshold
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)

            avg_wait_query = """
                SELECT AVG(TIMESTAMPDIFF(SECOND, creation_time, start_processing)) as avg_wait_seconds
                FROM queue_items
                WHERE start_processing BETWEEN %s AND %s
            """
            
            max_age_query = """
                SELECT MAX(TIMESTAMPDIFF(SECOND, creation_time, NOW())) as max_age_seconds
                FROM queue_items
                WHERE status = 'New'
            """
            
            threshold_seconds = threshold_hours * 3600
            threshold_query = f"""
                SELECT COUNT(*) as count
                FROM queue_items
                WHERE status = 'New' AND TIMESTAMPDIFF(SECOND, creation_time, NOW()) > {threshold_seconds}
            """

            buckets_query = """
                SELECT 
                    SUM(CASE WHEN age_sec <= 3600 THEN 1 ELSE 0 END) as bucket_0_1h,
                    SUM(CASE WHEN age_sec > 3600 AND age_sec <= 14400 THEN 1 ELSE 0 END) as bucket_1_4h,
                    SUM(CASE WHEN age_sec > 14400 AND age_sec <= 86400 THEN 1 ELSE 0 END) as bucket_4_24h,
                    SUM(CASE WHEN age_sec > 86400 THEN 1 ELSE 0 END) as bucket_24h_plus
                FROM (
                    SELECT TIMESTAMPDIFF(SECOND, creation_time, NOW()) as age_sec
                    FROM queue_items
                    WHERE status = 'New' AND creation_time BETWEEN %s AND %s
                ) as sub
            """

            cursor.execute(avg_wait_query, (start_date, end_date))
            avg_wait_res = cursor.fetchone()
            
            cursor.execute(max_age_query)
            max_age_res = cursor.fetchone()
            
            cursor.execute(threshold_query)
            threshold_res = cursor.fetchone()

            # Execute buckets query with date params
            cursor.execute(buckets_query, (start_date, end_date))
            buckets_res = cursor.fetchone()

            buckets = {}
            if buckets_res:
                buckets = {
                    "0-1h": int(buckets_res['bucket_0_1h']) if buckets_res['bucket_0_1h'] else 0,
                    "1-4h": int(buckets_res['bucket_1_4h']) if buckets_res['bucket_1_4h'] else 0,
                    "4-24h": int(buckets_res['bucket_4_24h']) if buckets_res['bucket_4_24h'] else 0,
                    "24h+": int(buckets_res['bucket_24h_plus']) if buckets_res['bucket_24h_plus'] else 0
                }

            return {
                "avg_queue_wait_seconds": float(avg_wait_res['avg_wait_seconds']) if avg_wait_res and avg_wait_res['avg_wait_seconds'] else 0,
                "max_queue_age_seconds": int(max_age_res['max_age_seconds']) if max_age_res and max_age_res['max_age_seconds'] else 0,
                "items_beyond_threshold": threshold_res['count'] if threshold_res else 0,
                "threshold_hours_used": threshold_hours,
                "aging_buckets": buckets
            }

        except Error as e:
            logger.error(f"Error fetching aging metrics: {e}")
            return None
        finally:
            conn.close()

    def get_processing_performance(self, start_date=None, end_date=None):
        """
        E. Processing Time Performance
        KPIs: Avg Processing Time, Median (approx), Trend
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(dictionary=True)
            start_date, end_date = self._get_date_range(start_date, end_date)

            avg_proc_query = """
                SELECT AVG(TIMESTAMPDIFF(SECOND, start_processing, end_processing)) as avg_proc_seconds
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s
            """
            
            median_query = """
                SELECT TIMESTAMPDIFF(SECOND, start_processing, end_processing) as duration
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s
            """

            trend_query = """
                SELECT DATE_FORMAT(end_processing, '%Y-%m-%d %H:00:00') as time_bucket, 
                       AVG(TIMESTAMPDIFF(SECOND, start_processing, end_processing)) as avg_seconds
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s
                GROUP BY time_bucket
                ORDER BY time_bucket
            """

            cursor.execute(avg_proc_query, (start_date, end_date))
            avg_res = cursor.fetchone()
            
            cursor.execute(median_query, (start_date, end_date))
            median_rows = cursor.fetchall()

            cursor.execute(trend_query, (start_date, end_date))
            trend_rows = cursor.fetchall()
            
            durations = [row['duration'] for row in median_rows if row['duration'] is not None]
            import statistics
            median_val = 0
            if durations:
                median_val = statistics.median(durations)

            trend_data = []
            for row in trend_rows:
                trend_data.append({
                    "time_bucket": row['time_bucket'],
                    "avg_processing_time_seconds": float(row['avg_seconds']) if row['avg_seconds'] else 0
                })

            return {
                "avg_processing_time_seconds": float(avg_res['avg_proc_seconds']) if avg_res and avg_res['avg_proc_seconds'] else 0,
                "median_processing_time_seconds": median_val,
                "processing_time_trend": trend_data
            }

        except Error as e:
            logger.error(f"Error fetching performance metrics: {e}")
            return None
        finally:
            conn.close()

    def get_benchmarking(self, start_date=None, end_date=None):
        """
        F. Queue Performance Benchmarking
        Metrics per Queue (Queue Definition ID or Name)
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
                    COUNT(*) as items_processed,
                    AVG(TIMESTAMPDIFF(SECOND, creation_time, start_processing)) as avg_wait_seconds,
                    AVG(TIMESTAMPDIFF(SECOND, start_processing, end_processing)) as avg_proc_seconds,
                    SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) as fail_count,
                    SUM(CASE WHEN status = 'Retried' THEN 1 ELSE 0 END) as retry_count
                FROM queue_items
                WHERE end_processing BETWEEN %s AND %s
                GROUP BY queue_definition_id
            """
            
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            cleaned = []
            for row in results:
                total = row['items_processed']
                fail_rate = (row['fail_count'] / total * 100) if total > 0 else 0
                retry_rate = (row['retry_count'] / total * 100) if total > 0 else 0
                
                cleaned.append({
                    "queue_definition_id": row['queue_definition_id'],
                    "items_processed": total,
                    "avg_wait_seconds": float(row['avg_wait_seconds']) if row['avg_wait_seconds'] else 0,
                    "avg_proc_seconds": float(row['avg_proc_seconds']) if row['avg_proc_seconds'] else 0,
                    "failure_rate_percent": round(fail_rate, 2),
                    "retry_rate_percent": round(retry_rate, 2)
                })
                
            return cleaned

        except Error as e:
            logger.error(f"Error fetching benchmarking: {e}")
            return None
        finally:
            conn.close()
