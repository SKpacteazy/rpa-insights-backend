import requests
import json
import logging
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import uuid

try:
    # If running locally or in Docker with PYTHONPATH set to current dir
    from shared import config
except ImportError:
    # If imported as a package (e.g., from Airflow DAGs)
    try:
        from uipath_etl.shared import config
    except ImportError:
        from .shared import config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UiPathClient:
    def __init__(self):
        self.base_url = None
        self.client_id = None
        self.client_secret = None
        self.org = None
        self.tenant = None
        self.scope = None
        self.access_token = None
        self.db_conn = None

    def connect_db(self):
        """Connect to MySQL Database"""
        try:
            # First connect to server to ensure DB exists
            conn = mysql.connector.connect(
                host=config.MYSQL_HOST,
                user=config.MYSQL_USER,
                password=config.MYSQL_PASSWORD
            )
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.MYSQL_DATABASE}")
                logger.info(f"Database {config.MYSQL_DATABASE} checked/created")
                cursor.close()
                conn.close() # Close server connection

            # Now connect to specific DB
            self.db_conn = mysql.connector.connect(
                host=config.MYSQL_HOST,
                user=config.MYSQL_USER,
                password=config.MYSQL_PASSWORD,
                database=config.MYSQL_DATABASE
            )
            logger.info(f"Connected to database: {config.MYSQL_DATABASE}")
            
            # Load configuration after connection
            self.load_config()
            
            return self.db_conn
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            return None

    def load_config(self):
        """Load UiPath configuration from database"""
        if not self.db_conn:
            logger.error("Cannot load config: Database connection not established")
            return

        try:
            cursor = self.db_conn.cursor(dictionary=True)
            cursor.execute("SELECT base_url, client_id, client_secret, org, tenant, scope FROM uipath_configuration ORDER BY id DESC LIMIT 1")
            result = cursor.fetchone()
            
            if result:
                self.base_url = result['base_url']
                self.client_id = result['client_id']
                self.client_secret = result['client_secret']
                self.org = result['org']
                self.tenant = result['tenant']
                self.scope = result['scope']
                logger.info("Configuration loaded from database")
            else:
                logger.error("No configuration found in uipath_configuration table")
                
            cursor.close()
        except Error as e:
            logger.error(f"Error loading config from DB: {e}")

    def create_table(self):
        """Create queue_items table if not exists"""
        if not self.db_conn:
            return

        query = """
        CREATE TABLE IF NOT EXISTS queue_items (
            id BIGINT PRIMARY KEY,
            queue_definition_id INT,
            folder_id INT,
            `key` TEXT,
            status TEXT,
            reference TEXT,
            priority TEXT,
            defer_date DATETIME,
            start_processing DATETIME,
            end_processing DATETIME,
            seconds_prev_attempts INT,
            retry_number INT,
            creation_time DATETIME,
            org_unit_id INT,
            run_duration VARCHAR(50),
            waiting_duration VARCHAR(50),
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query)
            self.db_conn.commit()
            logger.info("Table 'queue_items' checked/created")
        except Error as e:
            logger.error(f"Error creating table: {e}")

    def authenticate(self):
        """Authenticate with UiPath Identity Server to get Access Token"""
        auth_url = f"{self.base_url}/identity_/connect/token"
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': self.scope
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        
        try:
            response = requests.post(auth_url, data=payload, headers=headers)
            response.raise_for_status()
            self.access_token = response.json().get('access_token')
            logger.info("Authentication successful")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {e}")
            if response:
                logger.error(f"Response: {response.text}")
            raise

    def _get_headers(self, org_unit_id=None):
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        if org_unit_id:
            headers['X-UIPATH-OrganizationUnitId'] = str(org_unit_id)
        return headers

    def get_folders(self):
        """Retrieve all folders"""
        url = f"{self.base_url}/{self.org}/{self.tenant}/odata/Folders"
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            folders = response.json().get('value', [])
            logger.info(f"Retrieved {len(folders)} folders")
            return folders
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get folders: {e}")
            raise



    def get_queue_items(self, folder_id):
        """Retrieve queue items with specific OData filters (Replaces original unfiltered method)"""
        url = f"{self.base_url}/{self.org}/{self.tenant}/odata/queueitems"
        params = {
            "$orderby": "Id desc",
            "$top": 100,
            "$skip": 0,
            "$filter": "(CreationTime gt 2025-12-15 or StartProcessing gt 2025-12-15 or EndProcessing lt 2025-12-15)"
        }
        try:
            logger.info(f"Fetching filtered items for folder {folder_id} with params: {params}")
            response = requests.get(url, headers=self._get_headers(org_unit_id=folder_id), params=params)
            response.raise_for_status()
            items = response.json().get('value', [])
            logger.info(f"Retrieved {len(items)} filtered queue items from folder {folder_id}")
            return items
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get filtered queue items for folder {folder_id}: {e}")
            return []

    def get_filtered_queue_items(self, folder_id):
        """Retrieve queue items with specific OData filters"""
        url = f"{self.base_url}/{self.org}/{self.tenant}/odata/queueitems"
        params = {
            "$orderby": "Id desc",
            "$top": 100,
            "$skip": 0,
            "$filter": "(CreationTime gt 2025-12-15 or StartProcessing gt 2025-12-15 or EndProcessing lt 2025-12-15)"
        }
        try:
            logger.info(f"Fetching filtered items for folder {folder_id} with params: {params}")
            response = requests.get(url, headers=self._get_headers(org_unit_id=folder_id), params=params)
            response.raise_for_status()
            items = response.json().get('value', [])
            logger.info(f"Retrieved {len(items)} filtered queue items from folder {folder_id}")
            return items
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get filtered queue items for folder {folder_id}: {e}")
            return []

    def get_jobs(self, folder_id):
        """Retrieve jobs for a specific folder"""
        url = f"{self.base_url}/{self.org}/{self.tenant}/odata/Jobs"
        params = {
            "$orderby": "CreationTime desc",
            "$top": 1000
        }
        try:
            logger.info(f"Fetching jobs for folder {folder_id}")
            response = requests.get(url, headers=self._get_headers(org_unit_id=folder_id), params=params)
            response.raise_for_status()
            jobs = response.json().get('value', [])
            logger.info(f"Retrieved {len(jobs)} jobs from folder {folder_id}")
            return jobs
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get jobs for folder {folder_id}: {e}")
            return []

    def calculate_duration(self, start, end):
        """Calculate duration between two timestamp strings"""
        if not start or not end:
            # logger.debug(f"Duration calc skipped: start={start}, end={end}")
            return None
        try:
            # UiPath dates usually isoformat like: 2025-11-18T14:00:44.18Z
            # Simple Replace Z with +00:00 for python fromisoformat compatibility if needed
            # But standard ISO 8601 often parsed ok.
            # Let's clean Z if present
            s_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
            e_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
            # Calculate duration
            duration_pool = e_dt - s_dt
            
            # Format: remove microseconds
            # total_seconds = int(duration_pool.total_seconds())
            # hours, remainder = divmod(total_seconds, 3600)
            # minutes, seconds = divmod(remainder, 60)
            # return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
            
            # Or just str() but split off microseconds if user finds them "incorrect"?
            # The user saw "29 days, 19:58:20.397580".
            # Let's clean it up.
            return str(duration_pool).split('.')[0]
        except ValueError as e:
            logger.warning(f"Duration calc error: {e}")
            return None

    def clean_date(self, date_str):
        """Convert ISO string to MySQL compatible datetime format"""
        if not date_str:
            return None
        try:
            # Replace Z with +00:00 to handle UTC in fromisoformat
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            return None

    def parse_dt(self, date_str):
        """Helper to parse ISO date string to datetime object"""
        if not date_str:
            return None
        try:
            # Parse as aware, then strip tzinfo to match datetime.utcnow() (which is naive)
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.replace(tzinfo=None)
        except ValueError:
            return None

    def transform_data(self, queue_items, folder_id):
        """Filter and format queue items as requested"""
        transformed = []
        
        for item in queue_items:
            # Reverting: Get Start/End Processing from Data
            start_proc = item.get("StartProcessing")
            end_proc = item.get("EndProcessing")
            creation_str = item.get("CreationTime")
            defer_date = item.get("DeferDate")
            
            # Parse timestamps
            s_dt = self.parse_dt(start_proc)
            e_dt = self.parse_dt(end_proc)
            c_dt = self.parse_dt(creation_str)
            
            # 3. Waiting Duration = Start Processing - Creation Time
            # "waiting_duration => start_processing - creation_time"
            waiting_duration = None
            if s_dt and c_dt:
                duration_pool = s_dt - c_dt
                waiting_duration = str(duration_pool).split('.')[0]
            
            # 5. Run Duration = End Processing - Start Processing
            # "run_duration = > end_processing - start_processing"
            run_duration = None
            if s_dt and e_dt:
                run_duration = str(e_dt - s_dt).split('.')[0]
            
            mapped_item = {
                "id": item.get("Id"),
                "queue_definition_id": item.get("QueueDefinitionId"),
                "folder_id": folder_id,
                "key": item.get("Key"),
                "status": item.get("Status"),
                "reference": item.get("Reference"),
                "priority": item.get("Priority"),
                "defer_date": self.clean_date(defer_date),
                "start_processing": self.clean_date(start_proc),
                "end_processing": self.clean_date(end_proc),
                "seconds_prev_attempts": item.get("SecondsInPreviousAttempts"),
                "retry_number": item.get("RetryNumber"),
                "creation_time": self.clean_date(creation_str),
                "org_unit_id": item.get("OrganizationUnitId"),
                "run_duration": run_duration,
                "waiting_duration": waiting_duration
            }
            transformed.append(mapped_item)
        return transformed



    def transform_jobs(self, jobs):
        """Map jobs API response to DB schema"""
        transformed = []
        for job in jobs:
            
            # Helper for booleans: True/False -> 1/0
            def to_int_bool(val):
                if val is None: return 0 # Default 0 as per TINYINT NOT NULL usually (except nullable ones)
                return 1 if val else 0

            # JSON fields
            def to_json(val):
                return json.dumps(val) if val is not None else None

            mapped = {
                "id": str(uuid.uuid4()), # New Schema: id is generated UUID
                "folder_id": job.get("OrganizationUnitId"), # New Schema: folder_id exists (mapped from OrgUnitId)
                "key_uuid": job.get("Key"), # Redundant but kept
                "folder_key": job.get("FolderKey"),
                "start_time": self.clean_date(job.get("StartTime")),
                "end_time": self.clean_date(job.get("EndTime")),
                "state": job.get("State"),
                "sub_state": job.get("SubState"),
                "job_priority": job.get("JobPriority"),
                "specific_priority_value": job.get("SpecificPriorityValue"),
                "resource_overwrites": to_json(job.get("ResourceOverwrites")),
                "source": job.get("Source"),
                "source_type": job.get("SourceType"),
                "batch_execution_key": job.get("BatchExecutionKey"),
                "info": job.get("Info"),
                "creation_time": self.clean_date(job.get("CreationTime")),
                "starting_schedule_id": job.get("StartingScheduleId"),
                "release_name": job.get("ReleaseName"),
                "type": job.get("Type"),
                "input_arguments": to_json(job.get("InputArguments")),
                "input_file": job.get("InputFile"),
                "environment_variables": job.get("EnvironmentVariables"),
                "output_arguments": to_json(job.get("OutputArguments")),
                "output_file": job.get("OutputFile"),
                "host_machine_name": job.get("HostMachineName"),
                "has_media_recorded": to_int_bool(job.get("HasMediaRecorded")),
                "has_video_recorded": to_int_bool(job.get("HasVideoRecorded")),
                "persistence_id": job.get("PersistenceId"),
                "resume_version": job.get("ResumeVersion"),
                "stop_strategy": job.get("StopStrategy"),
                "runtime_type": job.get("RuntimeType"),
                "requires_user_interaction": to_int_bool(job.get("RequiresUserInteraction")),
                "release_version_id": job.get("ReleaseVersionId"),
                "entry_point_path": job.get("EntryPointPath"),
                "organization_unit_id": job.get("OrganizationUnitId"),
                "organization_unit_fqn": job.get("OrganizationUnitFullyQualifiedName"),
                "reference": job.get("Reference"),
                "process_type": job.get("ProcessType"),
                "target_runtime": job.get("TargetRuntime"),
                "profiling_options": to_json(job.get("ProfilingOptions")),
                "resume_on_same_context": to_int_bool(job.get("ResumeOnSameContext")),
                "local_system_account": job.get("LocalSystemAccount"),
                "orchestrator_user_identity": job.get("OrchestratorUserIdentity"),
                "remote_control_access": job.get("RemoteControlAccess"),
                "starting_trigger_id": job.get("StartingTriggerId"),
                "max_expected_running_time_seconds": job.get("MaxExpectedRunningTimeSeconds"),
                "serverless_job_type": job.get("ServerlessJobType"),
                "parent_job_key": job.get("ParentJobKey"),
                "resume_time": self.clean_date(job.get("ResumeTime")),
                "last_modification_time": self.clean_date(job.get("LastModificationTime")),
                "error_code": job.get("ErrorCode"),
                "fps_properties": to_json(job.get("FpsProperties")),
                "trace_id": job.get("TraceId"),
                "parent_span_id": job.get("ParentSpanId"),
                "root_span_id": job.get("RootSpanId"),
                "parent_context": to_json(job.get("ParentContext")),
                "project_key": job.get("ProjectKey"),
                "creator_user_key": job.get("CreatorUserKey"),
                "parent_operation_id": job.get("ParentOperationId"),
                "enable_autopilot_healing": to_int_bool(job.get("EnableAutopilotHealing")),
                "fps_context": to_json(job.get("FpsContext")),
                "auto_heal_status": job.get("AutoHealStatus"),
                "autopilot_for_robots": job.get("AutopilotForRobots")
            }
            transformed.append(mapped)
        return transformed

    def get_record_count(self):
        """Get total count of records in queue_items"""
        if not self.db_conn:
            return 0
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM queue_items")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Error as e:
            logger.error(f"Error checking record count: {e}")
            return 0

    def insert_data(self, data):
        """Insert data into MySQL"""
        if not self.db_conn or not data:
            return
        
        logger.info(f"Attempting to insert/update {len(data)} items...")

        query = """
        INSERT INTO queue_items 
        (id, queue_definition_id, folder_id, `key`, status, reference, priority, defer_date, 
        start_processing, end_processing, seconds_prev_attempts, retry_number, creation_time, 
        org_unit_id, run_duration, waiting_duration)
        VALUES (
            %(id)s, %(queue_definition_id)s, %(folder_id)s, %(key)s, %(status)s, %(reference)s, 
            %(priority)s, %(defer_date)s, %(start_processing)s, %(end_processing)s, 
            %(seconds_prev_attempts)s, %(retry_number)s, %(creation_time)s, %(org_unit_id)s, 
            %(run_duration)s, %(waiting_duration)s
        )
        ON DUPLICATE KEY UPDATE
        status = VALUES(status),
        start_processing = VALUES(start_processing),
        end_processing = VALUES(end_processing),
        run_duration = VALUES(run_duration),
        waiting_duration = VALUES(waiting_duration)
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.executemany(query, data)
            self.db_conn.commit()
            logger.info(f"Inserted/Updated {cursor.rowcount} records")
        except Error as e:
            logger.error(f"Error inserting data: {e}")

    def insert_jobs(self, data):
        """Insert jobs data into MySQL"""
        if not self.db_conn or not data:
            return
        
        logger.info(f"Attempting to insert/update {len(data)} jobs...")

        # Columns
        columns = [
            "id", "folder_id", "key_uuid", "folder_key", "start_time", "end_time", "state", "sub_state",
            "job_priority", "specific_priority_value", "resource_overwrites", "source", "source_type",
            "batch_execution_key", "info", "creation_time", "starting_schedule_id", "release_name",
            "type", "input_arguments", "input_file", "environment_variables", "output_arguments",
            "output_file", "host_machine_name", "has_media_recorded", "has_video_recorded",
            "persistence_id", "resume_version", "stop_strategy", "runtime_type", "requires_user_interaction",
            "release_version_id", "entry_point_path", "organization_unit_id", "organization_unit_fqn",
            "reference", "process_type", "target_runtime", "profiling_options", "resume_on_same_context",
            "local_system_account", "orchestrator_user_identity", "remote_control_access",
            "starting_trigger_id", "max_expected_running_time_seconds", "serverless_job_type",
            "parent_job_key", "resume_time", "last_modification_time", "error_code", "fps_properties",
            "trace_id", "parent_span_id", "root_span_id", "parent_context", "project_key",
            "creator_user_key", "parent_operation_id", "enable_autopilot_healing", "fps_context",
            "auto_heal_status", "autopilot_for_robots"
        ]
        
        placeholders = ", ".join([f"%({col})s" for col in columns])
        col_names = ", ".join([f"`{col}`" for col in columns]) # Backtick all to be safe (e.g. `type` is keyword)
        
        # ON DUPLICATE UPDATE: Update every field that might change
        update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in columns if col != 'id']) # Skip ID if PK

        query = f"INSERT INTO jobs ({col_names}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
        
        try:
            cursor = self.db_conn.cursor()
            cursor.executemany(query, data)
            self.db_conn.commit()
            logger.info(f"Inserted/Updated {cursor.rowcount} job records")
        except Error as e:
            logger.error(f"Error inserting jobs: {e}")

    def run_etl(self):
        """Main ETL process"""
        if not all([self.client_id, self.client_secret, self.org, self.tenant]):
            logger.error("Missing configuration. Please check environment variables.")
            return

        # 1. Connect DB
        self.connect_db()
        self.create_table()
        
        initial_count = self.get_record_count()
        logger.info(f"Initial record count in DB: {initial_count}")

        # 2. Authenticate
        self.authenticate()
        
        folders = self.get_folders()
        all_queue_data = []

        for folder in folders:
            folder_id = folder.get('Id')
            folder_name = folder.get('DisplayName')
            logger.info(f"Processing folder: {folder_name} (ID: {folder_id})")
            
            queue_items = self.get_queue_items(folder_id)
            if queue_items:
                transformed_items = self.transform_data(queue_items, folder_id)
                self.insert_data(transformed_items) # Insert per folder
                all_queue_data.extend(transformed_items)
        
        final_count = self.get_record_count()
        logger.info(f"Final record count in DB: {final_count}")
        
        # Close connection
        if self.db_conn and self.db_conn.is_connected():
            self.db_conn.close()

        final_output = {"value": all_queue_data}
        return final_output

    def run_update_job(self):
        """Task specifically for updating filtered items"""
        if not all([self.client_id, self.client_secret, self.org, self.tenant]):
            logger.error("Missing configuration. Please check environment variables.")
            return

        # 1. Connect DB
        self.connect_db()
        self.create_table()
        
        # 2. Authenticate
        self.authenticate()
        
        folders = self.get_folders()
        all_queue_data = []

        for folder in folders:
            folder_id = folder.get('Id')
            folder_name = folder.get('DisplayName')
            logger.info(f"Update Job - Processing folder: {folder_name} (ID: {folder_id})")
            
            queue_items = self.get_filtered_queue_items(folder_id)
            if queue_items:
                transformed_items = self.transform_data(queue_items, folder_id)
                self.insert_data(transformed_items) # Upsert
                all_queue_data.extend(transformed_items)
        
        if self.db_conn and self.db_conn.is_connected():
            self.db_conn.close()

        logger.info(f"Update Job Finished. Processed {len(all_queue_data)} specific items.")
        return {"value": all_queue_data}

    def run_jobs_etl(self):
        """Task specifically for fetching and updating Jobs"""
        if not all([self.client_id, self.client_secret, self.org, self.tenant]):
            logger.error("Missing configuration. Please check environment variables.")
            return

        # 1. Connect DB
        self.connect_db()
        # Assumes jobs table already exists as per user request
        
        # 2. Authenticate
        self.authenticate()
        
        folders = self.get_folders()
        all_jobs_data = []

        for folder in folders:
            folder_id = folder.get('Id')
            folder_name = folder.get('DisplayName')
            logger.info(f"Jobs ETL - Processing folder: {folder_name} (ID: {folder_id})")
            
            jobs = self.get_jobs(folder_id)
            if jobs:
                transformed_jobs = self.transform_jobs(jobs)
                self.insert_jobs(transformed_jobs)
                all_jobs_data.extend(transformed_jobs)
        
        if self.db_conn and self.db_conn.is_connected():
            self.db_conn.close()

        logger.info(f"Jobs ETL Finished. Processed {len(all_jobs_data)} jobs.")
        return {"value": all_jobs_data}

if __name__ == "__main__":
    import sys
    client = UiPathClient()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--update":
        print("Starting Incremental Update Job...")
        client.run_update_job()
    else:
        print("Starting Full ETL Job...")
        client.run_etl()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--jobs":
        print("Starting Jobs ETL Job...")
        client.run_jobs_etl()

