"""
Singleton Lock for Spark Job Definition

Provides a distributed lock mechanism to prevent concurrent runs of the same
Spark Job Definition in Microsoft Fabric.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone, timedelta

import notebookutils

logger = logging.getLogger(__name__)


class SingletonJobLock:
    """
    Distributed lock mechanism to prevent concurrent runs of the Spark Job Definition.
    Uses atomic file creation in OneLake to ensure only one instance runs at a time.
    Includes heartbeat mechanism to detect and recover from stale locks.
    """
    
    # How often the heartbeat should be updated (seconds)
    HEARTBEAT_INTERVAL = 60
    # Lock is considered stale if heartbeat is older than this (seconds)
    STALE_THRESHOLD = 300  # 5 minutes
    
    def __init__(self, lock_path: str, job_name: str = "stream_bronze_and_silver"):
        self.lock_path = lock_path
        self.job_name = job_name
        self.lock_acquired = False
        self.instance_id = str(uuid.uuid4())
        self._heartbeat_thread = None
        self._stop_heartbeat = False
        
    def _file_exists(self) -> bool:
        """Check if lock file exists."""
        try:
            return notebookutils.fs.exists(self.lock_path)
        except Exception as e:
            logger.warning(f"Failed to check if lock file exists: {e}")
            # Try alternative method - attempt to get file info
            try:
                notebookutils.fs.head(self.lock_path, max_bytes=1)
                return True
            except Exception:
                return False

    def _read_lock_file(self) -> dict | None:
        """Read existing lock file if it exists."""
        try:
            content = notebookutils.fs.head(self.lock_path, max_bytes=4096)
            logger.debug(f"Lock file content: {content}")
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Lock file exists but contains invalid JSON: {e}")
            return None
        except Exception as e:
            logger.debug(f"Could not read lock file: {e}")
            return None
    
    def _write_lock_file(self, lock_info: dict, overwrite: bool = False) -> bool:
        """Write lock file with job information."""
        try:
            content = json.dumps(lock_info, indent=2)
            notebookutils.fs.put(self.lock_path, content, overwrite=overwrite)
            return True
        except Exception as e:
            error_msg = str(e).lower()
            if "exists" in error_msg or "already" in error_msg:
                logger.debug(f"Lock file already exists: {e}")
            else:
                logger.error(f"Failed to write lock file: {e}")
            return False
    
    def _delete_lock_file(self) -> bool:
        """Delete the lock file."""
        try:
            notebookutils.fs.rm(self.lock_path, recurse=False)
            return True
        except Exception as e:
            logger.warning(f"Failed to delete lock file: {e}")
            return False

    def _is_lock_stale(self, lock_info: dict) -> bool:
        """Check if an existing lock is stale based on heartbeat timestamp."""
        heartbeat_str = lock_info.get('heartbeat')
        if not heartbeat_str:
            # Old lock format without heartbeat - check started_at instead
            heartbeat_str = lock_info.get('started_at')
        
        if not heartbeat_str:
            # No timestamp at all - consider stale
            return True
            
        try:
            heartbeat_time = datetime.fromisoformat(heartbeat_str.replace('Z', '+00:00'))
            age = (datetime.now(timezone.utc) - heartbeat_time).total_seconds()
            is_stale = age > self.STALE_THRESHOLD
            if is_stale:
                logger.info(f"Lock is stale (last heartbeat: {age:.0f}s ago, threshold: {self.STALE_THRESHOLD}s)")
            return is_stale
        except Exception as e:
            logger.warning(f"Failed to parse heartbeat timestamp: {e}")
            return True

    def _log_lock_holder(self, lock_info: dict):
        """Log information about the current lock holder."""
        logger.error("=" * 80)
        logger.error("LOCK ACQUISITION FAILED - Another instance is already running!")
        logger.error("=" * 80)
        logger.error(f"  Job Name: {lock_info.get('job_name', 'unknown')}")
        logger.error(f"  Started At: {lock_info.get('started_at', 'unknown')}")
        logger.error(f"  Last Heartbeat: {lock_info.get('heartbeat', 'unknown')}")
        logger.error(f"  Activity ID: {lock_info.get('activity_id', 'unknown')}")
        logger.error(f"  Livy ID: {lock_info.get('livy_id', 'unknown')}")
        logger.error(f"  Instance ID: {lock_info.get('instance_id', 'unknown')}")
        logger.error("=" * 80)
        logger.error("This job will exit to prevent concurrent streaming operations.")
        logger.error("To force a new run, manually delete the lock file at:")
        logger.error(f"  {self.lock_path}")
        logger.error("=" * 80)

    def _create_lock_info(self) -> dict:
        """Create lock info dictionary with current job details."""
        context = notebookutils.runtime.context
        now = datetime.now(timezone.utc).isoformat()
        return {
            "job_name": self.job_name,
            "started_at": now,
            "heartbeat": now,
            "activity_id": context.get('activityId', 'unknown'),
            "livy_id": context.get('livyId', 'unknown'),
            "workspace_id": context.get('currentWorkspaceId', 'unknown'),
            "instance_id": self.instance_id,
        }

    def _start_heartbeat(self):
        """Start background thread to update heartbeat periodically."""
        import threading
        
        def heartbeat_loop():
            while not self._stop_heartbeat:
                time.sleep(self.HEARTBEAT_INTERVAL)
                if self._stop_heartbeat:
                    break
                self._update_heartbeat()
        
        self._stop_heartbeat = False
        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        logger.info(f"Heartbeat thread started (interval: {self.HEARTBEAT_INTERVAL}s)")

    def _stop_heartbeat_thread(self):
        """Stop the heartbeat background thread."""
        self._stop_heartbeat = True
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5)
            self._heartbeat_thread = None

    def _update_heartbeat(self):
        """Update the heartbeat timestamp in the lock file."""
        try:
            lock_info = self._read_lock_file()
            if lock_info and lock_info.get('instance_id') == self.instance_id:
                lock_info['heartbeat'] = datetime.now(timezone.utc).isoformat()
                self._write_lock_file(lock_info, overwrite=True)
                logger.debug("Heartbeat updated")
            else:
                logger.warning("Lock ownership lost - stopping heartbeat")
                self._stop_heartbeat = True
        except Exception as e:
            logger.warning(f"Failed to update heartbeat: {e}")
    
    def acquire(self) -> bool:
        """
        Attempt to acquire the lock using atomic file creation.
        Handles stale locks from crashed/terminated jobs.
        Returns True if lock was acquired, False if another instance is running.
        """
        lock_info = self._create_lock_info()
        
        # First check if lock file already exists
        if self._file_exists():
            logger.info("Lock file exists, checking if stale...")
            existing_lock = self._read_lock_file()
            
            # Determine if we should attempt takeover
            should_takeover = False
            if not existing_lock:
                # File exists but unreadable/invalid - treat as corrupted
                logger.warning("Lock file exists but couldn't parse it - treating as corrupted")
                should_takeover = True
            elif self._is_lock_stale(existing_lock):
                logger.warning("=" * 80)
                logger.warning("STALE LOCK DETECTED - Taking over from crashed/terminated job")
                logger.warning(f"  Previous holder: {existing_lock.get('instance_id', 'unknown')}")
                logger.warning(f"  Previous start: {existing_lock.get('started_at', 'unknown')}")
                logger.warning(f"  Last heartbeat: {existing_lock.get('heartbeat', 'unknown')}")
                logger.warning("=" * 80)
                should_takeover = True
            else:
                # Lock is held by an active job
                self._log_lock_holder(existing_lock)
                return False
            
            if should_takeover:
                # Delete the stale lock first, then create new one atomically
                logger.info("Deleting stale/corrupted lock file...")
                self._delete_lock_file()
                time.sleep(1)  # Brief delay to ensure deletion propagates
        
        # Try atomic creation (overwrite=False)
        logger.info("Attempting to create lock file...")
        if self._write_lock_file(lock_info, overwrite=False):
            # Verify we own the lock
            time.sleep(1)
            verified = self._read_lock_file()
            if verified and verified.get('instance_id') == self.instance_id:
                self.lock_acquired = True
                self._start_heartbeat()
                logger.info("=" * 80)
                logger.info("LOCK ACQUIRED - This is the only running instance")
                logger.info(f"  Lock file: {self.lock_path}")
                logger.info(f"  Instance ID: {self.instance_id}")
                logger.info("=" * 80)
                return True
            elif verified:
                # Another job got the lock
                logger.error("Another instance acquired the lock during our attempt")
                self._log_lock_holder(verified)
                return False
            else:
                logger.error("Created lock file but couldn't verify ownership")
                return False
        else:
            # Another job created the lock between our check and create
            logger.error("Failed to create lock file - another instance may have acquired it")
            existing_lock = self._read_lock_file()
            if existing_lock:
                self._log_lock_holder(existing_lock)
            return False
    
    def release(self):
        """Release the lock by stopping heartbeat and deleting the lock file."""
        self._stop_heartbeat_thread()
        
        if self.lock_acquired:
            # Verify we still own the lock before deleting
            current_lock = self._read_lock_file()
            if current_lock and current_lock.get('instance_id') == self.instance_id:
                if self._delete_lock_file():
                    logger.info("Lock released successfully")
                    self.lock_acquired = False
                else:
                    logger.warning("Failed to release lock - manual cleanup may be required")
            else:
                logger.warning("Lock ownership changed - not releasing")
                self.lock_acquired = False
