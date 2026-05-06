"""
File System Monitoring Service for Allocare
Monitors configuration and data directories for changes and publishes events to RabbitMQ
Implements Chapter 7: Operating System Support - File System Notifications
"""

import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
import pika
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent, FileDeletedEvent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - FILE_MONITOR - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# RabbitMQ Configuration
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))

# Monitored Directories
MONITORED_PATHS = [
    'data/',           # Data directory
    'config/',         # Configuration directory
    'templates/',      # HTML templates
    'static/css/',     # CSS files
    'static/js/'       # JavaScript files
]

# File Extensions to Monitor
MONITORED_EXTENSIONS = {'.json', '.csv', '.txt', '.conf', '.css', '.js', '.html', '.py'}


class AllocationAwareFileEventHandler(FileSystemEventHandler):
    """Handles file system events and publishes them to RabbitMQ"""
    
    def __init__(self, channel: pika.adapters.blocking_connection.BlockingChannel):
        self.channel = channel
        self.last_event_time = {}  # Debounce rapid events
        self.debounce_delay = 0.5  # seconds
    
    def should_process_file(self, file_path: str) -> bool:
        """Determine if file should be monitored"""
        path = Path(file_path)
        
        # Check extension
        if path.suffix not in MONITORED_EXTENSIONS:
            return False
        
        # Skip hidden files
        if path.name.startswith('.'):
            return False
        
        # Skip temp files
        if '~' in path.name or path.suffix == '.tmp':
            return False
        
        return True
    
    def debounce_event(self, file_path: str) -> bool:
        """Prevent duplicate events from rapid file changes"""
        current_time = time.time()
        last_time = self.last_event_time.get(file_path, 0)
        
        if current_time - last_time < self.debounce_delay:
            return False
        
        self.last_event_time[file_path] = current_time
        return True
    
    def publish_event(self, event_type: str, file_path: str, details: dict = None):
        """Publish file change event to RabbitMQ"""
        if not self.should_process_file(file_path):
            return
        
        if not self.debounce_event(file_path):
            return
        
        path_obj = Path(file_path)
        
        try:
            # Get file metadata
            stat_info = path_obj.stat() if path_obj.exists() else {}
            
            # Create event payload
            event = {
                'event_id': f"fs_{int(time.time() * 1000)}",
                'topic': 'allocare.filesystem.events',
                'timestamp': datetime.now().isoformat(),
                'source': 'file-monitor-service',
                'event_type': event_type,
                'data': {
                    'file_path': str(file_path),
                    'file_name': path_obj.name,
                    'file_size': stat_info.get('st_size', 0) if stat_info else 0,
                    'event_category': 'config' if 'config' in file_path else 'data',
                    'file_extension': path_obj.suffix
                },
                'metadata': {
                    'monitor_service': 'file-system-monitor',
                    'correlation_id': f"fs_corr_{int(time.time() * 1000)}"
                }
            }
            
            # Publish to RabbitMQ
            self.channel.basic_publish(
                exchange='',
                routing_key='allocare.filesystem.events',
                body=json.dumps(event),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent message
                    content_type='application/json'
                )
            )
            
            logger.info(f"Event published {event_type}: {path_obj.name}")
            
        except Exception as e:
            logger.error(f"Failed to publish file event: {e}")
    
    def on_created(self, event: FileCreatedEvent):
        """Called when a file is created"""
        if not event.is_directory:
            self.publish_event('file_created', event.src_path, {
                'reason': 'new_file_added'
            })
    
    def on_modified(self, event: FileModifiedEvent):
        """Called when a file is modified"""
        if not event.is_directory:
            self.publish_event('file_modified', event.src_path, {
                'reason': 'content_changed'
            })
    
    def on_deleted(self, event: FileDeletedEvent):
        """Called when a file is deleted"""
        if not event.is_directory:
            self.publish_event('file_deleted', event.src_path, {
                'reason': 'file_removed'
            })


class FileMonitorService:
    """Main service for monitoring file system events"""
    
    def __init__(self):
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self.observer: Optional[Observer] = None
        self.event_handler: Optional[AllocationAwareFileEventHandler] = None
    
    def connect_to_rabbitmq(self, max_retries: int = 5):
        """Establish connection to RabbitMQ with retry logic"""
        for attempt in range(max_retries):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_HOST,
                        port=RABBITMQ_PORT,
                        connection_attempts=3,
                        retry_delay=2
                    )
                )
                self.channel = self.connection.channel()
                logger.info("Connected to RabbitMQ")
                return
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    raise
    
    def setup_queue(self):
        """Declare the file system events queue"""
        try:
            self.channel.queue_declare(
                queue='allocare.filesystem.events',
                durable=True,
                auto_delete=False
            )
            logger.info("Queue 'allocare.filesystem.events' initialized")
        except Exception as e:
            logger.error(f"Failed to declare queue: {e}")
            raise
    
    def start_monitoring(self):
        """Start monitoring file system"""
        try:
            self.observer = Observer()
            self.event_handler = AllocationAwareFileEventHandler(self.channel)
            
            # Create monitored directories if they don't exist
            for path in MONITORED_PATHS:
                path_obj = Path(path)
                path_obj.mkdir(parents=True, exist_ok=True)
                
                # Schedule observer for this path
                self.observer.schedule(self.event_handler, path, recursive=True)
                logger.info(f"Monitoring directory: {path}")
            
            self.observer.start()
            logger.info("File system monitoring started")
            
        except Exception as e:
            logger.error(f"Failed to start monitoring: {e}")
            raise
    
    def run(self):
        """Main run loop"""
        try:
            logger.info("=" * 60)
            logger.info("FILE SYSTEM MONITOR SERVICE STARTING")
            logger.info("=" * 60)
            
            # Connect to RabbitMQ
            self.connect_to_rabbitmq()
            
            # Setup queue
            self.setup_queue()
            
            # Start monitoring
            self.start_monitoring()
            
            logger.info("FILE MONITOR: Ready. Watching for file system changes...")
            
            # Keep the service running
            while True:
                time.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("\nShutdown signal received")
            self.stop()
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            self.stop()
            raise
    
    def stop(self):
        """Gracefully stop the service"""
        logger.info("Shutting down FILE MONITOR...")
        
        if self.observer:
            self.observer.stop()
            self.observer.join()
            logger.info("Observer stopped")
        
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("RabbitMQ connection closed")
        
        logger.info("FILE MONITOR: Shutdown complete")


def main():
    """Entry point for file monitor service"""
    service = FileMonitorService()
    service.run()


if __name__ == '__main__':
    main()
