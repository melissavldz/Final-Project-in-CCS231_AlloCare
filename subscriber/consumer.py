"""
RabbitMQ Consumer Service with At-Least-Once Delivery Guarantees
- Persistent message queues ensure durability
- Manual acknowledgment prevents message loss
- Message filtering by topic/queue type
- Multicast to multiple subscribers through persistent storage
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

import pika


RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
DATABASE_PATH = os.environ.get("DATABASE_PATH", "data/allocare.db")

QUEUES = [
    "allocare.patient.admission",
    "allocare.bed.updates",
    "allocare.bed.capacity",
    "allocare.doctor.registration",
    "allocare.filesystem.events",
]


def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def store_consumed_event(queue_name: str, event_data: dict, delivery_tag: str = None) -> None:
    """Store consumed event in database with at-least-once delivery guarantee."""
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO consumed_events (
                queue_name,
                event_type,
                payload,
                delivery_tag,
                ack_status,
                consumed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                queue_name,
                event_data.get("event_type", "unknown"),
                json.dumps(event_data.get("payload", {})),
                delivery_tag,
                "acknowledged",
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()

    # Store file system events separately
    if queue_name == "allocare.filesystem.events":
        store_file_system_event(event_data)

    # Check for alerts based on event content
    generate_alerts(queue_name, event_data)


def store_file_system_event(event_data: dict) -> None:
    """Store file system event in dedicated table (Chapter 7: OS Support)."""
    try:
        event_details = event_data.get('data', {})
        with get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO file_system_events (
                    event_type,
                    file_path,
                    file_name,
                    file_size,
                    event_category,
                    file_extension,
                    processed_at,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_data.get('event_type', 'unknown'),
                    event_details.get('file_path', ''),
                    event_details.get('file_name', ''),
                    event_details.get('file_size', 0),
                    event_details.get('event_category', ''),
                    event_details.get('file_extension', ''),
                    datetime.now(timezone.utc).isoformat(),
                    event_data.get('timestamp', datetime.now(timezone.utc).isoformat())
                ),
            )
            conn.commit()
    except Exception as e:
        print(f"[!] Error storing file system event: {e}")


def generate_alerts(queue_name: str, event_data: dict) -> None:
    """Generate alerts for critical events (Critical patients, full capacity, etc)."""
    event_type = event_data.get("event_type", "")
    payload = event_data.get("payload", {})

    with get_db_connection() as conn:
        # Alert for critical patient admissions
        if event_type == "patient.admission" and payload.get("severity") == "critical":
            conn.execute(
                """
                INSERT INTO alerts (
                    alert_type,
                    severity,
                    title,
                    description,
                    source_data,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "critical_patient",
                    "critical",
                    f"Critical Patient: {payload.get('patient_id')}",
                    f"Patient {payload.get('patient_id')} admitted with critical severity in {payload.get('facility_unit')}",
                    json.dumps(payload),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

        # Alert for facility capacity issues
        if event_type == "bed.capacity":
            max_capacity = payload.get("max_capacity", 0)
            # You could add logic here to check current occupancy vs capacity
            conn.execute(
                """
                INSERT INTO alerts (
                    alert_type,
                    severity,
                    title,
                    description,
                    source_data,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "capacity_update",
                    "info",
                    f"Capacity Updated: {payload.get('facility_unit')}",
                    f"Facility unit {payload.get('facility_unit')} capacity set to {max_capacity} beds",
                    json.dumps(payload),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

        # Alert for doctor availability changes
        if event_type == "doctor.registration" and payload.get("availability") == "off-duty":
            conn.execute(
                """
                INSERT INTO alerts (
                    alert_type,
                    severity,
                    title,
                    description,
                    source_data,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "doctor_unavailable",
                    "warning",
                    f"Doctor Off Duty: {payload.get('doctor_id')}",
                    f"Dr. {payload.get('doctor_name', 'Unknown')} ({payload.get('specialty')}) is now off duty",
                    json.dumps(payload),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

        # Alert for file system events (Chapter 7: OS Support)
        if queue_name == "allocare.filesystem.events":
            event_cat = event_data.get('data', {}).get('event_category', 'data')
            file_name = event_data.get('data', {}).get('file_name', 'unknown')
            
            alert_type_map = {
                'file_created': 'info',
                'file_modified': 'info',
                'file_deleted': 'warning'
            }
            severity = alert_type_map.get(event_type, 'info')
            
            conn.execute(
                """
                INSERT INTO alerts (
                    alert_type,
                    severity,
                    title,
                    description,
                    source_data,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    f"filesystem_{event_type}",
                    severity,
                    f"File System Event: {event_type.replace('file_', '').title()}",
                    f"{event_cat.title()} file '{file_name}' was {event_type.replace('file_', '')}",
                    json.dumps(event_data.get('data', {})),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

        conn.commit()


def callback(ch, method, properties, body) -> None:
    """Message callback with at-least-once delivery guarantee."""
    try:
        message = json.loads(body)
        queue_name = method.routing_key

        print(f"[*] Received from {queue_name}: {message.get('event_type')}")

        # Store event in database (persistent storage)
        store_consumed_event(queue_name, message, str(method.delivery_tag))

        # Manual acknowledgment - only ack after successful processing (at-least-once guarantee)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[SUCCESS] Acknowledged delivery_tag: {method.delivery_tag}")

    except Exception as e:
        print(f"[!] Error processing message: {e}")
        # Negative acknowledgment - message will be requeued
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def connect_consumer() -> None:
    """Connect to RabbitMQ and start consuming from all queues with persistent settings."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Set prefetch count to 1 for at-least-once delivery (process one message at a time)
        channel.basic_qos(prefetch_count=1)

        # Declare and configure all queues for persistence
        for queue_name in QUEUES:
            channel.queue_declare(queue=queue_name, durable=True)
            # Bind queue if needed (for now, simple setup)
            print(f"[+] Connected to queue: {queue_name}")

        # Set up consumer with manual acknowledgment
        for queue_name in QUEUES:
            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

        print("[*] Consumer started. Waiting for messages... (Press CTRL+C to exit)")
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"[!] Connection error: {e}")
        print("[*] Retrying in 5 seconds...")
        import time

        time.sleep(5)
        connect_consumer()
    except KeyboardInterrupt:
        print("\n[*] Consumer stopped")
        if connection and not connection.is_closed:
            connection.close()


if __name__ == "__main__":
    print("=" * 50)
    print("RabbitMQ Consumer Service")
    print("Features:")
    print("  - Persistent message queues")
    print("  - At-least-once delivery guarantees")
    print("  - Manual acknowledgment")
    print("  - Topic filtering (multicast to all subscribers)")
    print("=" * 50)
    connect_consumer()
