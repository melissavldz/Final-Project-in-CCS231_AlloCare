import json
import os
import sqlite3
from datetime import datetime, timezone

import pika
from flask import Flask, flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-only-change-me")

DATABASE_PATH = os.environ.get("DATABASE_PATH", "data/allocare.db")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE = os.environ.get("RABBITMQ_QUEUE", "allocare.auth.events")
RABBITMQ_PATIENT_QUEUE = os.environ.get("RABBITMQ_PATIENT_QUEUE", "allocare.patient.admission")
RABBITMQ_BED_QUEUE = os.environ.get("RABBITMQ_BED_QUEUE", "allocare.bed.updates")
RABBITMQ_BED_CAPACITY_QUEUE = os.environ.get("RABBITMQ_BED_CAPACITY_QUEUE", "allocare.bed.capacity")
RABBITMQ_DOCTOR_QUEUE = os.environ.get("RABBITMQ_DOCTOR_QUEUE", "allocare.doctor.registration")


def init_db() -> None:
    db_dir = os.path.dirname(DATABASE_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS patient_admissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id TEXT NOT NULL,
                severity TEXT NOT NULL,
                facility_unit TEXT NOT NULL,
                patient_name TEXT,
                admission_notes TEXT,
                admitted_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bed_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bed_id TEXT NOT NULL,
                status TEXT NOT NULL,
                facility_type TEXT NOT NULL,
                updated_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS facility_capacities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                facility_unit TEXT NOT NULL UNIQUE,
                max_capacity INTEGER NOT NULL,
                updated_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS doctors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doctor_id TEXT NOT NULL UNIQUE,
                specialty TEXT NOT NULL,
                availability TEXT NOT NULL,
                doctor_name TEXT,
                registered_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'subscriber',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS consumed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                delivery_tag TEXT,
                ack_status TEXT DEFAULT 'acknowledged',
                consumed_at TEXT NOT NULL,
                consumed_by TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                source_data TEXT,
                is_resolved INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                resolved_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_id INTEGER,
                queue_name TEXT NOT NULL,
                topic_filter TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                FOREIGN KEY (subscriber_id) REFERENCES subscribers(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_system_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_size INTEGER,
                event_category TEXT,
                file_extension TEXT,
                processed_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


init_db()


def publish_event(event_type: str, payload: dict, queue: str = None) -> None:
    if queue is None:
        queue = RABBITMQ_QUEUE
    
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
    )
    message = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        connection.close()
    except Exception as exc:  # noqa: BLE001
        app.logger.warning("RabbitMQ publish failed: %s", exc)


def store_patient_admission(payload: dict) -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO patient_admissions (
                patient_id,
                severity,
                facility_unit,
                patient_name,
                admission_notes,
                admitted_by,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["patient_id"],
                payload["severity"],
                payload["facility_unit"],
                payload.get("patient_name", ""),
                payload.get("admission_notes", ""),
                payload["admitted_by"],
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()


def store_bed_update(payload: dict) -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO bed_updates (
                bed_id,
                status,
                facility_type,
                updated_by,
                created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                payload["bed_id"],
                payload["status"],
                payload["facility_type"],
                payload["updated_by"],
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()


def store_facility_capacity(payload: dict) -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO facility_capacities (
                facility_unit,
                max_capacity,
                updated_by,
                created_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(facility_unit) DO UPDATE SET
                max_capacity = excluded.max_capacity,
                updated_by = excluded.updated_by,
                created_at = excluded.created_at
            """,
            (
                payload["facility_unit"],
                payload["max_capacity"],
                payload["updated_by"],
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()


def store_doctor(payload: dict) -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO doctors (
                doctor_id,
                specialty,
                availability,
                doctor_name,
                registered_by,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(doctor_id) DO UPDATE SET
                specialty = excluded.specialty,
                availability = excluded.availability,
                doctor_name = excluded.doctor_name,
                created_at = excluded.created_at
            """,
            (
                payload["doctor_id"],
                payload["specialty"],
                payload["availability"],
                payload.get("doctor_name", ""),
                payload["registered_by"],
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()


def get_dashboard_data() -> dict:
    with get_db_connection() as conn:
        admissions = conn.execute(
            "SELECT patient_id, severity, facility_unit, created_at FROM patient_admissions ORDER BY id DESC LIMIT 6"
        ).fetchall()
        beds = conn.execute(
            "SELECT bed_id, status, facility_type, created_at FROM bed_updates ORDER BY id DESC LIMIT 6"
        ).fetchall()
        total_admissions = conn.execute("SELECT COUNT(*) AS count FROM patient_admissions").fetchone()["count"]
        total_doctors = conn.execute("SELECT COUNT(*) AS count FROM doctors").fetchone()["count"]
        severity_counts_rows = conn.execute(
            "SELECT severity, COUNT(*) AS count FROM patient_admissions GROUP BY severity"
        ).fetchall()
        bed_status_counts_rows = conn.execute(
            "SELECT status, COUNT(*) AS count FROM bed_updates GROUP BY status"
        ).fetchall()
        capacity_rows = conn.execute(
            "SELECT facility_unit, max_capacity, updated_by, created_at FROM facility_capacities ORDER BY facility_unit ASC"
        ).fetchall()
        latest_beds = conn.execute(
            """
            SELECT b1.bed_id, b1.status, b1.facility_type, b1.created_at
            FROM bed_updates b1
            INNER JOIN (
                SELECT bed_id, MAX(id) AS latest_id
                FROM bed_updates
                GROUP BY bed_id
            ) latest ON latest.latest_id = b1.id
            """
        ).fetchall()
        last_updated_row = conn.execute(
            "SELECT MAX(created_at) AS created_at FROM (SELECT created_at FROM patient_admissions UNION ALL SELECT created_at FROM bed_updates)"
        ).fetchone()

    severity_counts = {level: 0 for level in ["low", "medium", "high", "critical"]}
    for row in severity_counts_rows:
        severity_counts[row["severity"]] = row["count"]

    bed_status_counts = {status: 0 for status in ["vacant", "occupied"]}
    for row in bed_status_counts_rows:
        if row["status"] in bed_status_counts:
            bed_status_counts[row["status"]] = row["count"]

    max_severity = max(severity_counts.values()) if severity_counts else 0
    if max_severity == 0:
        max_severity = 1

    capacity_lookup = {row["facility_unit"]: row["max_capacity"] for row in capacity_rows}
    occupied_lookup = {unit: 0 for unit in capacity_lookup}
    for row in latest_beds:
        facility_unit = row["facility_type"]
        if facility_unit in occupied_lookup and row["status"] == "occupied":
            occupied_lookup[facility_unit] += 1

    capacity_summary = []
    for row in capacity_rows:
        facility_unit = row["facility_unit"]
        max_capacity = row["max_capacity"]
        occupied_count = occupied_lookup.get(facility_unit, 0)
        ratio = round((occupied_count / max_capacity) * 100) if max_capacity else 0
        remaining = max_capacity - occupied_count
        capacity_summary.append(
            {
                "facility_unit": facility_unit,
                "max_capacity": max_capacity,
                "occupied_count": occupied_count,
                "remaining_capacity": remaining if remaining > 0 else 0,
                "occupancy_percent": ratio,
                "updated_by": row["updated_by"],
                "created_at": row["created_at"],
            }
        )

    total_capacity = sum(row["max_capacity"] for row in capacity_rows)
    total_occupied = sum(occupied_lookup.values())
    total_capacity_ratio = round((total_occupied / total_capacity) * 100) if total_capacity else 0

    severity_series = [
        {"label": level, "count": severity_counts[level], "percent": round((severity_counts[level] / max_severity) * 100)}
        for level in ["low", "medium", "high", "critical"]
    ]

    return {
        "total_admissions": total_admissions,
        "total_doctors": total_doctors,
        "severity_counts": severity_counts,
        "bed_status_counts": bed_status_counts,
        "facility_capacities": capacity_rows,
        "capacity_summary": capacity_summary,
        "total_capacity": total_capacity,
        "total_occupied": total_occupied,
        "total_capacity_ratio": total_capacity_ratio,
        "severity_series": severity_series,
        "max_severity": max_severity,
        "recent_admissions": admissions,
        "recent_beds": beds,
        "last_updated": last_updated_row["created_at"] if last_updated_row and last_updated_row["created_at"] else "No activity yet",
    }


@app.route("/")
def home():
    if "username" in session:
        return redirect(url_for("publisher_dashboard"))
    return redirect(url_for("login"))


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        if not username or not password:
            flash("Username and password are required.", "error")
            return render_template("signup.html")

        if password != confirm_password:
            flash("Passwords do not match.", "error")
            return render_template("signup.html")

        hashed_password = generate_password_hash(password)

        try:
            with get_db_connection() as conn:
                conn.execute(
                    "INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)",
                    (username, hashed_password, datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
        except sqlite3.IntegrityError:
            flash("That username is already taken.", "error")
            return render_template("signup.html")

        publish_event("user.signup", {"username": username})
        flash("Account created. Please log in.", "success")
        return redirect(url_for("login"))

    return render_template("signup.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        with get_db_connection() as conn:
            user = conn.execute(
                "SELECT id, username, password_hash FROM users WHERE username = ?",
                (username,),
            ).fetchone()

        if user is None or not check_password_hash(user["password_hash"], password):
            flash("Invalid username or password.", "error")
            return render_template("login.html")

        session["user_id"] = user["id"]
        session["username"] = user["username"]
        publish_event("user.login", {"username": user["username"]})
        return redirect(url_for("publisher_dashboard"))

    return render_template("login.html")


@app.route("/dashboard")
def dashboard_alias():
    if "username" not in session:
        return redirect(url_for("login"))
    return redirect(url_for("publisher_dashboard"))


@app.route("/publisher-dashboard")
def publisher_dashboard():
    if "username" not in session:
        return redirect(url_for("login"))
    return render_template("dashboard.html", **get_dashboard_data(), username=session["username"])


@app.route("/dashboard-main")
def dashboard():
    if "username" not in session:
        return redirect(url_for("login"))
    return render_template("dashboard.html", **get_dashboard_data(), username=session["username"])


@app.route("/patient-admission", methods=["GET", "POST"])
def patient_admission():
    if "username" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        patient_id = request.form.get("patient_id", "").strip()
        severity = request.form.get("severity", "").strip()
        facility_unit = request.form.get("facility_unit", "").strip()
        patient_name = request.form.get("patient_name", "").strip()
        admission_notes = request.form.get("admission_notes", "").strip()

        if not patient_id or not severity or not facility_unit:
            flash("Patient ID, Severity, and Facility Unit are required.", "error")
            return render_template("patient-admission.html")

        payload = {
            "patient_id": patient_id,
            "severity": severity,
            "facility_unit": facility_unit,
            "patient_name": patient_name,
            "admission_notes": admission_notes,
            "admitted_by": session["username"],
        }

        store_patient_admission(payload)
        publish_event("patient.admission", payload, RABBITMQ_PATIENT_QUEUE)
        flash(f"Patient {patient_id} admitted successfully.", "success")
        return redirect(url_for("publisher_dashboard"))

    return render_template("patient-admission.html")


@app.route("/bed-management", methods=["GET", "POST"])
def bed_management():
    if "username" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        form_action = request.form.get("form_action", "")

        if form_action == "set_capacity":
            facility_unit = request.form.get("capacity_facility_unit", "").strip()
            max_capacity = request.form.get("max_capacity", "").strip()

            if not facility_unit or not max_capacity:
                flash("Facility unit and max capacity are required.", "error")
                return render_template("bed-management.html")

            try:
                max_capacity_value = int(max_capacity)
            except ValueError:
                flash("Max capacity must be a valid number.", "error")
                return render_template("bed-management.html")

            if max_capacity_value <= 0:
                flash("Max capacity must be greater than zero.", "error")
                return render_template("bed-management.html")

            payload = {
                "facility_unit": facility_unit,
                "max_capacity": max_capacity_value,
                "updated_by": session["username"],
            }

            store_facility_capacity(payload)
            publish_event("bed.capacity", payload, RABBITMQ_BED_CAPACITY_QUEUE)
            flash(f"Capacity for {facility_unit} set to {max_capacity_value} beds.", "success")
            return redirect(url_for("dashboard"))

        if form_action == "update_bed":
            bed_id = request.form.get("bed_id", "").strip()
            bed_status = request.form.get("bed_status", "").strip()
            facility_type = request.form.get("facility_type", "").strip()

            if not bed_id or not bed_status or not facility_type:
                flash("Bed ID, Status, and Facility Type are required.", "error")
                return render_template("bed-management.html")

            payload = {
                "bed_id": bed_id,
                "status": bed_status,
                "facility_type": facility_type,
                "updated_by": session["username"],
            }

            store_bed_update(payload)
            publish_event("bed.update", payload, RABBITMQ_BED_QUEUE)
            flash(f"Bed {bed_id} status updated to {bed_status}.", "success")
            return redirect(url_for("publisher_dashboard"))

        flash("Choose a valid bed management action.", "error")

    return render_template("bed-management.html", **get_dashboard_data())


@app.route("/doctor-registration", methods=["GET", "POST"])
def doctor_registration():
    if "username" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        doctor_id = request.form.get("doctor_id", "").strip()
        specialty = request.form.get("specialty", "").strip()
        availability = request.form.get("availability", "").strip()
        doctor_name = request.form.get("doctor_name", "").strip()

        if not doctor_id or not specialty or not availability:
            flash("Doctor ID, Specialty, and Availability are required.", "error")
            return render_template("doctor-registration.html")

        payload = {
            "doctor_id": doctor_id,
            "specialty": specialty,
            "availability": availability,
            "doctor_name": doctor_name,
            "registered_by": session["username"],
        }

        store_doctor(payload)
        publish_event("doctor.registration", payload, RABBITMQ_DOCTOR_QUEUE)
        flash(f"Doctor {doctor_id} registered successfully.", "success")
        return redirect(url_for("publisher_dashboard"))

    return render_template("doctor-registration.html")


@app.route("/logout", methods=["POST"])
def logout():
    username = session.get("username")
    session.clear()
    if username:
        publish_event("user.logout", {"username": username})
    return redirect(url_for("login"))


# ============ SUBSCRIBER ROUTES ============

@app.route("/subscriber-signup", methods=["GET", "POST"])
def subscriber_signup():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        if not username or not password:
            flash("Username and password are required.", "error")
            return render_template("subscriber-signup.html")

        if password != confirm_password:
            flash("Passwords do not match.", "error")
            return render_template("subscriber-signup.html")

        hashed_password = generate_password_hash(password)

        try:
            with get_db_connection() as conn:
                conn.execute(
                    "INSERT INTO subscribers (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
                    (username, hashed_password, "subscriber", datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
        except sqlite3.IntegrityError:
            flash("That username is already taken.", "error")
            return render_template("subscriber-signup.html")

        flash("Subscriber account created. Please log in.", "success")
        return redirect(url_for("subscriber_login"))

    return render_template("subscriber-signup.html")


@app.route("/subscriber-login", methods=["GET", "POST"])
def subscriber_login():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        with get_db_connection() as conn:
            user = conn.execute(
                "SELECT id, username, password_hash FROM subscribers WHERE username = ?",
                (username,),
            ).fetchone()

        if user is None or not check_password_hash(user["password_hash"], password):
            flash("Invalid username or password.", "error")
            return render_template("subscriber-login.html")

        session["subscriber_id"] = user["id"]
        session["subscriber_username"] = user["username"]
        return redirect(url_for("subscriber_dashboard"))

    return render_template("subscriber-login.html")


@app.route("/subscriber-dashboard")
def subscriber_dashboard():
    if "subscriber_id" not in session:
        return redirect(url_for("subscriber_login"))

    with get_db_connection() as conn:
        total_events = conn.execute("SELECT COUNT(*) AS count FROM consumed_events").fetchone()["count"]
        event_types = conn.execute(
            "SELECT event_type, COUNT(*) AS count FROM consumed_events GROUP BY event_type"
        ).fetchall()
        recent_events = conn.execute(
            "SELECT event_type, queue_name, consumed_at FROM consumed_events ORDER BY id DESC LIMIT 10"
        ).fetchall()
        
        # Analytics for admissions
        admissions = conn.execute(
            "SELECT payload FROM consumed_events WHERE event_type = 'patient.admission' ORDER BY id DESC LIMIT 6"
        ).fetchall()
        
        # Analytics for beds
        bed_events = conn.execute(
            "SELECT payload FROM consumed_events WHERE event_type = 'bed.update' ORDER BY id DESC LIMIT 6"
        ).fetchall()

    event_type_counts = {row["event_type"]: row["count"] for row in event_types}

    return render_template(
        "subscriber-dashboard.html",
        total_events=total_events,
        event_type_counts=event_type_counts,
        recent_events=recent_events,
        username=session.get("subscriber_username"),
    )


@app.route("/subscriber-alerts")
def subscriber_alerts():
    if "subscriber_id" not in session:
        return redirect(url_for("subscriber_login"))

    with get_db_connection() as conn:
        # Fetch unresolved alerts
        alerts = conn.execute(
            "SELECT * FROM alerts WHERE is_resolved = 0 ORDER BY severity DESC, created_at DESC"
        ).fetchall()

        # Count alerts by severity
        severity_counts = conn.execute(
            "SELECT severity, COUNT(*) AS count FROM alerts WHERE is_resolved = 0 GROUP BY severity"
        ).fetchall()

    severity_map = {row["severity"]: row["count"] for row in severity_counts}

    return render_template(
        "subscriber-alerts.html",
        alerts=alerts,
        severity_counts=severity_map,
        username=session.get("subscriber_username"),
    )


@app.route("/subscriber-history")
def subscriber_history():
    if "subscriber_id" not in session:
        return redirect(url_for("subscriber_login"))

    # Get filter parameters
    queue_filter = request.args.get("queue", "")
    event_type_filter = request.args.get("type", "")
    page = request.args.get("page", 1, type=int)
    per_page = 20

    with get_db_connection() as conn:
        query = "SELECT * FROM consumed_events WHERE 1=1"
        params = []

        if queue_filter:
            query += " AND queue_name = ?"
            params.append(queue_filter)

        if event_type_filter:
            query += " AND event_type = ?"
            params.append(event_type_filter)

        query += " ORDER BY id DESC LIMIT ? OFFSET ?"
        offset = (page - 1) * per_page
        params.extend([per_page + 1, offset])

        events = conn.execute(query, params).fetchall()

        # Get unique queues and event types for filters
        queues = conn.execute("SELECT DISTINCT queue_name FROM consumed_events ORDER BY queue_name").fetchall()
        event_types = conn.execute("SELECT DISTINCT event_type FROM consumed_events ORDER BY event_type").fetchall()

        total_count = conn.execute(
            "SELECT COUNT(*) AS count FROM consumed_events"
        ).fetchone()["count"]

    has_next = len(events) > per_page
    events = events[:per_page]

    return render_template(
        "subscriber-history.html",
        events=events,
        queues=[q["queue_name"] for q in queues],
        event_types=[e["event_type"] for e in event_types],
        current_queue_filter=queue_filter,
        current_type_filter=event_type_filter,
        page=page,
        has_next=has_next,
        username=session.get("subscriber_username"),
    )


@app.route("/subscriber-logout", methods=["POST"])
def subscriber_logout():
    session.clear()
    return redirect(url_for("subscriber_login"))


@app.route("/health")
def health():
    return {"status": "ok", "service": "allocare"}


if __name__ == "__main__":
    # Disable the auto-reloader on Windows to avoid watchdog thread errors.
    # When developing, you can set debug=False or run with an external WSGI server.
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)