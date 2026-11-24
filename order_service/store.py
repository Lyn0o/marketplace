"""Append-only event store with transactional outbox and rebuildable read model."""
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path


def evolve(state, event):
    data = event["payload"]
    if event["eventType"] == "OrderCreated":
        state = {**data, "orderId": event["orderId"], "createdAt": event["timestamp"], "status": "CREATED"}
    elif event["eventType"] == "OrderPaid":
        state = {**state, "status": "PAID", "paidAmount": data["paidAmount"], "paymentMethod": data["paymentMethod"]}
    elif event["eventType"] == "OrderCancelled":
        state = {**state, "status": "CANCELLED", "reason": data["reason"]}
    return {**state, "version": event["version"]}


class EventStore:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self.connect() as db:
            db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL,
                    order_id TEXT NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL,
                    UNIQUE(order_id, version));
                CREATE TABLE IF NOT EXISTS outbox (
                    event_id TEXT PRIMARY KEY, kafka_sent INTEGER DEFAULT 0,
                    rabbit_sent INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS orders_view (order_id TEXT PRIMARY KEY, body TEXT NOT NULL);
            ''')

    def connect(self):
        return sqlite3.connect(self.path, timeout=15)

    def history(self, oid):
        with self.connect() as db:
            return [json.loads(r[0]) for r in db.execute(
                "SELECT body FROM events WHERE order_id=? ORDER BY version", (oid,))]

    def replay(self, oid):
        state = None
        for event in self.history(oid):
            state = evolve(state, event)
        return state

    def append(self, oid, kind, data, expected_version):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT body FROM events WHERE order_id=? ORDER BY version", (oid,)).fetchall()
            if len(rows) != expected_version:
                raise ValueError("Concurrent order update")
            event = {"eventId": str(uuid.uuid4()), "orderId": oid, "eventType": kind,
                     "version": expected_version + 1, "timestamp": datetime.now(timezone.utc).isoformat(), "payload": data}
            state = None
            for row in rows:
                state = evolve(state, json.loads(row[0]))
            state = evolve(state, event)
            db.execute("INSERT INTO events(event_id,order_id,version,body) VALUES(?,?,?,?)",
                       (event["eventId"], oid, event["version"], json.dumps(event)))
            db.execute("INSERT INTO outbox(event_id) VALUES(?)", (event["eventId"],))
            return state

    def get(self, oid):
        with self.connect() as db:
            row = db.execute("SELECT body FROM orders_view WHERE order_id=?", (oid,)).fetchone()
            return json.loads(row[0]) if row else None

    def project(self, event):
        """The Kafka consumer alone updates this disposable read model."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT body FROM orders_view WHERE order_id=?", (event['orderId'],)).fetchone()
            state = json.loads(row[0]) if row else None
            version = state['version'] if state else 0
            if event['version'] <= version:
                return
            if event['version'] != version + 1:
                raise RuntimeError('Projection version gap; rebuild the read model')
            state = evolve(state, event)
            db.execute("INSERT OR REPLACE INTO orders_view VALUES(?,?)", (event['orderId'], json.dumps(state)))

    def list(self):
        with self.connect() as db:
            return [json.loads(r[0]) for r in db.execute("SELECT body FROM orders_view ORDER BY order_id")]

    def rebuild(self):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            states = {}
            for row in db.execute("SELECT body FROM events ORDER BY seq"):
                event = json.loads(row[0])
                oid = event["orderId"]
                states[oid] = evolve(states.get(oid), event)
            db.execute("DELETE FROM orders_view")
            db.executemany("INSERT INTO orders_view VALUES(?,?)", [(k, json.dumps(v)) for k, v in states.items()])
            return len(states)

    def pending(self):
        with self.connect() as db:
            return [(json.loads(r[0]), r[1], r[2]) for r in db.execute(
                "SELECT e.body,o.kafka_sent,o.rabbit_sent FROM events e JOIN outbox o ON e.event_id=o.event_id "
                "WHERE o.kafka_sent=0 OR o.rabbit_sent=0 ORDER BY e.seq LIMIT 100")]

    def mark(self, event_id, destination):
        column = {"kafka": "kafka_sent", "rabbit": "rabbit_sent"}[destination]
        with self.connect() as db:
            db.execute(f"UPDATE outbox SET {column}=1 WHERE event_id=?", (event_id,))
