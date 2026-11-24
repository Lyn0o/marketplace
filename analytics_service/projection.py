"""Projection and deduplication are saved in one transaction."""
import sqlite3
from pathlib import Path


class AnalyticsProjection:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self.connect() as db:
            db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS processed(event_id TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS analytics_orders(
                    order_id TEXT PRIMARY KEY, amount REAL NOT NULL,
                    paid_amount REAL NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL);
            ''')

    def connect(self):
        return sqlite3.connect(self.path, timeout=15)

    def apply(self, event):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM processed WHERE event_id=?', (event['eventId'],)).fetchone():
                return
            row = db.execute('SELECT version FROM analytics_orders WHERE order_id=?', (event['orderId'],)).fetchone()
            version = row[0] if row else 0
            if event['version'] > version:
                if event['version'] != version + 1:
                    raise RuntimeError('Analytics version gap; replay the event stream')
                kind, payload = event['eventType'], event['payload']
                if kind == 'OrderCreated':
                    db.execute('INSERT INTO analytics_orders VALUES(?,?,?,?,?)',
                        (event['orderId'], payload['totalAmount'], 0, 'CREATED', event['version']))
                elif kind == 'OrderPaid':
                    db.execute('UPDATE analytics_orders SET paid_amount=?,status=?,version=? WHERE order_id=?',
                        (payload['paidAmount'], 'PAID', event['version'], event['orderId']))
                else:
                    # Cancellation changes status; it is not a refund event.
                    db.execute('UPDATE analytics_orders SET status=?,version=? WHERE order_id=?',
                        ('CANCELLED', event['version'], event['orderId']))
            db.execute('INSERT INTO processed VALUES(?)', (event['eventId'],))

    def summary(self):
        with self.connect() as db:
            db.execute('BEGIN')
            total, paid, average = db.execute(
                'SELECT COUNT(*),COALESCE(SUM(paid_amount),0),COALESCE(AVG(amount),0) FROM analytics_orders').fetchone()
            counts = dict.fromkeys(('CREATED', 'PAID', 'CANCELLED'), 0)
            counts.update(dict(db.execute('SELECT status,COUNT(*) FROM analytics_orders GROUP BY status')))
            return {'total_orders': total, 'paid_amount': round(paid, 2),
                    'status_counts': counts, 'average_order_amount': round(average, 2)}
