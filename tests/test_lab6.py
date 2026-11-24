import json
import os
from concurrent.futures import ThreadPoolExecutor
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault('JWT_SECRET', 'test-secret-that-is-long-enough-for-tests-only')
from order_service.store import EventStore
from order_service.commands import router as commands
from order_service.queries import router as queries
from analytics_service.projection import AnalyticsProjection
from shared.security import current_user
from shared.events import decode_event


@pytest.fixture
def system(tmp_path):
    store = EventStore(tmp_path / 'orders.db')
    analytics = AnalyticsProjection(tmp_path / 'analytics.db')
    app = FastAPI()
    app.include_router(commands(store))
    app.include_router(queries(store))
    app.dependency_overrides[current_user] = lambda: {'sub': 'student', 'role': 'user'}
    return store, analytics, TestClient(app)


def create(client, amount=1500):
    response = client.post('/', json={'userId': 'student', 'items': [{'productId': '1', 'quantity': 1}], 'totalAmount': amount})
    assert response.status_code == 201
    return response.json()['orderId']


def test_commands_replay_and_async_read_model(system):
    store, analytics, client = system
    oid = create(client)
    assert client.get('/' + oid).status_code == 404
    assert store.replay(oid)['status'] == 'CREATED'
    assert client.post(f'/{oid}/pay', json={}).status_code == 200
    assert client.post(f'/{oid}/pay', json={}).status_code == 409
    assert client.post(f'/{oid}/cancel', json={'reason': 'Changed my mind'}).status_code == 200
    assert client.post(f'/{oid}/cancel', json={}).status_code == 409
    assert client.post(f'/{oid}/pay', json={}).status_code == 409
    history = store.history(oid)
    assert [e['eventType'] for e in history] == ['OrderCreated', 'OrderPaid', 'OrderCancelled']
    assert [e['version'] for e in history] == [1, 2, 3]
    for event in history:
        decode_event(json.dumps(event))
        store.project(event)
    assert client.get('/' + oid).json()['status'] == 'CANCELLED'
    # GET must not rely on the event store, even for a single order.
    store.replay = lambda _: pytest.fail('GET called replay')
    store.history = lambda _: pytest.fail('GET called history')
    assert client.get('/' + oid).status_code == 200


def test_analytics_deduplication_cancellation_and_restart(system):
    store, analytics, client = system
    a, b, c = [create(client, amount) for amount in (1500, 3000, 750)]
    client.post(f'/{a}/pay', json={})
    client.post(f'/{a}/cancel', json={})
    client.post(f'/{b}/pay', json={})
    for oid in (a, b, c):
        for event in store.history(oid):
            analytics.apply(event)
            analytics.apply(event)
    expected = {'total_orders': 3, 'paid_amount': 4500,
                'status_counts': {'CREATED': 1, 'PAID': 1, 'CANCELLED': 1},
                'average_order_amount': 1750}
    assert analytics.summary() == expected
    assert AnalyticsProjection(analytics.path).summary() == expected
    for event in store.history(a):
        analytics.apply(event)
    assert analytics.summary() == expected


def test_concurrent_append_and_rebuild(system):
    store, _, client = system
    oid = create(client)
    def append(_):
        try:
            store.append(oid, 'OrderPaid', {'paidAmount': 1500, 'paymentMethod': 'card'}, 1)
            return True
        except ValueError:
            return False
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(append, range(2))) == [False, True]
    assert len(store.history(oid)) == 2
    assert store.get(oid) is None
    assert store.rebuild() == 1
    assert store.get(oid) == store.replay(oid)
    assert len(store.pending()) == 2


def test_invalid_input_owner_and_projection_gap(system):
    store, analytics, client = system
    assert client.post('/', json={'userId': 'student', 'items': [], 'totalAmount': -1}).status_code == 422
    assert client.post('/', json={'userId': 'other', 'items': [{'productId': '1', 'quantity': 1}], 'totalAmount': 10}).status_code == 403
    assert client.post('/missing/pay', json={}).status_code == 404
    oid = create(client)
    client.post(f'/{oid}/pay', json={})
    with pytest.raises(RuntimeError):
        analytics.apply(store.history(oid)[1])
    assert analytics.summary()['total_orders'] == 0


@pytest.mark.parametrize('value', [b'not-json', b'[]', b'{}', b'null'])
def test_malformed_events(value):
    with pytest.raises(ValueError):
        decode_event(value)
