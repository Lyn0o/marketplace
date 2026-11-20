import uuid
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from shared.security import current_user, admin, owner


class OrderItem(BaseModel):
    productId: str = Field(min_length=1)
    quantity: int = Field(gt=0)


class OrderCreate(BaseModel):
    userId: str = Field(min_length=1)
    items: list[OrderItem] = Field(min_length=1)
    totalAmount: float = Field(gt=0, allow_inf_nan=False)


class Payment(BaseModel):
    paymentMethod: str = Field(default='card', min_length=1)


class Cancellation(BaseModel):
    reason: str = Field(default='Customer request', min_length=1)


def router(store):
    routes = APIRouter(tags=['Commands'])

    def load(oid, user):
        state = store.replay(oid)
        if state is None:
            raise HTTPException(404, 'Order not found')
        owner(user, state)
        return state

    def save(oid, kind, payload, version):
        try:
            return store.append(oid, kind, payload, version)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @routes.post('/', status_code=201)
    def create_order(body: OrderCreate, user=Depends(current_user)):
        owner(user, {'userId': body.userId})
        oid = str(uuid.uuid4())
        state = store.replay(oid)
        if state is not None:
            raise HTTPException(409, 'Order already exists')
        return save(oid, 'OrderCreated', body.model_dump(), 0)

    @routes.post('/{order_id}/pay')
    def pay(order_id: str, body: Payment = Payment(), user=Depends(current_user)):
        state = load(order_id, user)
        if state['status'] != 'CREATED':
            raise HTTPException(409, 'Only a created order can be paid')
        return save(order_id, 'OrderPaid', {'paidAmount': state['totalAmount'],
                    'paymentMethod': body.paymentMethod}, state['version'])

    @routes.post('/{order_id}/cancel')
    def cancel(order_id: str, body: Cancellation = Cancellation(), user=Depends(current_user)):
        state = load(order_id, user)
        if state['status'] == 'CANCELLED':
            raise HTTPException(409, 'Order is already cancelled')
        # The lab explicitly demonstrates cancellation after payment.
        return save(order_id, 'OrderCancelled', body.model_dump(), state['version'])

    @routes.post('/admin/rebuild', dependencies=[Depends(admin)])
    def rebuild():
        return {'rebuilt': store.rebuild(), 'source': 'events'}

    return routes
