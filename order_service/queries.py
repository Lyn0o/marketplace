from fastapi import APIRouter, Depends, HTTPException
from shared.security import current_user, owner


def router(store):
    routes = APIRouter(tags=["Queries"])

    @routes.get("/")
    def orders(user=Depends(current_user)):
        return {"orders": [o for o in store.list() if user["role"] == "admin" or o["userId"] == user["sub"]]}

    @routes.get("/{order_id}")
    def order(order_id: str, user=Depends(current_user)):
        state = store.get(order_id)
        if state is None:
            raise HTTPException(404, "Order not found")
        owner(user, state)
        return state

    @routes.get("/{order_id}/events")
    def events(order_id: str, user=Depends(current_user)):
        history = store.history(order_id)
        if not history:
            raise HTTPException(404, 'Order not found')
        owner(user, history[0]['payload'])
        return {"events": history}

    return routes
