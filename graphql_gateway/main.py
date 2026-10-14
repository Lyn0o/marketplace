import strawberry, httpx, asyncio
from typing import AsyncGenerator
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from datetime import datetime, timezone

ORDER_URL = "http://order:8003"

@strawberry.type
class Product:
    id: int
    name: str
    price: float

@strawberry.type
class OrderItem:
    product: Product
    quantity: int

@strawberry.type
class Order:
    id: str
    user_id: str
    created_at: str
    status: str
    items: list[OrderItem]

@strawberry.type
class OrderStatusEvent:
    order_id: str
    status: str
    at: str

# ---------- QUERY ----------
@strawberry.type
class Query:
    @strawberry.field
    async def orders(self) -> list[Order]:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{ORDER_URL}/orders/")
            data = r.json().get("orders", [])
        return [
            Order(
                id=o["orderId"], user_id=o["userId"],
                created_at=o["createdAt"], status=o["status"],
                items=[
                    OrderItem(
                        product=Product(id=hash(i["productId"]) % 10000, name=i["productId"], price=0),
                        quantity=i["quantity"],
                    ) for i in o["items"]
                ],
            )
            for o in data
        ]

    @strawberry.field
    async def order(self, order_id: str) -> Order | None:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{ORDER_URL}/orders/{order_id}")
            if r.status_code != 200: return None
            o = r.json()
        return Order(
            id=o["orderId"], user_id=o["userId"],
            created_at=o["createdAt"], status=o["status"],
            items=[],
        )

# ---------- MUTATION ----------
@strawberry.input
class OrderItemInput:
    productId: str
    quantity: int

@strawberry.type
class Mutation:
    @strawberry.mutation
    async def create_order(self, user_id: str, total_amount: float, items: list[OrderItemInput]) -> Order:
        payload = {
            "userId": user_id,
            "totalAmount": total_amount,
            "items": [{"productId": i.productId, "quantity": i.quantity} for i in items],
        }
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{ORDER_URL}/orders/", json=payload)
        o = r.json()
        return Order(id=o["orderId"], user_id=o["userId"], created_at=o["createdAt"],
                     status=o["status"], items=[])

# ---------- SUBSCRIPTION ----------
@strawberry.type
class Subscription:
    @strawberry.subscription
    async def order_status(self, order_id: str) -> AsyncGenerator[OrderStatusEvent, None]:
        """Стрим статуса заказа — клиент видит переходы PENDING → CONFIRMED/CANCELLED."""
        last = None
        for _ in range(60):  # максимум минуту
            try:
                async with httpx.AsyncClient() as c:
                    r = await c.get(f"{ORDER_URL}/orders/{order_id}")
                    if r.status_code == 200:
                        st = r.json()["status"]
                        if st != last:
                            last = st
                            yield OrderStatusEvent(
                                order_id=order_id, status=st,
                                at=datetime.now(timezone.utc).isoformat(),
                            )
                            if st in ("CONFIRMED", "CANCELLED"):
                                return
            except Exception as e:
                print("[Sub]", e)
            await asyncio.sleep(1)

schema = strawberry.Schema(query=Query, mutation=Mutation, subscription=Subscription)
graphql_app = GraphQLRouter(schema)

app = FastAPI(title="GraphQL Gateway")
app.include_router(graphql_app, prefix="/graphql")

@app.get("/health")
def health(): return {"status": "healthy", "service": "graphql-gateway"}