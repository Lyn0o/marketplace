import strawberry
import httpx
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter


@strawberry.type
class Product:
    id: int
    name: str
    price: float

@strawberry.type
class Order:
    order_id: int
    date: str
    product_id: int

@strawberry.type
class OrderWithProduct:
    order_id: int
    date: str
    product: Product


async def get_catalog_products() -> list[Product]:
    """Получаем товары из Catalog сервиса"""
    async with httpx.AsyncClient() as client:
        response = await client.get("http://catalog-1:8002/products")
        if response.status_code == 200:
            data = response.json()
            return [Product(**p) for p in data.get("products", [])]
    return []

async def get_mock_orders() -> list[Order]:
    """(пока нет Order сервиса)"""
    return [
        Order(order_id=1, date="2024-01-15", product_id=1),
        Order(order_id=2, date="2024-01-16", product_id=2),
    ]


@strawberry.type
class Query:
    @strawberry.field
    async def products(self) -> list[Product]:
        """Получить все товары из каталога"""
        return await get_catalog_products()
    
    @strawberry.field
    async def orders_with_products(self) -> list[OrderWithProduct]:
        """Получить заказы вместе с информацией о товарах"""
        orders = await get_mock_orders()
        products = await get_catalog_products()
        
        result = []
        for order in orders:
            product = next((p for p in products if p.id == order.product_id), None)
            if product:
                result.append(OrderWithProduct(
                    order_id=order.order_id,
                    date=order.date,
                    product=product
                ))
        return result


schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)

app = FastAPI(title="GraphQL Gateway")
app.include_router(graphql_app, prefix="/graphql")

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "graphql-gateway"}