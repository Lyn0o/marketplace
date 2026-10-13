import strawberry
import httpx
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter


@strawberry.type(description="Товар из каталога")
class Product:
    id: int = strawberry.field(description="Уникальный идентификатор товара")
    name: str = strawberry.field(description="Название товара")
    price: float = strawberry.field(description="Цена товара в рублях")

@strawberry.type(description="Позиция (строка) в заказе")
class OrderItem:
    product: Product = strawberry.field(description="Информация о товаре")
    quantity: int = strawberry.field(description="Количество единиц товара")

@strawberry.type(description="Заказ пользователя")
class Order:
    id: int = strawberry.field(description="Уникальный идентификатор заказа")
    user_id: int = strawberry.field(description="ID пользователя, сделавшего заказ")
    created_at: str = strawberry.field(description="Дата и время создания заказа")
    items: list[OrderItem] = strawberry.field(description="Список товаров в заказе")


async def get_catalog_products() -> dict[int, Product]:
    """Получаем товары из Catalog сервиса и возвращаем словарь {id: Product} для быстрого поиска"""
    async with httpx.AsyncClient() as client:
        response = await client.get("http://catalog-1:8002/products")
        if response.status_code == 200:
            data = response.json()
            return {p["id"]: Product(**p) for p in data.get("products", [])}
    return {}

async def get_mock_orders(user_id: int) -> list[dict]:
    """Моковые заказы. Фильтруем по user_id."""
    all_orders = [
        {"id": 1, "user_id": 1, "created_at": "2024-01-15", "items": [{"product_id": 1, "quantity": 2}]},
        {"id": 2, "user_id": 1, "created_at": "2024-01-16", "items": [{"product_id": 2, "quantity": 1}]},
        {"id": 3, "user_id": 2, "created_at": "2024-01-17", "items": [{"product_id": 1, "quantity": 1}]},
    ]
    return [order for order in all_orders if order["user_id"] == user_id]


@strawberry.type
class Query:
    @strawberry.field(description="Получить все заказы пользователя с деталями товаров")
    async def orders_by_user_id(self, user_id: int) -> list[Order]:
        raw_orders = await get_mock_orders(user_id)
        
        products_dict = await get_catalog_products()
        
        result_orders = []
        for raw_order in raw_orders:
            order_items = []
            for item in raw_order["items"]:
                product = products_dict.get(item["product_id"])
                if product:
                    order_items.append(OrderItem(
                        product=product,
                        quantity=item["quantity"]
                    ))
            
            result_orders.append(Order(
                id=raw_order["id"],
                user_id=raw_order["user_id"],
                created_at=raw_order["created_at"],
                items=order_items
            ))
            
        return result_orders


schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)

app = FastAPI(title="GraphQL Gateway")
app.include_router(graphql_app, prefix="/graphql")

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "graphql-gateway"}