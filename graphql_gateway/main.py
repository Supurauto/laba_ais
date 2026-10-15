import logging
import os
from pathlib import Path
import httpx
from ariadne import QueryType, load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from ariadne.explorer import ExplorerPlayground
from graphql import GraphQLError

logger = logging.getLogger("uvicorn.error")
ORDER_URL = os.getenv("ORDER_URL", "http://order:8000").rstrip("/")
CATALOG_URL = os.getenv("CATALOG_URL", "http://catalog-1:8000").rstrip("/")
query = QueryType()

async def context_value(request, data):
    return {"request": request, "products": {}}

async def get_json(client, url, params, service):
    try:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except (httpx.HTTPError, ValueError) as error:
        logger.warning("Ошибка обращения к %s: %s", service, type(error).__name__)
        raise GraphQLError(f"Не удалось получить данные сервиса {service}") from error
    
    
@query.field("ordersByUserId")
async def resolve_orders_by_user_id(_, info, userId):
    try:
        user_id = int(userId)
        if user_id <= 0:
            raise ValueError
    except ValueError:
        raise GraphQLError("userId должен быть положительным целым числом")
    
    async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
        orders = await get_json(client, f"{ORDER_URL}/api/orders", {"userId": user_id}, "Order")    
        if not orders:
            return []
        product_ids = {item["productId"] for order in orders for item in order["items"]}	
        products_by_id = info.context["products"]
        missing_ids = product_ids - products_by_id.keys()
        if missing_ids:
            ids = ",".join(str(product_id) for product_id in sorted(missing_ids))
            page = await get_json(client, f"{CATALOG_URL}/api/catalog/products", {"ids": ids}, "Catalog")
            products_by_id.update({product["id"]: product for product in page["products"]})
            logger.info("Catalog: 1 пакетный запрос, ids=%s", ids) 
            
        absent_ids = product_ids - products_by_id.keys()
        if absent_ids:
            raise GraphQLError(f"В Catalog отсутствуют книги: {sorted(absent_ids)}")	
        
        return [
            {"id": order["id"], "userId": order["userId"], "createdAt": order["createdAt"],	
             "items": [{"quantity": item["quantity"], "product": products_by_id[item["productId"]]}
                        for item in order["items"]]}
            for order in orders
        ]


schema = make_executable_schema(load_schema_from_path(str(Path(__file__).with_name("schema.graphql"))), query)	
app = GraphQL(schema, context_value=context_value, introspection=True,
              explorer=ExplorerPlayground(), debug=False)