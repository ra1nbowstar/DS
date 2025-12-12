from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, PositiveInt
from core.database import get_conn
from typing import List, Dict, Any

router = APIRouter()

class CartManager:
    @staticmethod
    def add(user_id: int, product_id: int, quantity: int = 1) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 用户是否存在
                cur.execute("SELECT 1 FROM users WHERE id = %s", (user_id,))
                if not cur.fetchone():
                    raise HTTPException(
                        status_code=404, detail=f"users 表中不存在 id={user_id}"
                    )

                # 2. 商品是否存在
                cur.execute("SELECT 1 FROM products WHERE id = %s", (product_id,))
                if not cur.fetchone():
                    raise HTTPException(
                        status_code=404, detail=f"products 表中不存在 id={product_id}"
                    )

                # 3. 插入或更新
                cur.execute(
                    "SELECT quantity FROM cart WHERE user_id=%s AND product_id=%s",
                    (user_id, product_id),
                )
                row = cur.fetchone()
                if row:
                    cur.execute(
                        "UPDATE cart SET quantity = quantity + %s "
                        "WHERE user_id=%s AND product_id=%s",
                        (quantity, user_id, product_id),
                    )
                else:
                    cur.execute(
                        "INSERT INTO cart(user_id, product_id, quantity) VALUES (%s,%s,%s)",
                        (user_id, product_id, quantity),
                    )
                conn.commit()
                return True

    @staticmethod
    def list_items(user_id: int) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 主查 SKU 价格，保留 product_name 用于展示
                sql = """
                    SELECT c.*,
                           p.name           AS product_name,
                           s.price          AS unit_price,
                           (c.quantity * s.price) AS total_price
                    FROM cart c
                    JOIN products p ON c.product_id = p.id
                    JOIN product_skus s ON s.product_id = c.product_id
                    WHERE c.user_id = %s
                    ORDER BY c.added_at DESC
                """
                cur.execute(sql, (user_id,))
                rows = cur.fetchall()
                for r in rows:  # 处理 Decimal
                    r["unit_price"] = float(r["unit_price"])
                    r["total_price"] = float(r["total_price"])
                return rows

    @staticmethod
    def remove(user_id: int, product_id: int) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM cart WHERE user_id=%s AND product_id=%s",
                    (user_id, product_id),
                )
                conn.commit()
                return True

# ----------- 请求模型（无下拉框） -----------
class CartAdd(BaseModel):
    user_id: int
    product_id: int
    quantity: PositiveInt = 1

# ----------- 路由 -----------
@router.post("/add", summary="添加商品到购物车")
def cart_add(body: CartAdd):
    return {"ok": CartManager.add(body.user_id, body.product_id, body.quantity)}

@router.get("/{user_id}", summary="获取购物车列表")
def get_cart(user_id: int):
    return CartManager.list_items(user_id)

@router.delete("/{user_id}/{product_id}", summary="从购物车移除商品")
def cart_remove(user_id: int, product_id: int):
    return {"ok": CartManager.remove(user_id, product_id)}
