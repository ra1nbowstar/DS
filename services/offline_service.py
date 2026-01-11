# services/offline_service.py
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Optional

from core.database import get_conn
from core.config import settings
from core.logging import get_logger
from services.finance_service import FinanceService
from services.notify_service import notify_merchant
import aiomysql

logger = get_logger(__name__)


class OfflineService:
    # ---------- 1. 创建线下支付单 ----------
    @staticmethod
    async def create_order(
        merchant_id: int,
        store_name: str,
        amount: int,
        product_name: str = "",
        remark: str = "",
        user_id: Optional[int] = None,
    ) -> dict:
        import uuid
        order_no = f"OFF{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6]}"
        expire = datetime.now() + timedelta(seconds=settings.qrcode_expire_seconds)
        qrcode_url = f"https://your-domain.com/offline/pay?order_no={order_no}"

        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO offline_order "
                    "(order_no,merchant_id,user_id,store_name,amount,product_name,remark,"
                    "qrcode_url,qrcode_expire,status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1)",
                    (order_no, merchant_id, user_id, store_name, amount,
                     product_name, remark, qrcode_url, expire)
                )
                await conn.commit()

        logger.info(f"[Offline] 创建订单 {order_no} 金额 {amount}")
        return {"order_no": order_no, "qrcode_url": qrcode_url, "expire_at": expire}

    # ---------- 2. 刷新收款码（限 1 次） ----------
    @staticmethod
    async def refresh_qrcode(order_no: str, user_id: int) -> dict:
        expire = datetime.now() + timedelta(seconds=settings.qrcode_expire_seconds)
        new_url = f"https://your-domain.com/offline/pay?order_no={order_no}"

        async with get_conn() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT refresh_count,status FROM offline_order "
                    "WHERE order_no=%s AND merchant_id=%s",
                    (order_no, user_id)
                )
                row = await cur.fetchone()
                if not row or row["status"] != 1:
                    raise ValueError("订单不存在或状态异常")
                if row["refresh_count"] >= 1:
                    raise ValueError("收款码已刷新一次，请重新创建订单")

                await cur.execute(
                    "UPDATE offline_order SET qrcode_url=%s,qrcode_expire=%s,refresh_count=refresh_count+1 "
                    "WHERE order_no=%s",
                    (new_url, expire, order_no)
                )
                await conn.commit()

        logger.info(f"[Offline] 刷新码 {order_no}")
        return {"qrcode_url": new_url, "expire_at": expire}

    # ---------- 3. 订单详情 + 可用优惠券 ----------
    @staticmethod
    async def get_order_detail(order_no: str, user_id: int) -> dict:
        async with get_conn() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT order_no,amount,store_name,product_name,status "
                    "FROM offline_order WHERE order_no=%s", (order_no,)
                )
                order = await cur.fetchone()
                if not order:
                    raise ValueError("订单不存在")

                coupons = await FinanceService.list_available(user_id, order["amount"])
                for c in coupons:
                    c["amount"] = float(c["amount"])
                    c["threshold"] = float(c["threshold"])

        return {**order, "coupons": coupons}

    # ---------- 4. 统一下单（核销优惠券 + 调起支付） ----------
    @staticmethod
    async def unified_order(
        order_no: str,
        coupon_id: Optional[int],
        user_id: int,
    ) -> dict:
        async with get_conn() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT amount,status,merchant_id FROM offline_order WHERE order_no=%s",
                    (order_no,)
                )
                row = await cur.fetchone()
                if not row or row["status"] != 1:
                    raise ValueError("订单不可支付")
                amount: int = row["amount"]

                if coupon_id:
                    amount = await FinanceService.apply_coupon(
                        user_id=user_id, coupon_id=coupon_id, amount=amount
                    )

                await cur.execute(
                    "UPDATE offline_order SET amount=%s WHERE order_no=%s",
                    (amount, order_no)
                )
                await conn.commit()

        import uuid, time
        return {
            "appId": "wx123456",
            "timeStamp": str(int(time.time())),
            "nonceStr": uuid.uuid4().hex,
            "package": f"prepay_id=wx{int(time.time())}",
            "signType": "RSA",
            "paySign": "fake_sign"
        }

    # ---------- 5. 支付回调 ----------
    @staticmethod
    async def handle_notify():
        order_no = "OFF202601100001"  # 模拟
        async with get_conn() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT id,amount,merchant_id FROM offline_order WHERE order_no=%s AND status=1",
                    (order_no,)
                )
                order = await cur.fetchone()
                if not order:
                    raise ValueError("订单不存在或已处理")

                await cur.execute(
                    "UPDATE offline_order SET status=2,updated_at=NOW() WHERE order_no=%s",
                    (order_no,)
                )
                await cur.execute(
                    "INSERT INTO orders (order_number,user_id,merchant_id,total_amount,status,offline_order_flag,pay_way)"
                    "VALUES (%s,NULL,%s,%s,'completed',1,'wechat')",
                    (order_no, order["merchant_id"], order["amount"])
                )
                await conn.commit()

        await notify_merchant(order["merchant_id"], order_no, order["amount"])
        logger.info(f"[Offline] 支付完成 {order_no}")
        return "<xml><return_code><![CDATA[SUCCESS]]></return_code></xml>"

    # ---------- 6. 订单列表 ----------
    @staticmethod
    async def list_orders(merchant_id: int, page: int, size: int):
        offset = (page - 1) * size
        async with get_conn() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT order_no,store_name,amount,status,created_at "
                    "FROM offline_order WHERE merchant_id=%s "
                    "ORDER BY id DESC LIMIT %s OFFSET %s",
                    (merchant_id, size, offset)
                )
                rows = await cur.fetchall()
        return {"list": rows, "page": page, "size": size}

    # ---------- 7. 退款 ----------
    @staticmethod
    async def refund(order_no: str, refund_amount: Optional[int], user_id: int):
        async with get_conn() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT id,amount,status FROM offline_order WHERE order_no=%s AND merchant_id=%s",
                    (order_no, user_id)
                )
                row = await cur.fetchone()
                if not row or row["status"] != 2:
                    raise ValueError("订单未支付")
                amount = row["amount"]
                money = refund_amount or amount

                await cur.execute(
                    "UPDATE offline_order SET status=4 WHERE order_no=%s",
                    (order_no,)
                )
                await conn.commit()

        await FinanceService.refund_order(order_no)
        logger.info(f"[Offline] 退款 {order_no} 金额 {money}")
        return {"refund_no": f"REF{order_no}"}

    # ---------- 8. 收款码状态 ----------
    @staticmethod
    async def qrcode_status(order_no: str):
        async with get_conn() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT status,qrcode_expire FROM offline_order WHERE order_no=%s",
                    (order_no,)
                )
                row = await cur.fetchone()
                if not row:
                    raise ValueError("订单不存在")
                now = datetime.now()
                if row["status"] != 1:
                    return {"status": "paid" if row["status"] == 2 else "closed"}
                if row["qrcode_expire"] < now:
                    return {"status": "expired"}
                return {"status": "valid"}

    # ---------- 9. 供优惠券接口调用的原始订单 ----------
    @staticmethod
    async def get_raw_order(order_no: str):
        async with get_conn() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "SELECT order_no,amount,status FROM offline_order WHERE order_no=%s",
                    (order_no,)
                )
                return await cur.fetchone()