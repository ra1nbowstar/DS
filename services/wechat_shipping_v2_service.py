# services/wechat_shipping_v2.py
import json
import re
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from zoneinfo import ZoneInfo

import requests
from core.config import settings
from core.database import get_conn
from core.logging import get_logger

logger = get_logger(__name__)

# 支付成功瞬间调用发货录入时，微信侧常尚未同步，返回「支付单不存在」，需延迟重试（见开放社区）
ERR_WX_SHIPPING_PAYMENT_NOT_FOUND = 10060001
# 第 1 次立即请求，之后每次请求前的等待（秒）
PICKUP_UPLOAD_RETRY_DELAYS_SEC = (2, 4, 8, 16, 24)

# 物流类型映射：将你系统中的 delivery_way 映射为微信要求的枚举值
# 1: 实体物流, 2: 同城配送, 3: 虚拟商品, 4: 用户自提
LOGISTICS_TYPE_MAP = {
    "platform": 1,
    "express": 1,
    "pickup": 4,
    "same_city": 2,
    "virtual": 3,
}


def upload_pickup_shipping_to_wechat(order_number: str, transaction_id: str) -> None:
    """
    自提订单支付成功后向微信录入发货（logistics_type=4）。
    微信「小程序订单」需先发货录入才会从「待发货」进入待收货流程；本地已直接 pending_recv，
    商家端 ship 仅处理 pending_ship，故在此处补录。
    """
    tid = (transaction_id or "").strip()
    if not tid:
        logger.warning("自提同步微信发货跳过: 无 transaction_id, order=%s", order_number)
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT o.id, o.delivery_way, u.openid
                       FROM orders o JOIN users u ON o.user_id = u.id
                       WHERE o.order_number=%s""",
                    (order_number,),
                )
                row = cur.fetchone()
                if not row:
                    logger.warning("自提同步微信发货跳过: 订单不存在 %s", order_number)
                    return
                if row.get("delivery_way") != "pickup":
                    return
                openid = (row.get("openid") or "").strip()
                if not openid:
                    logger.warning("自提同步微信发货跳过: 无 openid, order=%s", order_number)
                    return
                cur.execute(
                    """
                    SELECT p.name
                    FROM order_items oi
                    JOIN products p ON oi.product_id = p.id
                    WHERE oi.order_id = %s
                    """,
                    (row["id"],),
                )
                products = cur.fetchall() or []
    except Exception as e:
        logger.error("自提同步微信发货查询失败 order=%s: %s", order_number, e, exc_info=True)
        return

    clean_names = [re.sub(r"[\r\n\t]", "", p["name"]) for p in products]
    joined = "、".join(clean_names)
    if len(joined) > 120:
        item_desc = joined[:117] + "…"
    else:
        item_desc = joined or "商品"

    mchid = (
        (getattr(settings, "WECHAT_PAY_MCH_ID", None) or getattr(settings, "WX_MCHID", None) or "")
        .strip()
    )
    shipping_list = [{"item_desc": item_desc}]

    try:
        wx_service = WechatShippingService()

        if mchid:
            wx_mch = wx_service.upload_shipping_info(
                "",
                openid,
                4,
                shipping_list,
                mchid=mchid,
                out_trade_no=order_number,
            )
            if wx_mch.get("errcode") == 0:
                logger.info("自提订单微信发货录入成功(商户单号): order=%s", order_number)
                return
            logger.info(
                "自提订单按商户单号录入未成功，将按微信支付单号重试: order=%s err=%s",
                order_number,
                wx_mch,
            )

        max_tx_attempts = 1 + len(PICKUP_UPLOAD_RETRY_DELAYS_SEC)
        last: Dict[str, Any] = {}
        for attempt in range(max_tx_attempts):
            if attempt > 0:
                wait_sec = PICKUP_UPLOAD_RETRY_DELAYS_SEC[attempt - 1]
                logger.info(
                    "自提发货录入遇支付单未同步，%ss 后进行第 %s/%s 次请求: order=%s",
                    wait_sec,
                    attempt + 1,
                    max_tx_attempts,
                    order_number,
                )
                time.sleep(wait_sec)
            last = wx_service.upload_shipping_info(
                tid,
                openid,
                4,
                shipping_list,
                delivery_mode=1,
            )
            code = last.get("errcode")
            if code == 0:
                logger.info(
                    "自提订单微信发货录入成功: order=%s tx=%s 第%s次请求",
                    order_number,
                    tid,
                    attempt + 1,
                )
                return
            if code != ERR_WX_SHIPPING_PAYMENT_NOT_FOUND:
                logger.error(
                    "自提订单微信发货录入失败: order=%s tx=%s err=%s",
                    order_number,
                    tid,
                    last,
                )
                return

        logger.error(
            "自提订单微信发货录入失败(多次重试仍支付单不存在): order=%s tx=%s err=%s",
            order_number,
            tid,
            last,
        )
    except Exception as e:
        logger.error("自提订单微信发货录入异常 order=%s: %s", order_number, e, exc_info=True)


class WechatShippingService:
    """微信小程序发货信息管理服务 V2"""

    BASE_URL = "https://api.weixin.qq.com/wxa/sec/order"

    def __init__(self):
        self.access_token = None
        self.token_expires_at = 0

    def _get_access_token(self) -> str:
        """获取并缓存 access_token"""
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token

        url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={settings.WECHAT_APP_ID}&secret={settings.WECHAT_APP_SECRET}"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            self.access_token = data.get("access_token")
            expires_in = data.get("expires_in", 7200)
            self.token_expires_at = time.time() + expires_in - 200
            logger.info("成功获取微信 access_token")
            return self.access_token
        except Exception as e:
            logger.error(f"获取微信 access_token 失败: {e}")
            raise

    def _request(self, endpoint: str, payload: dict) -> dict:
        """通用的微信API请求方法，处理 token 和错误"""
        url = f"{self.BASE_URL}{endpoint}?access_token={self._get_access_token()}"
        try:
            # 微信对请求体 UTF-8 校验较严；显式 charset 与 bytes 可避免 47007(not UTF8)
            body = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
            body_bytes = body.encode("utf-8")
            headers = {"Content-Type": "application/json; charset=utf-8"}
            resp = requests.post(url, data=body_bytes, headers=headers, timeout=15)
            resp.raise_for_status()
            result = resp.json()
            if result.get("errcode") != 0:
                logger.error(f"微信发货API调用失败: endpoint={endpoint}, errcode={result.get('errcode')}, errmsg={result.get('errmsg')}")
            return result
        except requests.RequestException as e:
            logger.error(f"微信发货API请求异常: {e}")
            raise

    def upload_shipping_info(
        self,
        transaction_id: str,
        openid: str,
        logistics_type: int,
        shipping_list: List[Dict[str, Any]],
        delivery_mode: int = 1,
        is_all_delivered: bool = True,
        *,
        mchid: Optional[str] = None,
        out_trade_no: Optional[str] = None,
    ) -> dict:
        """
        发货信息录入接口 (核心)
        :param transaction_id: 微信支付单号（order_number_type=2 时必填）
        :param mchid / out_trade_no: 同时传入时使用 order_number_type=1（商户订单号），支付回调当下更易命中
        """
        mchid = (mchid or "").strip()
        out_trade_no = (out_trade_no or "").strip()
        if mchid and out_trade_no:
            order_key: Dict[str, Any] = {
                "order_number_type": 1,
                "mchid": mchid,
                "out_trade_no": out_trade_no,
            }
            log_ident = f"order_number_type=1 out_trade_no={out_trade_no}"
        else:
            tid = (transaction_id or "").strip()
            if not tid:
                return {"errcode": -1, "errmsg": "缺少 transaction_id 或 mchid+out_trade_no"}
            order_key = {"order_number_type": 2, "transaction_id": tid}
            log_ident = f"order_number_type=2 transaction_id={tid}"

        # 微信要求: YYYY-MM-DDTHH:mm:ss+08:00（RFC3339，含时区冒号，不含毫秒）
        dt = datetime.now(ZoneInfo("Asia/Shanghai"))
        upload_time = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        if len(upload_time) >= 5 and upload_time[-5] in "+-":
            upload_time = upload_time[:-2] + ":" + upload_time[-2:]
        else:
            upload_time = dt.strftime("%Y-%m-%dT%H:%M:%S") + "+08:00"

        payload = {
            "order_key": order_key,
            "logistics_type": logistics_type,
            "delivery_mode": delivery_mode,
            "shipping_list": shipping_list,
            "upload_time": upload_time,
            "payer": {"openid": openid},
        }
        if delivery_mode == 2:
            payload["is_all_delivered"] = is_all_delivered

        logger.info("调用微信发货信息录入接口: %s", log_ident)
        logger.info("发货payload: %s", payload)
        return self._request("/upload_shipping_info", payload)

    def set_msg_jump_path(self, path: str) -> dict:
        """设置发货/确认收货消息点击后跳转的小程序页面路径（需已接入官方确认收货组件）。"""
        payload = {"path": (path or "").strip()}
        logger.info("调用微信 set_msg_jump_path: path=%s", payload["path"])
        return self._request("/set_msg_jump_path", payload)

    def notify_confirm_receive(
        self,
        transaction_id: str,
        received_time: int,
        merchant_id: str | None = None,
        merchant_trade_no: str | None = None,
    ) -> dict:
        """
        确认收货提醒接口
        :param transaction_id: 微信支付单号
        :param received_time: 快递签收时间 (unix时间戳)
        """
        payload: Dict[str, Any] = {
            "transaction_id": transaction_id,
            "received_time": received_time,
        }
        if merchant_id:
            payload["merchant_id"] = merchant_id
        if merchant_trade_no:
            payload["merchant_trade_no"] = merchant_trade_no
        logger.info(f"调用微信确认收货提醒接口: transaction_id={transaction_id}")
        return self._request("/notify_confirm_receive", payload)

    def get_order_response(self, transaction_id: str) -> dict:
        """查询订单发货状态（完整接口返回，含 errcode）"""
        payload = {"transaction_id": transaction_id}
        return self._request("/get_order", payload)

    def get_order(self, transaction_id: str) -> dict:
        """查询订单发货状态（仅 order 对象；失败时返回空 dict）"""
        result = self.get_order_response(transaction_id)
        if result.get("errcode") != 0:
            return {}
        return result.get("order") or {}

    def get_order_list(self, openid: Optional[str] = None, order_state: Optional[int] = None,
                       begin_time: Optional[int] = None, end_time: Optional[int] = None,
                       last_index: str = "", page_size: int = 20) -> dict:
        """查询订单列表"""
        payload = {"page_size": page_size}
        if openid:
            payload["openid"] = openid
        if order_state:
            payload["order_state"] = order_state
        if begin_time or end_time:
            payload["pay_time_range"] = {}
            if begin_time:
                payload["pay_time_range"]["begin_time"] = begin_time
            if end_time:
                payload["pay_time_range"]["end_time"] = end_time
        if last_index:
            payload["last_index"] = last_index

        return self._request("/get_order_list", payload)