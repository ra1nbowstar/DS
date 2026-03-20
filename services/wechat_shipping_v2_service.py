# services/wechat_shipping_v2.py
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

import requests
from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)

# 物流类型映射：将你系统中的 delivery_way 映射为微信要求的枚举值
# 1: 实体物流, 2: 同城配送, 3: 虚拟商品, 4: 用户自提
LOGISTICS_TYPE_MAP = {
    "platform": 1,
    "express": 1,
    "pickup": 4,
    "same_city": 2,
    "virtual": 3,
}

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
            resp = requests.post(url, json=payload, timeout=15)
            resp.raise_for_status()
            result = resp.json()
            if result.get("errcode") != 0:
                logger.error(f"微信发货API调用失败: endpoint={endpoint}, errcode={result.get('errcode')}, errmsg={result.get('errmsg')}")
            return result
        except requests.RequestException as e:
            logger.error(f"微信发货API请求异常: {e}")
            raise

    def upload_shipping_info(self, transaction_id: str, openid: str, logistics_type: int,
                             shipping_list: List[Dict[str, Any]], delivery_mode: int = 1,
                             is_all_delivered: bool = True) -> dict:
        """
        发货信息录入接口 (核心)
        :param transaction_id: 微信支付单号
        :param openid: 用户openid
        :param logistics_type: 物流类型 (1快递,2同城,3虚拟,4自提)
        :param shipping_list: 物流信息列表，例如 [{"tracking_no": "SF123456", "express_company": "SF", "item_desc": "商品描述"}]
        :param delivery_mode: 发货模式，1统一发货，2分拆发货
        :param is_all_delivered: 分拆发货时是否全部发货完成
        """
        order_key = {
            "order_number_type": 2,
            "transaction_id": transaction_id
        }
        payload = {
            "order_key": order_key,
            "logistics_type": logistics_type,
            "delivery_mode": delivery_mode,
            "shipping_list": shipping_list,
            "upload_time": datetime.now().astimezone().isoformat(timespec='milliseconds'),
            "payer": {"openid": openid}
        }
        if delivery_mode == 2:
            payload["is_all_delivered"] = is_all_delivered

        logger.info(f"调用微信发货信息录入接口: transaction_id={transaction_id}")
        return self._request("/upload_shipping_info", payload)

    def notify_confirm_receive(self, transaction_id: str, received_time: int) -> dict:
        """
        确认收货提醒接口
        :param transaction_id: 微信支付单号
        :param received_time: 快递签收时间 (unix时间戳)
        """
        payload = {
            "transaction_id": transaction_id,
            "received_time": received_time
        }
        logger.info(f"调用微信确认收货提醒接口: transaction_id={transaction_id}")
        return self._request("/notify_confirm_receive", payload)

    def get_order(self, transaction_id: str) -> dict:
        """查询订单发货状态"""
        payload = {"transaction_id": transaction_id}
        result = self._request("/get_order", payload)
        return result.get("order", {})

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