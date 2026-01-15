# api/wechat_pay/routes.py
from fastapi import APIRouter, Request, HTTPException
from core.wx_pay_client import WeChatPayClient
from core.config import ENVIRONMENT
from core.response import success_response
from core.database import get_conn
from services.finance_service import FinanceService
from decimal import Decimal
from services.wechat_applyment_service import WechatApplymentService
import json
import logging
import xml.etree.ElementTree as ET  # 用于生成XML响应

router = APIRouter(prefix="/wechat-pay", tags=["微信支付"])

logger = logging.getLogger(__name__)
pay_client = WeChatPayClient()


@router.post("/create-order", summary="创建JSAPI订单并返回前端支付参数")
async def create_jsapi_order(request: Request):
    """创建 JSAPI 订单并返回前端调用 `wx.requestPayment`/小程序支付所需参数。
    请求 JSON 应包含：out_trade_no/order_id, total_fee(分), openid, description(可选)
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON payload")

    out_trade_no = payload.get('out_trade_no') or payload.get('order_id')
    total_fee = payload.get('total_fee')
    openid = payload.get('openid')
    description = payload.get('description', '商品支付')

    if not out_trade_no or not total_fee or not openid:
        raise HTTPException(status_code=400, detail="missing out_trade_no/total_fee/openid")

    try:
        # 幂等校验：确保订单存在且处于待支付状态
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, user_id, status, delivery_way FROM orders WHERE order_number=%s", (out_trade_no,))
                order_row = cur.fetchone()
                if not order_row:
                    raise HTTPException(status_code=404, detail="order not found")
                if order_row.get('status') != 'pending_pay':
                    raise HTTPException(status_code=400, detail="order not in pending_pay state")

        # 1) 调用微信下单，获取 prepay_id
        resp = pay_client.create_jsapi_order(out_trade_no=str(out_trade_no), total_fee=int(total_fee), openid=str(openid), description=description)
        prepay_id = resp.get('prepay_id') or resp.get('prepayId')
        if not prepay_id:
            logger.error(f"下单失败，微信返回: {resp}")
            raise HTTPException(status_code=500, detail="wechat create order failed")

        # 2) 生成前端支付参数（含 paySign）
        pay_params = pay_client.generate_jsapi_pay_params(prepay_id)

        return {
            "prepay_id": prepay_id,
            "pay_params": pay_params,
            "wechat_raw_response": resp
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("创建JSAPI订单失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/notify", summary="微信支付回调通知")
async def wechat_pay_notify(request: Request):
    """
    处理微信支付异步通知
    1. 验证签名
    2. 解密回调数据
    3. 更新订单/进件状态
    4. 返回成功响应
    """
    try:
        body = await request.body()
        # 调试：记录收到的原始请求体及长度（repr 格式，便于发现隐藏字符）
        try:
            logger.debug(f"收到原始请求体 ({len(body)} bytes): {body!r}")
            logger.debug(f"请求头 Content-Type: {headers.get('content-type') if 'headers' in locals() else request.headers.get('content-type')}")
        except Exception:
            logger.debug("无法记录原始请求体（调试日志）")

        # 检查请求体是否为空（防止JSONDecodeError）
        if not body or len(body.strip()) == 0:
            logger.warning("收到空请求体，返回错误响应")
            return _xml_response("FAIL", "Empty request body")

        headers = request.headers

        # 验证签名头
        signature = headers.get("Wechatpay-Signature")
        timestamp = headers.get("Wechatpay-Timestamp")
        nonce = headers.get("Wechatpay-Nonce")
        serial = headers.get("Wechatpay-Serial")

        # 开发绕过：允许在非 production 环境下通过自定义头跳过签名校验（仅用于本地/测试）
        bypass_header = headers.get("X-DEV-BYPASS-VERIFY") or headers.get("X-DEV-BYPASS")
        # 支持基于共享测试令牌的绕过（在 systemd/.env 中设置 TEST_NOTIFY_TOKEN）
        test_token_header = headers.get("X-DEV-TEST-TOKEN")
        test_token_env = None
        try:
            import os

            test_token_env = os.getenv("TEST_NOTIFY_TOKEN")
        except Exception:
            test_token_env = None

        if (bypass_header and ENVIRONMENT != "production") or (
            test_token_header and test_token_env and test_token_header == test_token_env
        ):
            logger.warning("开发模式：绕过回调签名校验（开发头或测试令牌触发）")
        else:
            if not all([signature, timestamp, nonce, serial]):
                logger.error("缺少必要的回调头信息")
                return _xml_response("FAIL", "Missing callback headers")

            try:
                if not pay_client.verify_signature(signature, timestamp, nonce, body.decode()):
                    logger.error("签名验证失败")
                    return _xml_response("FAIL", "Signature verification failed")
            except Exception as e:
                logger.error(f"签名验证异常: {str(e)}")
                return _xml_response("FAIL", f"Signature error: {str(e)}")

        # 支持开发调试绕过签名验证（兼容性备用头）
        if headers.get("X-Bypass-Signature", "").lower() == "true" and ENVIRONMENT != "production":
            logger.warning("开发模式：跳过签名验证 (X-Bypass-Signature)")

        # 解析回调数据（真实微信通知是JSON，部分测试可能使用XML包装）
        content_type = headers.get("content-type", "")
        if "xml" in content_type:
            import xmltodict  # 需要安装: pip install xmltodict

            data_dict = xmltodict.parse(body)
            data = data_dict.get("xml", {})
            if "resource" in data:
                resource = data["resource"]
                if isinstance(resource, str):
                    data = json.loads(resource)
                else:
                    data = {"resource": resource}
            else:
                data = {"resource": data}
        else:
            data = json.loads(body)

        # 解密回调数据
        resource = data.get("resource", {})
        if not resource:
            logger.error("回调数据中缺少resource字段")
            return _xml_response("FAIL", "Missing resource")

        # 开发绕过：若请求头包含 X-DEV-PLAIN-BODY，则认为 resource 已是明文 JSON（跳过 decrypt）
        plain_header = headers.get("X-DEV-PLAIN-BODY") or headers.get("X-DEV-PLAIN")
        if plain_header and ENVIRONMENT != "production":
            logger.info("开发模式：跳过回调解密，直接使用明文 resource（X-DEV-PLAIN-BODY detected）")
            decrypted_data = resource
        else:
            decrypted_data = pay_client.decrypt_callback_data(resource)

        # 根据事件类型处理
        event_type = decrypted_data.get("event_type")

        if event_type == "APPLYMENT_STATE_CHANGE":
            await handle_applyment_state_change(decrypted_data)
            return _xml_response("SUCCESS", "OK")
        elif event_type == "TRANSACTION.SUCCESS":
            await handle_transaction_success(decrypted_data)
            return _xml_response("SUCCESS", "OK")
        else:
            logger.warning(f"未知的事件类型: {event_type}")
            return _xml_response("FAIL", f"Unknown event_type: {event_type}")

    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {str(e)}")
        return _xml_response("FAIL", "Invalid JSON format")
    except Exception as e:
        logger.error(f"微信支付回调处理失败: {str(e)}", exc_info=True)
        return _xml_response("FAIL", str(e))


def _xml_response(code: str, message: str) -> str:
    """
    生成微信支付回调要求的XML格式响应
    微信要求返回格式：
    <xml>
        <return_code><![CDATA[SUCCESS/FAIL]]></return_code>
        <return_msg><![CDATA[OK/错误信息]]></return_msg>
    </xml>
    """
    return f"""<xml>
<return_code><![CDATA[{code}]]></return_code>
<return_msg><![CDATA[{message}]]></return_msg>
</xml>"""


async def handle_applyment_state_change(data: dict):
    """处理进件状态变更回调"""
    try:
        applyment_id = data.get("applyment_id")
        state = data.get("applyment_state")

        if not applyment_id or not state:
            logger.error("进件回调缺少必要字段")
            return

        service = WechatApplymentService()
        await service.handle_applyment_state_change(
            applyment_id,
            state,
            {
                "state_msg": data.get("state_msg"),
                "sub_mchid": data.get("sub_mchid"),
            },
        )
        logger.info(f"进件状态更新成功: {applyment_id} -> {state}")
    except Exception as e:
        logger.error(f"进件状态处理失败: {str(e)}", exc_info=True)


async def handle_transaction_success(data: dict):
    """处理支付成功回调"""
    try:
        out_trade_no = data.get("out_trade_no")
        transaction_id = data.get("transaction_id")
        amount = data.get("amount", {}).get("total")

        if not out_trade_no:
            logger.error("支付回调缺少out_trade_no")
            return

        logger.info(f"支付成功: 订单号={out_trade_no}, 微信流水号={transaction_id}, 金额={amount}")

        # 支付成功后的业务逻辑：
        # 1. 在 orders 表中查找订单并幂等更新（仅处理 pending_pay）
        # 2. 调用 FinanceService.settle_order 完成结算（积分/分账/流水）
        # 3. 更新订单状态为 pending_ship 或 pending_recv（或 completed，对于虚拟/线下场景）
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, user_id, status, delivery_way FROM orders WHERE order_number=%s",
                        (out_trade_no,)
                    )
                    order = cur.fetchone()
                    if not order:
                        logger.warning(f"支付回调：未找到订单 {out_trade_no}")
                        return

                    if order.get('status') != 'pending_pay':
                        logger.info(f"支付回调：订单 {out_trade_no} 状态为 {order.get('status')}，已处理，忽略")
                        return

                    order_id = order['id']
                    user_id = order.get('user_id') or 0

                    # amount 单位为分，转换为 Decimal 元（FinanceService 内部可能期望元）
                    pay_amount = None
                    try:
                        if amount is not None:
                            pay_amount = Decimal(str(amount)) / Decimal('100')
                    except Exception:
                        pay_amount = None

                    # 结算（在同一连接上执行以保证事务一致性）
                    fs = FinanceService()
                    try:
                        fs.settle_order(
                            order_no=out_trade_no,
                            user_id=user_id,
                            order_id=order_id,
                            points_to_use=Decimal('0'),
                            coupon_discount=Decimal('0'),
                            external_conn=conn
                        )
                    except Exception as e:
                        logger.exception(f"结算失败 for order {out_trade_no}: {e}")
                        # 不抛出，继续尝试更新状态以避免微信重复回调带来的多次结算


                    # 确保订单已完成分账（防止某些路径未执行分账）——幂等检查
                    try:
                        cur.execute("SELECT COUNT(1) AS c FROM account_flow WHERE remark LIKE %s", (f"订单分账: {out_trade_no}%",))
                        cnt_row = cur.fetchone()
                        cnt = cnt_row['c'] if isinstance(cnt_row, dict) else (cnt_row[0] if cnt_row else 0)
                    except Exception:
                        cnt = 0

                    if not cnt:
                        # 尝试从 orders 读取金额与 is_vip_item
                        try:
                            cur.execute("SELECT total_amount, is_vip_item FROM orders WHERE order_number=%s", (out_trade_no,))
                            oinfo = cur.fetchone()
                            total_amt = Decimal(str(oinfo.get('total_amount'))) if oinfo and oinfo.get('total_amount') is not None else None
                            is_vip = bool(oinfo.get('is_vip_item')) if oinfo else False
                            if total_amt is not None:
                                from services.finance_service import split_order_funds

                                try:
                                    split_order_funds(out_trade_no, total_amt, is_vip, cursor=cur)
                                except Exception as e:
                                    logger.warning(f"尝试执行分账失败: {e}")
                        except Exception as e:
                            logger.debug(f"读取订单金额用于分账失败: {e}")

                    # 尝试把微信的 transaction_id / success_time 写入 orders（如果表存在对应列）
                    try:
                        # 检查 transaction_id 列
                        cur.execute("SHOW COLUMNS FROM orders LIKE 'transaction_id'")
                        if cur.fetchone():
                            cur.execute("UPDATE orders SET transaction_id=%s WHERE id=%s", (transaction_id, order_id))
                        # 检查 pay_time 或 paid_at 列
                        pay_time_val = data.get('success_time') or data.get('time_end')
                        if pay_time_val:
                            cur.execute("SHOW COLUMNS FROM orders LIKE 'pay_time'")
                            if cur.fetchone():
                                # 支持两种时间格式：YYYYMMDDHHMMSS 或 ISO8601
                                if isinstance(pay_time_val, str) and pay_time_val.isdigit():
                                    cur.execute("UPDATE orders SET pay_time=STR_TO_DATE(%s,'%%Y%%m%%d%%H%%i%%s') WHERE id=%s", (pay_time_val, order_id))
                                else:
                                    cur.execute("UPDATE orders SET pay_time=%s WHERE id=%s", (pay_time_val, order_id))
                            else:
                                cur.execute("SHOW COLUMNS FROM orders LIKE 'paid_at'")
                                if cur.fetchone():
                                    if isinstance(pay_time_val, str) and pay_time_val.isdigit():
                                        cur.execute("UPDATE orders SET paid_at=STR_TO_DATE(%s,'%%Y%%m%%d%%H%%i%%s') WHERE id=%s", (pay_time_val, order_id))
                                    else:
                                        cur.execute("UPDATE orders SET paid_at=%s WHERE id=%s", (pay_time_val, order_id))
                    except Exception as e:
                        logger.debug(f"尝试写入交易信息到 orders 表失败: {e}")

                    # 更新订单状态
                    next_status = "pending_recv" if order.get('delivery_way') == 'pickup' else "pending_ship"
                    cur.execute(
                        "UPDATE orders SET status=%s, updated_at=NOW() WHERE id=%s AND status='pending_pay'",
                        (next_status, order_id)
                    )
                    if cur.rowcount == 0:
                        logger.warning(f"订单 {out_trade_no} 状态更新未生效，可能已被并发处理")
                    conn.commit()

        except Exception as e:
            logger.exception(f"支付成功业务处理异常: {e}")
            return

    except Exception as e:
        logger.error(f"支付成功回调处理失败: {str(e)}", exc_info=True)


def register_wechat_pay_routes(app):
    """
    注册微信支付路由
    注意：prefix 已在 router 中定义，这里不需要重复
    """
    app.include_router(router)