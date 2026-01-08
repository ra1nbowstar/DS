# core/wx_pay_client.py
import json
import time
import uuid
import base64
from typing import Dict, Any, Optional
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import requests
import os
from core.config import (
    WX_MCHID, WX_CERT_SERIAL_NO, WX_APIV3_KEY,
    WX_PRIVATE_KEY_PATH, WX_PAY_BASE_URL, WECHATPAY_CERT_PATH, WX_WECHATPAY_SERIAL
)
from core.database import get_conn
from core.logging import get_logger

logger = get_logger(__name__)


class WeChatPayClient:
    """微信支付V3 API客户端（支持Mock模式）"""

    def __init__(self):
        # ✅ 核心修复：必须最先初始化 mock_mode
        self.mock_mode = os.getenv('WX_MOCK_MODE', 'true').lower() == 'true'
        if self.mock_mode:
            logger.warning("⚠️ 【MOCK模式】已启用，所有微信接口调用均为模拟！")

        # 再初始化其他属性
        self.mchid = WX_MCHID
        self.cert_serial_no = WX_CERT_SERIAL_NO
        self.apiv3_key = WX_APIV3_KEY.encode('utf-8')
        self.base_url = WX_PAY_BASE_URL

        # Mock模式下不强制加载证书
        self.private_key = self._load_private_key()
        self.wechat_public_key = self._load_wechat_public_key()

        # Mock模式下自动创建测试数据
        if self.mock_mode:
            self._ensure_mock_applyment_exists()

    def _is_mock_mode(self) -> bool:
        return self.mock_mode

    # ==================== MOCK数据生成器 ====================
    def _ensure_mock_applyment_exists(self):
        """确保Mock模式下有测试用的进件记录"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT user_id FROM wx_applyment 
                        WHERE user_id = 1 AND applyment_state = 'APPLYMENT_STATE_FINISHED'
                    """)
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO wx_applyment 
                            (user_id, business_code, sub_mchid, applyment_state, is_draft,
                             subject_type, subject_info, contact_info, bank_account_info)
                            VALUES (1, 'MOCK_BUSINESS_001', 'MOCK_SUB_MCHID_001', 
                                    'APPLYMENT_STATE_FINISHED', 0,
                                    'SUBJECT_TYPE_INDIVIDUAL', '{}', '{}', '{}')
                        """)
                        conn.commit()
                        logger.info("✅ Mock模式：已自动创建测试进件记录 (user_id=1)")
        except Exception as e:
            logger.debug(f"Mock初始化失败（可忽略）: {e}")

    def _generate_mock_application_no(self, sub_mchid: str) -> str:
        """生成模拟的申请单号"""
        return f"MOCK_APP_{int(time.time())}_{sub_mchid}"

    # core/wx_pay_client.py

    def _get_mock_settlement_data(self, sub_mchid: str) -> Dict[str, Any]:
        """模拟微信结算账户查询返回 - 从数据库读取实际数据"""
        logger.info(f"【MOCK】模拟查询结算账户: sub_mchid={sub_mchid} (从数据库读取)")

        # 在 Mock 模式下，直接从数据库读取最新的结算账户信息
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT account_bank, bank_name, 
                               account_number_encrypted, account_name_encrypted,
                               bank_address_code
                        FROM merchant_settlement_accounts
                        WHERE sub_mchid = %s AND status = 1
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        (sub_mchid,)
                    )
                    record = cur.fetchone()

                    if record:
                        # 使用与 BankcardService 相同的解密逻辑
                        try:
                            key = self.apiv3_key
                            if not key:
                                raise Exception("APIv3 key not loaded")

                            # 解密卡号
                            combined = base64.b64decode(record['account_number_encrypted'])
                            iv, ciphertext = combined[:12], combined[12:]
                            aesgcm = AESGCM(key)
                            full_number = aesgcm.decrypt(iv, ciphertext, b'').decode('utf-8')

                            # 解密户名
                            combined_name = base64.b64decode(record['account_name_encrypted'])
                            iv_name, ciphertext_name = combined_name[:12], combined_name[12:]
                            full_name = aesgcm.decrypt(iv_name, ciphertext_name, b'').decode('utf-8')

                            # 模拟微信脱敏格式：显示前6后4，中间用星号
                            if len(full_number) > 10:
                                masked_number = full_number[:6] + '*' * (len(full_number) - 10) + full_number[-4:]
                            else:
                                masked_number = full_number

                            return {
                                'account_type': 'ACCOUNT_TYPE_PRIVATE',  # Mock 模式下默认个人
                                'account_bank': record['account_bank'],
                                'bank_name': record['bank_name'] or record['account_bank'],
                                'account_number': masked_number,  # 脱敏后的卡号
                                'account_name': full_name,
                                'verify_result': 'VERIFY_SUCCESS',
                                'verify_fail_reason': '',
                                'bank_address_code': record['bank_address_code'] or '100000'
                            }
                        except Exception as e:
                            logger.warning(f"Mock 解密失败: {e}")

                    # 如果数据库没有数据，返回默认 Mock 数据
                    logger.info("【MOCK】未找到数据库记录，返回默认数据")
                    return {
                        'account_type': 'ACCOUNT_TYPE_PRIVATE',
                        'account_bank': '工商银行',
                        'bank_name': '中国工商银行股份有限公司北京朝阳支行',
                        'account_number': '6222021234567890000',
                        'account_name': '测试用户',
                        'verify_result': 'VERIFY_SUCCESS',
                        'verify_fail_reason': '',
                        'bank_address_code': '100000'
                    }
        except Exception as e:
            logger.warning(f"Mock 读取数据库失败: {e}")
            # 如果数据库查询失败，返回默认 Mock 数据
            return {
                'account_type': 'ACCOUNT_TYPE_PRIVATE',
                'account_bank': '工商银行',
                'bank_name': '中国工商银行股份有限公司北京朝阳支行',
                'account_number': '6222021234567890000',
                'account_name': '测试用户',
                'verify_result': 'VERIFY_SUCCESS',
                'verify_fail_reason': '',
                'bank_address_code': '100000'
            }

    def _get_mock_application_status(self, application_no: str) -> Dict[str, Any]:
        """模拟微信申请状态查询"""
        try:
            app_time = int(application_no.split('_')[2])
            elapsed = time.time() - app_time
        except:
            elapsed = 999

        if elapsed < 5:
            return {
                'applyment_state': 'APPLYMENT_STATE_AUDITING',
                'applyment_state_msg': '审核中，请稍后...'
            }
        else:
            if os.getenv('WX_MOCK_APPLY_RESULT') == 'FAIL':
                return {
                    'applyment_state': 'APPLYMENT_STATE_REJECTED',
                    'applyment_state_msg': '银行账户信息有误'
                }
            return {
                'applyment_state': 'APPLYMENT_STATE_FINISHED',
                'applyment_state_msg': '审核通过'
            }

    # ==================== 核心方法 ====================
    def _load_private_key(self):
        """加载商户私钥（PEM格式）"""
        try:
            with open(WX_PRIVATE_KEY_PATH, 'rb') as f:
                return serialization.load_pem_private_key(
                    f.read(),
                    password=None,
                    backend=default_backend()
                )
        except Exception as e:
            logger.error(f"加载微信支付私钥失败: {e}")
            # ✅ Mock模式下不强制要求证书
            if not self.mock_mode:
                raise
            return None  # Mock模式下返回None

    def _load_wechat_public_key(self):
        """加载微信支付平台公钥"""
        try:
            with open(WECHATPAY_CERT_PATH, 'rb') as f:
                return serialization.load_pem_public_key(
                    f.read(),
                    backend=default_backend()
                )
        except Exception as e:
            logger.warning(f"加载微信支付公钥失败: {e}")
            # ✅ Mock模式下不强制要求证书
            if not self.mock_mode:
                raise
            return None

    def _rsa_encrypt_with_wechat_public_key(self, plaintext: str) -> str:
        """使用微信支付平台公钥加密"""
        if not self.wechat_public_key:
            raise Exception("微信支付公钥未加载")
        ciphertext = self.wechat_public_key.encrypt(
            plaintext.encode('utf-8'),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return base64.b64encode(ciphertext).decode('utf-8')

    def _sign(self, method: str, url: str, timestamp: str, nonce_str: str, body: str = '') -> str:
        """RSA-SHA256签名"""
        sign_str = f'{method}\n{url}\n{timestamp}\n{nonce_str}\n{body}\n'
        signature = self.private_key.sign(
            sign_str.encode('utf-8'),
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

    def _build_auth_header(self, method: str, url: str, body: str = '') -> str:
        """构建Authorization请求头"""
        timestamp = str(int(time.time()))
        nonce_str = str(uuid.uuid4()).replace('-', '')
        signature = self._sign(method, url, timestamp, nonce_str, body)
        auth_str = f'mchid="{self.mchid}",serial_no="{self.cert_serial_no}",nonce_str="{nonce_str}",timestamp="{timestamp}",signature="{signature}"'
        return f'WECHATPAY2-SHA256-RSA2048 {auth_str}'

    def query_settlement_account(self, sub_mchid: str) -> Dict[str, Any]:
        """查询结算账户（GET）"""
        if self._is_mock_mode():
            logger.info(f"【MOCK】模拟查询结算账户: sub_mchid={sub_mchid}")
            return self._get_mock_settlement_data(sub_mchid)

        url = f'/v3/apply4sub/sub_merchants/{sub_mchid}/settlement'
        headers = {'Authorization': self._build_auth_header('GET', url), 'Accept': 'application/json'}
        response = requests.get(self.base_url + url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return {
            'account_type': data.get('account_type'),
            'account_bank': data.get('account_bank'),
            'bank_name': data.get('bank_name'),
            'account_number': data.get('account_number'),
            'account_name': data.get('account_name'),
            'verify_result': data.get('verify_result', 'VERIFYING'),
            'verify_fail_reason': data.get('verify_fail_reason', '')
        }

    def modify_settlement_account(self, sub_mchid: str, account_info: Dict[str, Any]) -> Dict[str, Any]:
        """修改结算账户（POST）"""
        if self._is_mock_mode():
            logger.info(f"【MOCK】模拟提交改绑申请: sub_mchid={sub_mchid}")
            return {
                'application_no': self._generate_mock_application_no(sub_mchid),
                'sub_mchid': sub_mchid,
                'status': 'APPLYMENT_STATE_AUDITING'
            }

        url = f'/v3/apply4sub/sub_merchants/{sub_mchid}/modify-settlement'
        body = {
            "account_type": account_info['account_type'],
            "account_bank": account_info['account_bank'],
            "bank_name": account_info.get('bank_name', ''),
            "bank_branch_id": account_info.get('bank_branch_id', ''),
            "bank_address_code": account_info['bank_address_code'],
            "account_number": self._rsa_encrypt_with_wechat_public_key(account_info['account_number']),
            "account_name": self._rsa_encrypt_with_wechat_public_key(account_info['account_name'])
        }
        body_str = json.dumps(body, ensure_ascii=False)
        headers = {
            'Authorization': self._build_auth_header('POST', url, body_str),
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Wechatpay-Serial': WX_WECHATPAY_SERIAL
        }
        response = requests.post(self.base_url + url, data=body_str.encode('utf-8'), headers=headers, timeout=30)
        response.raise_for_status()
        logger.info(f"微信修改结算账户成功: sub_mchid={sub_mchid}")
        return response.json()

    def query_application_status(self, sub_mchid: str, application_no: str) -> Dict[str, Any]:
        """查询申请单状态（GET）"""
        if self._is_mock_mode():
            logger.info(f"【MOCK】模拟查询改绑状态: application_no={application_no}")
            return self._get_mock_application_status(application_no)

        url = f'/v3/apply4sub/sub_merchants/{sub_mchid}/application/{application_no}'
        headers = {'Authorization': self._build_auth_header('GET', url), 'Accept': 'application/json'}
        response = requests.get(self.base_url + url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()


# 全局客户端实例
wxpay_client = WeChatPayClient()