# api/user/bankcard_routes.py
from fastapi import APIRouter, HTTPException, Depends, Request, Header, Query
from typing import Optional, List
from pydantic import BaseModel, Field, validator
import re

from services.bankcard_service import BankcardService
from core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter()


# bankcard_routes.py

class BankcardBindRequest(BaseModel):
    """银行卡绑定请求"""
    user_id: int = Field(..., description="用户ID")
    bank_name: str = Field(..., min_length=2, max_length=50, description="开户银行名称")
    bank_account: str = Field(..., min_length=16, max_length=30, description="银行卡号")
    account_name: str = Field(..., min_length=2, max_length=100, description="开户名称")
    bank_branch_id: Optional[str] = Field(None, max_length=128, description="开户行联行号")
    bank_address_code: str = Field(..., pattern=r'^\d{6}$', description="开户地区码(6位数字)")  # ✅ 必填项
    is_default: bool = Field(True, description="是否设为默认账户")

    @validator('bank_account')
    def validate_bank_account(cls, v):
        if not re.match(r'^\d{16,30}$', v):
            raise ValueError("银行卡号必须为16-30位数字")
        return v


class BankcardModifyRequest(BaseModel):
    """银行卡改绑请求"""
    user_id: int = Field(..., description="用户ID")
    new_bank_name: str = Field(..., min_length=2, max_length=50, description="新开户银行名称")
    new_bank_account: str = Field(..., min_length=16, max_length=30, description="新银行卡号")
    new_account_name: str = Field(..., min_length=2, max_length=100, description="新开户名称")
    bank_branch_id: Optional[str] = Field(None, max_length=128, description="新开户行联行号")
    bank_address_code: Optional[str] = Field(None, pattern=r'^\d{6}$', description="新开户地区码(6位数字)")

    @validator('new_bank_account')
    def validate_new_bank_account(cls, v):
        if not re.match(r'^\d{16,30}$', v):
            raise ValueError("银行卡号必须为16-30位数字")
        return v


def _err(msg: str, code: int = 400):
    raise HTTPException(status_code=code, detail={"msg": "error", "error": msg})


def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


@router.post("/bankcard/bind", summary="绑定银行卡", response_model=dict)
async def bind_bankcard(
        request: BankcardBindRequest,
        admin_key: Optional[str] = Header(None, description="管理员密钥"),
        client_ip: str = Depends(get_client_ip)
):
    """绑定银行卡接口"""
    try:
        result = BankcardService.bind_bankcard(
            user_id=request.user_id,
            bank_name=request.bank_name,
            bank_account=request.bank_account,
            account_name=request.account_name,
            bank_branch_id=request.bank_branch_id,
            bank_address_code=request.bank_address_code,
            is_default=request.is_default,
            admin_key=admin_key,
            ip_address=client_ip
        )
        return result
    except Exception as e:
        logger.error(f"绑定失败: {e}")
        _err(str(e))


@router.post("/bankcard/modify/apply", summary="申请改绑银行卡", response_model=dict)
async def apply_modify_bankcard(
        request: BankcardModifyRequest,
        admin_key: Optional[str] = Header(None, description="管理员密钥"),
        client_ip: str = Depends(get_client_ip)
):
    """申请改绑银行卡"""
    try:
        result = BankcardService.modify_bankcard(
            user_id=request.user_id,
            new_bank_name=request.new_bank_name,
            new_bank_account=request.new_bank_account,
            new_account_name=request.new_account_name,
            bank_branch_id=request.bank_branch_id,
            bank_address_code=request.bank_address_code,
            admin_key=admin_key,
            ip_address=client_ip
        )
        return result
    except Exception as e:
        logger.error(f"改绑申请失败: {e}")
        _err(str(e))


@router.get("/bankcard/modify/status", summary="查询改绑审核状态", response_model=dict)
async def get_modify_status(
        user_id: int,
        application_no: str
):
    """查询改绑申请审核状态"""
    try:
        result = BankcardService.poll_modify_status(user_id, application_no)
        return result
    except Exception as e:
        logger.error(f"查询改绑状态失败: {e}")
        _err(str(e))


@router.get("/bankcard/status", summary="查询绑定状态", response_model=dict)
async def get_bind_status(user_id: int):
    """查询用户银行卡绑定状态"""
    try:
        status = BankcardService.query_bind_status(user_id)
        return status
    except Exception as e:
        logger.error(f"查询状态失败: {e}")
        _err(str(e))


@router.get("/bankcard/logs", summary="获取操作日志", response_model=List[dict])
async def get_logs(
        user_id: int,
        limit: int = Query(50, ge=1, le=1000, description="返回条数")
):
    """获取银行卡操作日志列表"""
    try:
        logs = BankcardService.get_operation_logs(user_id, limit)
        return logs
    except Exception as e:
        logger.error(f"获取日志失败: {e}")
        _err(str(e))


@router.get("/bankcard/my", summary="查询我的银行卡信息（明文）", response_model=dict)
async def get_my_bankcard(user_id: int):
    """
    查询当前用户的银行卡完整信息（明文返回，前端负责脱敏）
    必须确保使用HTTPS传输并验证用户身份
    """
    try:
        info = BankcardService.query_my_bankcard(user_id)
        return info
    except Exception as e:
        logger.error(f"查询我的银行卡失败: {e}")
        _err(str(e))


def register_bankcard_routes(app):
    """注册用户中心路由"""
    app.include_router(
        router,
        prefix="/api/user",
        tags=["用户中心"],
        responses={400: {"description": "业务错误"}, 401: {"description": "权限不足"}, 500: {"description": "服务器内部错误"}}
    )
    logger.info("银行卡路由注册完成")