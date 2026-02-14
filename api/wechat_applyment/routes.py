# api/wechat_applyment/routes.py
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime
from services.wechat_applyment_service import WechatApplymentService
from core.auth import get_current_user
from core.response import success_response, error_response
import json

router = APIRouter(prefix="/wechat-applyment", tags=["微信进件"])

# 定义Pydantic模型（放在这里，不依赖schemas文件夹）
class SubjectType(str, Enum):
    INDIVIDUAL = "SUBJECT_TYPE_INDIVIDUAL"
    ENTERPRISE = "SUBJECT_TYPE_ENTERPRISE"

class ApplymentDraftCreate(BaseModel):
    """进件草稿创建"""
    subject_type: SubjectType = Field(..., description="主体类型")
    subject_info: dict = Field(default_factory=dict, description="主体资料")
    contact_info: dict = Field(default_factory=dict, description="联系人信息")
    bank_account_info: dict = Field(default_factory=dict, description="结算账户信息")

class ApplymentSubmit(BaseModel):
    """进件提交"""
    applyment_id: Optional[int] = Field(None, description="草稿ID")
    subject_type: SubjectType = Field(..., description="主体类型")
    subject_info: dict = Field(..., description="主体资料")
    contact_info: dict = Field(..., description="联系人信息")
    bank_account_info: dict = Field(..., description="结算账户信息")
    # 新增：经营类目锁定字段
    business_category_locked: bool = Field(default=False, description="经营类目是否锁定")

class CoreInfoModify(BaseModel):
    """核心信息修改"""
    bank_account_info: dict = Field(..., description="新的结算账户信息")
    reason: Optional[str] = Field(None, description="修改原因")
    contact_info: dict = Field(default_factory=dict, description="联系人信息")

@router.post("/draft", summary="保存进件草稿")
async def save_draft(
    data: ApplymentDraftCreate,
    current_user: dict = Depends(get_current_user)
):
    """保存进件草稿，有效期7天"""
    try:
        service = WechatApplymentService()
        result = service.save_draft(current_user["id"], data.dict())
        return success_response(data=result, message="草稿保存成功")
    except Exception as e:
        return error_response(message=str(e))

@router.post("/submit", summary="提交进件申请")
async def submit_applyment(
    data: ApplymentSubmit,
    current_user: dict = Depends(get_current_user)
):
    """提交微信进件申请"""
    try:
        service = WechatApplymentService()
        result = await service.submit_applyment(current_user["id"], data.dict())
        return success_response(data=result, message="进件申请已提交")
    except Exception as e:
        return error_response(message=str(e))

@router.get("/status", summary="查询进件状态")
async def get_applyment_status(
    current_user: dict = Depends(get_current_user)
):
    """查询当前用户的进件状态"""
    try:
        service = WechatApplymentService()
        status = service.get_applyment_status(current_user["id"])
        return success_response(data=status)
    except Exception as e:
        return error_response(message=str(e))

@router.post("/media/upload", summary="上传进件材料")
async def upload_media(
    file: UploadFile = File(...),
    media_type: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    """上传身份证、营业执照等材料"""
    try:
        service = WechatApplymentService()
        result = await service.upload_media(current_user["id"], file, media_type)
        return success_response(data=result, message="文件上传成功")
    except Exception as e:
        return error_response(message=str(e))

@router.get("/media/list", summary="获取材料列表")
async def list_media(
    applyment_id: Optional[int] = None,
    current_user: dict = Depends(get_current_user)
):
    """查询已上传的材料列表"""
    try:
        service = WechatApplymentService()
        media_list = service.list_media(current_user["id"], applyment_id)
        return success_response(data=media_list)
    except Exception as e:
        return error_response(message=str(e))

@router.post("/core-info/modify", summary="修改核心信息")
async def modify_core_info(
    data: CoreInfoModify,
    current_user: dict = Depends(get_current_user)
):
    """修改经营类目或结算账户等核心信息（需重新进件）"""
    try:
        service = WechatApplymentService()
        result = service.modify_core_info(current_user["id"], data.dict())
        return success_response(data=result, message="核心信息修改已提交，需重新审核")
    except Exception as e:
        return error_response(message=str(e))


@router.post("/resubmit", summary="重新提交被驳回的进件")
async def resubmit_applyment(
        applyment_id: int,
        current_user: dict = Depends(get_current_user)
):
    """根据驳回原因修改后重新提交"""
    try:
        service = WechatApplymentService()
        result = service.resubmit_applyment(current_user["id"], applyment_id)
        return success_response(data=result, message="进件已重新提交")
    except Exception as e:
        return error_response(message=str(e))


@router.get("/service-provider/status", summary="查询服务商配置状态")
async def get_service_provider_status(
        current_user: dict = Depends(get_current_user)
):
    """
    查询平台服务商配置状态
    - 检查平台是否已配置微信支付服务商资质
    - 返回服务商基本信息（脱敏）
    """
    try:
        from core.config import WECHAT_PAY_SP_MCH_ID, WECHAT_PAY_SP_APPID

        is_configured = bool(WECHAT_PAY_SP_MCH_ID and WECHAT_PAY_SP_APPID)

        # 脱敏处理
        sp_mchid_masked = None
        if WECHAT_PAY_SP_MCH_ID:
            # 显示前6位和后4位，中间用****代替
            sp_mchid_masked = f"{WECHAT_PAY_SP_MCH_ID[:6]}****{WECHAT_PAY_SP_MCH_ID[-4:]}" if len(
                WECHAT_PAY_SP_MCH_ID) > 10 else "****"

        sp_appid_masked = None
        if WECHAT_PAY_SP_APPID:
            # 显示前6位和后4位
            sp_appid_masked = f"{WECHAT_PAY_SP_APPID[:6]}****{WECHAT_PAY_SP_APPID[-4:]}" if len(
                WECHAT_PAY_SP_APPID) > 10 else "****"

        return success_response(data={
            "is_configured": is_configured,
            "sp_mchid": sp_mchid_masked,
            "sp_appid": sp_appid_masked,
            "can_apply_sub_merchant": is_configured,
            "message": "服务商资质已配置，可以为商家进件" if is_configured else "服务商资质未配置，无法为商家进件"
        })
    except Exception as e:
        return error_response(message=str(e))


@router.get("/merchant-info", summary="获取商户号信息")
async def get_merchant_info(
        current_user: dict = Depends(get_current_user)
):
    """获取已审核通过的商户号信息"""
    try:
        service = WechatApplymentService()
        info = service.get_merchant_info(current_user["id"])
        return success_response(data=info)
    except Exception as e:
        return error_response(message=str(e))

def register_wechat_applyment_routes(app):
    """注册路由到主应用"""
    app.include_router(router)