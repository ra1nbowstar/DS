#!/usr/bin/env python3
"""
修复 offline_order.amount 字段类型
将 amount 从 INT 改为 BIGINT，以支持更大的金额范围
"""

import pymysql
from core.config import get_db_config
from core.logging import get_logger

logger = get_logger(__name__)

def upgrade_offline_order_amount():
    """升级 offline_order.amount 字段类型"""
    try:
        config = get_db_config()
        conn = pymysql.connect(**config)
        cursor = conn.cursor()

        # 将 amount 从 INT 改为 BIGINT
        sql = "ALTER TABLE offline_order MODIFY COLUMN amount BIGINT NOT NULL COMMENT '订单金额（单位：分）'"
        cursor.execute(sql)
        logger.info("升级 offline_order.amount 字段成功")

        conn.commit()
        logger.info("offline_order.amount 字段类型升级完成")

    except Exception as e:
        logger.error(f"升级失败: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    upgrade_offline_order_amount()