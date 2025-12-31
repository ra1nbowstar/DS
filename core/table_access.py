"""
动态表访问工具模块
提供动态获取表结构并构造 SQL 查询的功能
"""
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
import re


# 缓存表结构信息，避免重复查询
_table_structure_cache: Dict[str, Dict[str, any]] = {}


def get_table_structure(cursor, table_name: str, use_cache: bool = True) -> Dict[str, any]:
    """
    获取表结构信息
    
    Args:
        cursor: 数据库游标
        table_name: 表名
        use_cache: 是否使用缓存
    
    Returns:
        包含字段信息的字典：{
            'fields': [字段名列表],
            'asset_fields': [资产字段名列表],
            'field_types': {字段名: 字段类型}
        }
    """
    cache_key = table_name
    
    # 如果使用缓存且缓存存在，直接返回
    if use_cache and cache_key in _table_structure_cache:
        return _table_structure_cache[cache_key]
    
    # 查询表结构
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns = cursor.fetchall()
    
    fields = []
    asset_fields = []
    field_types = {}
    
    for col in columns:
        field_name = col['Field']
        field_type = col['Type'].upper()
        
        fields.append(field_name)
        field_types[field_name] = field_type
        
        # 判断是否为资产字段（数值类型）
        if any(num_type in field_type for num_type in ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'INT', 'BIGINT', 'TINYINT', 'SMALLINT', 'MEDIUMINT']):
            asset_fields.append(field_name)
    
    result = {
        'fields': fields,
        'asset_fields': asset_fields,
        'field_types': field_types
    }
    
    # 缓存结果
    if use_cache:
        _table_structure_cache[cache_key] = result
    
    return result


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(name: str) -> str:
    """安全地引用 SQL 标识符（表名、列名）。

    只允许字母、数字和下划线，且不能以数字开头；否则抛出 ValueError。
    返回带反引号的标识符，防止注入。
    """
    if not isinstance(name, str):
        raise ValueError("identifier must be a string")

    if _IDENT_RE.match(name):
        return f"`{name}`"
    # 对于形如 schema.table 或 alias.col 的形式，逐段校验
    if "." in name:
        parts = name.split(".")
        quoted_parts = []
        for p in parts:
            if not _IDENT_RE.match(p):
                raise ValueError(f"invalid identifier part: {p}")
            quoted_parts.append(f"`{p}`")
        return ".".join(quoted_parts)

    raise ValueError(f"invalid identifier: {name}")


def build_select_list(fields: List[str]) -> str:
    """构造 SELECT 字段列表。

    对于简单的标识符会使用 `_quote_identifier` 进行引用；对于包含表达式、函数调用、别名或已经引用的字段，保留原样。
    规则：如果字段字符串包含空格、左括号、反引号或 AS(大小写不限)，则认为是表达式并保留原样；否则按标识符处理。
    """
    parts: List[str] = []
    for f in fields:
        if not isinstance(f, str):
            raise ValueError("select fields must be strings")
        low = f.lower()
        if " " in f or "(" in f or "`" in f or " as " in low:
            parts.append(f)
        else:
            parts.append(_quote_identifier(f))
    return ", ".join(parts)


def build_select_sql(table_name: str, structure: Dict[str, any], 
                     where_clause: Optional[str] = None,
                     order_by: Optional[str] = None,
                     limit: Optional[str] = None,
                     select_fields: Optional[List[str]] = None) -> str:
    """
    动态构造 SELECT 语句
    
    Args:
        table_name: 表名
        structure: 表结构信息（从 get_table_structure 获取）
        where_clause: WHERE 子句（不包含 WHERE 关键字）
        order_by: ORDER BY 子句（不包含 ORDER BY 关键字）
        limit: LIMIT 子句（不包含 LIMIT 关键字）
        select_fields: 指定要选择的字段列表，如果为 None 则选择所有字段
    
    Returns:
        构造的 SQL 语句
    """
    fields = select_fields if select_fields else structure['fields']
    asset_fields = structure['asset_fields']
    existing_fields = structure['fields']  # 实际存在的字段列表
    
    # 构造 SELECT 字段列表，对资产字段设置默认值
    select_parts = []
    for field in fields:
        # 如果传入的是数字字面量（例如 ['1'] 用于存在性检查），直接当作字面量处理
        if isinstance(field, str) and field.isdigit():
            select_parts.append(field)
            continue
        # 对字段名进行白名单校验与引用，防止注入
        if field not in existing_fields:
            # 字段不存在，使用默认值并引用别名
            if field in asset_fields or any(num_type in field.lower() for num_type in ['points', 'balance', 'amount']):
                select_parts.append(f"0 AS {_quote_identifier(field)}")
            else:
                select_parts.append(f"NULL AS {_quote_identifier(field)}")
        elif field in asset_fields:
            # 资产字段：引用字段并使用 COALESCE
            select_parts.append(f"COALESCE({_quote_identifier(field)}, 0) AS {_quote_identifier(field)}")
        else:
            # 非资产字段：直接引用字段名
            select_parts.append(_quote_identifier(field))
    
    # 引用表名
    sql = f"SELECT {build_select_list(select_parts)} FROM {_quote_identifier(table_name)}"
    
    if where_clause:
        # where_clause 可能包含参数占位符，仍然允许使用占位符，但禁止分号等附加语句
        if ";" in where_clause or "--" in where_clause or "/*" in where_clause:
            raise ValueError("unsafe characters in where_clause")
        sql += f" WHERE {where_clause}"
    
    if order_by:
        # 简单校验 ORDER BY，避免附加非标识符字符
        if ";" in order_by or "--" in order_by or "/*" in order_by:
            raise ValueError("unsafe characters in order_by")
        sql += f" ORDER BY {order_by}"
    
    if limit:
        # 仅允许数字或数字,数字 的形式
        if not re.match(r"^\d+(,\s*\d+)?$", str(limit)):
            raise ValueError("unsafe limit clause")
        sql += f" LIMIT {limit}"
    
    return sql


def build_dynamic_select(cursor, table_name: str, 
                        where_clause: Optional[str] = None,
                        order_by: Optional[str] = None,
                        limit: Optional[str] = None,
                        select_fields: Optional[List[str]] = None) -> str:
    """
    动态构造并返回 SELECT 语句（便捷方法）
    
    Args:
        cursor: 数据库游标
        table_name: 表名
        where_clause: WHERE 子句
        order_by: ORDER BY 子句
        limit: LIMIT 子句
        select_fields: 指定要选择的字段列表
    
    Returns:
        构造的 SQL 语句
    """
    structure = get_table_structure(cursor, table_name)
    return build_select_sql(table_name, structure, where_clause, order_by, limit, select_fields)


def clear_table_cache(table_name: Optional[str] = None):
    """
    清除表结构缓存
    
    Args:
        table_name: 表名，如果为 None 则清除所有缓存
    """
    global _table_structure_cache
    if table_name:
        _table_structure_cache.pop(table_name, None)
    else:
        _table_structure_cache.clear()

