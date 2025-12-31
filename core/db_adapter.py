"""
数据库适配器 - 封装 PyMySQL 连接和游标操作
提供统一的数据库操作接口，支持命名参数和便捷的结果访问
"""
from contextlib import contextmanager
import logging
from typing import Optional, Any, Dict, List
from core.database import get_conn
import pymysql
from typing import Iterable, Tuple


class PyMySQLAdapter:
    """
    PyMySQL 数据库适配器
    封装 PyMySQL 连接和事务管理，提供统一的数据库操作接口
    """
    
    def __init__(self):
        self._conn = None
        self._cursor = None
    
    @contextmanager
    def begin(self):
        """开始事务（上下文管理器）"""
        if self._conn is None:
            self._conn = get_conn().__enter__()
            self._cursor = self._conn.cursor()
        try:
            yield self
            self._conn.commit()
        except Exception:
            if self._conn:
                self._conn.rollback()
            raise
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None):
        """
        执行 SQL 语句
        
        Args:
            sql: SQL 语句（支持 :param 格式，会自动转换为 %s）
            params: 参数字典
        
        Returns:
            ResultProxy 对象，用于访问查询结果
        """
        if self._conn is None:
            self._conn = get_conn().__enter__()
            self._cursor = self._conn.cursor()
        
        # 简单校验 SQL，拒绝包含多语句或注释的输入
        self._validate_sql(sql)

        # 将 :param 格式转换为 %s 格式
        if params:
            sql, values = self._convert_sql_params(sql, params)
        else:
            values = None

        logger = logging.getLogger(__name__)
        logger.debug("Executing SQL: %s | params: %s", sql, values)
        try:
            self._cursor.execute(sql, values)
        except (pymysql.err.InterfaceError, pymysql.err.OperationalError) as e:
            # 连接可能已断开或游标已关闭，尝试重建连接并重试一次
            logger.warning("DB execute failed, reconnecting and retrying: %s; SQL=%s; params=%s", e, sql, values)
            try:
                # 关闭已有资源并重建
                self.close()
                self._conn = get_conn().__enter__()
                self._cursor = self._conn.cursor()
                logger.debug("Retrying SQL after reconnect: %s | params: %s", sql, values)
                self._cursor.execute(sql, values)
            except Exception as e2:
                # 若重试也失败，记录详细信息并抛出原始异常
                logger.exception("DB retry failed: %s; SQL=%s; params=%s", e2, sql, values)
                raise

        return ResultProxy(self._cursor)

    def _validate_sql(self, sql: str, allow_comments: bool = False):
        """对即将执行的 SQL 做简单安全校验，拒绝多语句和注释。

        说明：此校验为防御层之一，不能替代参数化查询和标识符白名单。
        """
        if not isinstance(sql, str):
            raise ValueError("sql must be a string")
        # 默认严格模式：若不允许注释，则直接拒绝包含注释或分号的 SQL
        if not allow_comments:
            if ";" in sql or "--" in sql or "/*" in sql or "*/" in sql:
                raise ValueError("unsafe SQL detected")

        # 放宽模式：允许注释存在但仍需拒绝多语句。
        # 我们通过去除注释（忽略字符串内部的注释标记）并检查分号是否出现在字符串之外来实现。
        # 如果允许注释，则执行如下更精确的检查；否则已在上面严格拒绝。
        if allow_comments:
            s = sql
            if not s:
                return

            # 去除注释（保留字符串字面量），实现同上
            i = 0
            n = len(s)
            cleaned_chars = []
            in_squote = False
            in_dquote = False
            in_line_comment = False
            in_block_comment = False

            while i < n:
                ch = s[i]
                if in_line_comment:
                    if ch == '\n':
                        in_line_comment = False
                        cleaned_chars.append(ch)
                    i += 1
                    continue
                if in_block_comment:
                    if ch == '*' and i + 1 < n and s[i+1] == '/':
                        in_block_comment = False
                        i += 2
                    else:
                        i += 1
                    continue
                if in_squote:
                    if ch == "'":
                        if i + 1 < n and s[i+1] == "'":
                            cleaned_chars.append("''")
                            i += 2
                            continue
                        else:
                            in_squote = False
                    cleaned_chars.append(ch)
                    i += 1
                    continue
                if in_dquote:
                    if ch == '"':
                        if i + 1 < n and s[i+1] == '"':
                            cleaned_chars.append('""')
                            i += 2
                            continue
                        else:
                            in_dquote = False
                    cleaned_chars.append(ch)
                    i += 1
                    continue
                if ch == '-' and i + 1 < n and s[i+1] == '-':
                    in_line_comment = True
                    i += 2
                    continue
                if ch == '/' and i + 1 < n and s[i+1] == '*':
                    in_block_comment = True
                    i += 2
                    continue
                if ch == "'":
                    in_squote = True
                    cleaned_chars.append(ch)
                    i += 1
                    continue
                if ch == '"':
                    in_dquote = True
                    cleaned_chars.append(ch)
                    i += 1
                    continue
                cleaned_chars.append(ch)
                i += 1

            cleaned = ''.join(cleaned_chars)

            # 检查分号是否出现在字符串之外
            i = 0
            n = len(cleaned)
            in_squote = in_dquote = False
            while i < n:
                ch = cleaned[i]
                if in_squote:
                    if ch == "'":
                        if i + 1 < n and cleaned[i+1] == "'":
                            i += 2
                            continue
                        else:
                            in_squote = False
                    i += 1
                    continue
                if in_dquote:
                    if ch == '"':
                        if i + 1 < n and cleaned[i+1] == '"':
                            i += 2
                            continue
                        else:
                            in_dquote = False
                    i += 1
                    continue
                if ch == "'":
                    in_squote = True
                elif ch == '"':
                    in_dquote = True
                elif ch == ';':
                    raise ValueError("unsafe SQL detected")
                i += 1
    
    def _convert_sql_params(self, sql: str, params: Dict[str, Any]) -> tuple:
        """将命名参数格式 `:param` 转换为 PyMySQL 的 `%s` 格式，并返回转换后的 SQL 和参数元组"""
        result_sql = sql
        param_list = []
        # 按顺序替换参数
        for key, value in params.items():
            result_sql = result_sql.replace(f":{key}", "%s", 1)  # 只替换第一个
            param_list.append(value)
        return result_sql, tuple(param_list)
    
    def commit(self):
        """提交事务"""
        if self._conn:
            self._conn.commit()
    
    def rollback(self):
        """回滚事务"""
        if self._conn:
            self._conn.rollback()
    
    def close(self):
        """关闭连接"""
        logger = logging.getLogger(__name__)
        if self._cursor:
            try:
                self._cursor.close()
            except Exception as e:
                logger.debug("ignoring cursor.close() error: %s", e)
        if self._conn:
            try:
                self._conn.close()
            except Exception as e:
                # pymysql may raise Error("Already closed") if connection was closed
                logger.debug("ignoring conn.close() error: %s", e)
            finally:
                self._conn = None
                self._cursor = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


def build_in_placeholders(values: Iterable) -> Tuple[str, dict]:
    """为 SQL IN(...) 构造占位符字符串和参数字典。

    返回 (placeholders, params_dict)，其中 placeholders 例如 '%s,%s,%s'，
    params_dict 为 {'id0': v0, 'id1': v1, ...}，方便传入本项目的 `PyMySQLAdapter.execute`。
    """
    vals = list(values)
    if not vals:
        raise ValueError("values must be a non-empty iterable")
    placeholders = ','.join(['%s'] * len(vals))
    params = {f"id{i}": v for i, v in enumerate(vals)}
    return placeholders, params


class ResultProxy:
    """数据库查询结果代理类，封装查询结果并提供便捷的访问方法"""
    
    def __init__(self, cursor: pymysql.cursors.DictCursor):
        self._cursor = cursor
        self._rows = None
    
    def fetchone(self):
        """获取单行结果"""
        if self._rows is None:
            self._rows = self._cursor.fetchall()
        if self._rows:
            row = self._rows[0]
            self._rows = self._rows[1:]
            return RowProxy(row)
        return None
    
    def fetchall(self):
        """获取所有结果"""
        if self._rows is None:
            self._rows = self._cursor.fetchall()
        return [RowProxy(row) for row in self._rows]
    
    @property
    def lastrowid(self):
        """获取最后插入的 ID"""
        return self._cursor.lastrowid
    
    @property
    def rowcount(self):
        """获取受影响的行数"""
        return self._cursor.rowcount


class RowProxy:
    """数据库行数据代理类，支持属性访问和字典访问两种方式"""
    
    def __init__(self, row: Dict):
        self._row = row
    
    def __getattr__(self, name: str):
        """支持属性访问，如 row.balance"""
        if name in self._row:
            return self._row[name]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
    
    def __getitem__(self, key: str):
        """支持字典访问，如 row['balance']"""
        return self._row[key]
    
    def __contains__(self, key: str):
        """支持 in 操作"""
        return key in self._row
    
    def get(self, key: str, default=None):
        """字典风格的 get 方法"""
        return self._row.get(key, default)
