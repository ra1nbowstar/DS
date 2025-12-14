#!/usr/bin/env python3
"""简单的 SQL 注入防护单元测试（无需数据库连接）

用法：在项目根目录下运行：
  source .venv/bin/activate
  python3 scripts/sql_injection_test.py

此脚本验证 `core.table_access._quote_identifier` 与
`core.db_adapter.PyMySQLAdapter._validate_sql` 对常见注入向量的拒绝行为。
"""
import sys
import pathlib

# Ensure project root is on sys.path so `from core import ...` works
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.table_access import _quote_identifier
from core.db_adapter import PyMySQLAdapter


def run_identifier_tests():
    tests = [
        ("users", True),
        ("users; DROP TABLE users;", False),
        ("1abc", False),
        ("schema.users", True),
        ("sch;ema.users", False),
        ("name-with-dash", False),
    ]

    ok = True
    for name, expected_ok in tests:
        try:
            res = _quote_identifier(name)
            passed = expected_ok
            if not expected_ok:
                print(f"[FAIL] identifier '{name}' was ACCEPTED as {res}, expected rejection")
                ok = False
            else:
                print(f"[OK] identifier '{name}' -> {res}")
        except Exception as e:
            if expected_ok:
                print(f"[FAIL] identifier '{name}' was REJECTED: {e}")
                ok = False
            else:
                print(f"[OK] identifier '{name}' rejected as expected: {e}")

    return ok


def run_sql_validation_tests():
    adapter = PyMySQLAdapter()
    tests = [
        ("SELECT * FROM users WHERE id = %s", True),
        ("SELECT * FROM users; DROP TABLE users;", False),
        ("SELECT name FROM users -- comment", False),
        ("SELECT * FROM users /* comment */", False),
    ]

    ok = True
    for sql, expected_ok in tests:
        try:
            adapter._validate_sql(sql)
            if not expected_ok:
                print(f"[FAIL] SQL was ACCEPTED but expected rejection: {sql}")
                ok = False
            else:
                print(f"[OK] SQL accepted: {sql}")
        except Exception as e:
            if expected_ok:
                print(f"[FAIL] SQL was REJECTED unexpectedly: {sql} -> {e}")
                ok = False
            else:
                print(f"[OK] SQL rejected as expected: {sql} -> {e}")

    return ok


def main():
    print("Running SQL injection defenses tests...")
    id_ok = run_identifier_tests()
    sql_ok = run_sql_validation_tests()

    all_ok = id_ok and sql_ok
    if all_ok:
        print("\nAll tests passed.")
        sys.exit(0)
    else:
        print("\nSome tests failed.")
        sys.exit(2)


if __name__ == '__main__':
    main()
