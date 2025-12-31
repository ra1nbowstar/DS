#!/usr/bin/env python3
"""扫描项目文件，查找可能不安全的动态 IN(...) 列表构造点。

此脚本不会修改代码，只会报告包含以下可疑模式的文件/行：
- 在字符串或 f-string 中使用 `join` 构造 IN 列表
- 在 f-string 中直接插入变量到 `IN(...)`
- 使用 `','.join(map(str, ...))` 等将值拼接进 SQL
"""
import re
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]

PATTERNS = [
    re.compile(r"IN\s*\(.*\{.*join", re.IGNORECASE),
    re.compile(r"IN\s*\(.*join\(.*map\(str", re.IGNORECASE),
    re.compile(r"IN\s*\(.*\+.*\)", re.IGNORECASE),
    re.compile(r"join\(.*map\(str", re.IGNORECASE),
    re.compile(r"\{.*join\(.*\}\)", re.IGNORECASE),
]


def scan_file(path: pathlib.Path):
    with path.open('r', encoding='utf-8') as f:
        lines = f.readlines()

    results = []
    for i, line in enumerate(lines, start=1):
        for p in PATTERNS:
            if p.search(line):
                results.append((i, line.strip()))
                break
    return results


def main():
    print(f"Scanning {ROOT} for suspicious IN-list patterns...")
    # 排除路径：虚拟环境、site-packages、缓存、构建目录等，减少第三方库误报
    exclude_parts = {'.venv', 'venv', 'site-packages', '__pycache__', 'node_modules', 'dist', 'build'}
    files = [p for p in ROOT.rglob('*.py') if not any(part in exclude_parts for part in p.parts)]
    total = 0
    for file in files:
        rel = file.relative_to(ROOT)
        hits = scan_file(file)
        if hits:
            total += 1
            print(f"\n{rel}:")
            for lineno, text in hits:
                print(f"  L{lineno}: {text}")

    if total == 0:
        print("\nNo suspicious dynamic IN-list constructions found by heuristic scan.")
    else:
        print(f"\nFound {total} files with suspicious patterns. Review manually before patching.")


if __name__ == '__main__':
    main()
