#!/usr/bin/env python3

# Author: Michael Welter <me@mikinho.com>
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# THE SOFTWARE.

"""
Infer SQL schema from JSON, including nested-array subtables and lists of primitives.

Usage:
    python infer_schema.py \
        --input data.json \
        [--table my_table] \
        [--output schema.sql] \
        [--primary-key COLUMN] \
        [--cushion PERCENT|VALUE] \
        [--pascal] \
        [--nerd] \
        [--sort] \
        [--map src:dest ...]
"""

import argparse
import os
import json
import re
import math
from collections import defaultdict, OrderedDict
from dateutil.parser import parse as parse_date, ParserError
from decimal import Decimal, InvalidOperation


def to_pascal(s):
    words = re.split(r"[^0-9a-zA-Z]+", s)
    tokens = []
    pattern = r"[A-Z]{2,}(?=[A-Z][a-z]|[0-9]|$)|[A-Z]?[a-z]+|[A-Z]+|[0-9]+"
    for w in words:
        if not w:
            continue
        for t in re.findall(pattern, w):
            tokens.append(t)
    def cap_part(p):
        if len(p) == 2:
            return p.upper()
        return p[0].upper() + p[1:].lower()
    return "".join(cap_part(part) for part in tokens)


def infer_sql_type(values, nerd=False, cushion_arg="10%"):
    has_int = has_float = has_date = has_datetime = has_str = has_bool = False
    min_len = None
    max_len = max_prec = max_scale = 0
    for v in values:
        if isinstance(v, bool):
            has_bool = True
            continue
        if isinstance(v, int):
            has_int = True
            continue
        if isinstance(v, float):
            has_float = True
            try:
                d = Decimal(str(v))
                prec = len(d.as_tuple().digits)
                scale = max(-d.as_tuple().exponent, 0)
                max_prec = max(max_prec, prec)
                max_scale = max(max_scale, scale)
            except InvalidOperation:
                pass
            continue
        if isinstance(v, str):
            s = v.strip()
            length = len(s)
            if not s or length == 0:
                continue

            if re.fullmatch(r"[+-]?\d+", s):
                has_int = True
                continue

            if re.fullmatch(r"[+-]?(?:\d+\.\d*|\.\d+)", s):
                has_float = True
                try:
                    d = Decimal(s)
                    prec = len(d.as_tuple().digits)
                    scale = max(-d.as_tuple().exponent, 0)
                    max_prec = max(max_prec, prec)
                    max_scale = max(max_scale, scale)
                except InvalidOperation:
                    pass
                continue

            if s.lower() in ("true", "false", "yes", "no"):
                has_bool = True
                continue

            try:
                _ = parse_date(s, fuzzy=False)
                if re.search(r"\d{1,2}:\d{2}(:\d{2})?", s):
                    has_datetime = True
                else:
                    has_date = True
                continue
            except (ParserError, ValueError):
                pass
            except (OverflowError):
                pass

            has_str = True
            max_len = max(max_len, length)
            min_len = length if min_len is None else min(min_len, length)
            continue

        txt = str(v)
        has_str = True
        length = len(txt)
        max_len = max(max_len, length)
        min_len = length if min_len is None else min(min_len, length)
    if has_datetime and not any([has_str,has_float,has_int,has_date,has_bool]):
        return "DATETIME"
    if has_date and not any([has_str,has_float,has_int,has_datetime,has_bool]):
        return "DATE"
    if has_bool and not any([has_str,has_float,has_int,has_date,has_datetime]):
        return "BOOLEAN"
    if has_float:
        precision = max(1, max_prec)
        scale = max_scale
        return f"DECIMAL({precision},{scale})"
    if has_int and not any([has_float,has_str,has_date,has_datetime,has_bool]):
        return "INTEGER"
    orig = max(1, max_len)
    if nerd:
        if cushion_arg.endswith("%"):
            try:
                pct = float(cushion_arg.rstrip("%")) / 100
            except ValueError:
                pct = 0.1
            cushion = math.ceil(orig * pct)
        else:
            try:
                cushion = int(cushion_arg)
            except ValueError:
                cushion = math.ceil(orig * 0.1)
        if min_len is not None and min_len == max_len:
            cushion = 0
        adjusted = orig + cushion
        exp = adjusted.bit_length() - 1
        block_size = 2 ** max(exp - 1, 0)
        length = math.ceil(adjusted / block_size) * block_size
    else:
        length = orig

    return f"VARCHAR({length})"


def _get_primary_key_type(col_defs):
    for d in col_defs:
        if '"id"' in d and "PRIMARY KEY" in d:
            return d.split()[2]
    return "INTEGER"


def process_table(
    records,
    table_name,
    parent_fk=None,
    schemas=None,
    pascal=False,
    nerd=False,
    rename_map=None,
    sort=False,
    pk_source=None,
    cushion_arg="10%"
):
    if schemas is None:
        schemas = OrderedDict()
    total_rows = len(records)
    cols = defaultdict(list)
    nested_objs = defaultdict(list)
    nested_prims = defaultdict(list)
    for row in records:
        for key, value in row.items():
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    continue
            if value is None:
                continue
            if isinstance(value, list) and value and all(isinstance(x, dict) for x in value):
                nested_objs[key].extend(value)
                continue
            if isinstance(value, list) and value and all(not isinstance(x, dict) for x in value):
                nested_prims[key].extend(value)
                continue
            cols[key].append(value)
    # Determine null status and grouping bases
    null_status = {c: len(vals) < total_rows for c, vals in cols.items()}
    groups = defaultdict(list)
    for c in cols.keys():
        base = re.sub(r"\d+$", "", c)
        groups[base].append(c)
    # Order groups: non-null first then null
    def group_has_non_null(base):
        return any(not null_status[c] for c in groups[base])
    ordered_bases = sorted(
        groups.keys(),
        key=lambda b: (not group_has_non_null(b), b.lower())
    )
    # Build definitions with PK first, FKs next
    pk_col = pk_source if pk_source else "ID"
    col_defs = []
    # Primary key
    if pk_col in cols:
        sql = infer_sql_type(cols[pk_col], nerd, cushion_arg)
        out = pk_col if rename_map is None else rename_map.get(pk_col, pk_col)
        if pascal:
            out = to_pascal(out)
        safe = out.replace('"', '""')
        col_defs.append(f"    \"{safe}\" {sql} PRIMARY KEY")
    # Foreign key
    if parent_fk:
        fk_name, fk_type = parent_fk
        out = rename_map.get(fk_name, fk_name)
        if pascal:
            out = to_pascal(out)
        safe = out.replace('"', '""')
        col_defs.append(f"    \"{safe}\" {fk_type} NOT NULL")
    # Other columns
    other_defs = []
    for base in ordered_bases:
        for c in sorted(groups[base]):
            if c == pk_col or (parent_fk and c == parent_fk[0]):
                continue
            sql = infer_sql_type(cols[c], nerd, cushion_arg)
            out = c if rename_map is None else rename_map.get(c, c)
            if pascal:
                out = to_pascal(out)
            safe = out.replace('"', '""')
            d = f"    \"{safe}\" {sql}"
            if not null_status[c]:
                d += " NOT NULL"
            other_defs.append(d)
    # If sort flag, grouping applied; else preserve insertion order
    if sort:
        col_defs.extend(other_defs)
    else:
        # insertion order as encountered
        for c in cols.keys():
            if pascal:
                c = to_pascal(c)
            if c == pk_col or (parent_fk and c == parent_fk[0]):
                continue
            for d in other_defs:
                if re.search(rf"\"{c}\"", d):
                    col_defs.append(d)
                    break
    schemas[table_name] = col_defs
    # Recurse nested objects and primitives
    for field, children in nested_objs.items():
        sub_name = f"{table_name}_{field}"
        fk_col = f"{table_name}_id"
        fk_type = _get_primary_key_type(col_defs)
        for child in children:
            child[fk_col] = next((r.get("ID") for r in records if isinstance(r, dict)), None)
        process_table(
            children,
            sub_name,
            parent_fk=(fk_col, fk_type),
            schemas=schemas,
            pascal=pascal,
            nerd=nerd,
            rename_map=rename_map,
            sort=sort,
            pk_source=pk_source,
            cushion_arg=cushion_arg
        )
    for field, prims in nested_prims.items():
        sub_name = f"{table_name}_{field}"
        fk_col = f"{table_name}_id"
        fk_type = _get_primary_key_type(col_defs)
        prim_records = []
        for row in records:
            for v in row.get(field, []):
                prim_records.append({fk_col: row.get("ID"), "value": v})
        process_table(
            prim_records,
            sub_name,
            parent_fk=(fk_col, fk_type),
            schemas=schemas,
            pascal=pascal,
            nerd=nerd,
            rename_map=rename_map,
            sort=sort,
            pk_source=pk_source,
            cushion_arg=cushion_arg
        )
    return schemas


def render_ddl(schemas):
    statements = []
    for table, defs in schemas.items():
        stmt = f"CREATE TABLE {table} (\n" + ",\n".join(defs) + "\n);"
        statements.append(stmt)
    return "\n\n".join(statements)


def main():
    parser = argparse.ArgumentParser(description="Infer SQL schema from JSON")
    parser.add_argument("-i", "--input", required=True, help="JSON file path")
    parser.add_argument("-t", "--table",default=None,help="Root table name (from filename if omitted)")
    parser.add_argument("-o", "--output", default=None, help="Output SQL file")
    parser.add_argument("--primary-key", "--pk", dest="primary_key", help="Source column for PRIMARY KEY")
    parser.add_argument("-c", "--cushion", default="10%", help="Cushion percent or absolute value")
    parser.add_argument("--pascal", action="store_true", help="PascalCase names")
    parser.add_argument("--nerd", action="store_true", help="Power-of-two VARCHAR sizing")
    parser.add_argument("--sort", action="store_true", help="Group non-null column groups first, grouping bases")
    parser.add_argument("-m", "--map", nargs="*", help="Column rename mappings src:dst")
    args = parser.parse_args()

    # derive table name from filename if omitted
    table_name = args.table if not args.table is None else os.path.splitext(os.path.basename(args.input))[0]
    if args.pascal:
        table_name = to_pascal(table_name)

    rename_map = {}
    if args.map:
        for mapping in args.map:
            if ':' in mapping:
                src, dst = mapping.split(':', 1)
                rename_map[src] = dst.replace("$table", table_name)
            else:
                parser.error(f"Invalid map '{mapping}', expected src:dst")
    data = json.load(open(args.input, encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("JSON top-level must be an array of objects.")

    schemas = process_table(
        data,
        table_name,
        pascal=args.pascal,
        nerd=args.nerd,
        rename_map=rename_map or None,
        sort=args.sort,
        pk_source=args.primary_key,
        cushion_arg=args.cushion
    )
    ddl = render_ddl(schemas)
    if args.output:
        with open(args.output, 'w', encoding="utf-8") as f:
            f.write(ddl)
        print(f"Schema DDL written to {args.output}")
    else:
        print(ddl)

if __name__ == "__main__":
    main()
