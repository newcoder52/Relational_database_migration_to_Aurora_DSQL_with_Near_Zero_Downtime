#!/usr/bin/env python3
"""ONE shared transform for the --null_notnull_text feature.

This module is the SINGLE SOURCE OF TRUTH for every text edit the feature makes. Both
deliverables import it so they cannot drift:

  * apply_null_notnull_text_patch.py  — hand-patches a deployed checkout / drop-in copy of the
                                        four Glue scripts + job1 + the six plumbing files.
  * integrate_into_repo.py            — applies the SAME edits to the repo working tree on a
                                        branch off main (real repo integration).

Every transform is:
  * anchor-checked and FAIL-CLOSED: each anchor must be present EXACTLY ONCE (an already-present
    anchor for a modified/renamed file aborts without writing);
  * idempotent: a file that already carries the feature is returned unchanged (no-op);
  * pure text in / text out (no I/O here) so the two callers share identical behaviour.

FEATURE (opt-in, default OFF = byte-identical to today)
-------------------------------------------------------
New optional Glue arg --null_notnull_text with values:  off (default) | keep_text.
It travels params.csv -> pipeline.json -> resolve_task -> create_glue_jobs -> Step Functions ->
the job scripts exactly like null_values/null_rules (blank -> __NULL_UNSET__ sentinel).

RULE. For a target column that is NOT NULL (information_schema.columns.is_nullable='NO') AND
whose type category is character varying / character / text ('varchar' or 'text'), NO text value
is ever converted to SQL NULL — the DMS endpoint marker and any null_values entry are stored as
the literal text. Empty field stays NULL in every mode (cannot occur in a NOT NULL Oracle column
anyway). Nullable columns and non-text types are unchanged. Precedence per column:
  explicit null_rules entry  >  this automatic rule  >  null_values  >  endpoint marker.

The rule is implemented at the SINGLE choke point every script already funnels through,
_effective_null_markers(schema, table, column[, category]): when the column is a protected
NOT NULL text column and the arg is keep_text, it returns [] (no text marker) UNLESS an explicit
null_rules entry names that exact column (which still wins). All four scripts and all their call
sites (loader Spark null_marker_expr, validate Spark null_marker_expr, CDC _coerce_null) pick it
up automatically, so load / validate / CDC always agree (no validation false-flag).
"""

import hashlib


def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class AnchorError(Exception):
    """An anchor was missing or not present exactly once — refuse to patch (fail closed)."""


def _require_once(src, anchor, label):
    n = src.count(anchor)
    if n != 1:
        raise AnchorError("anchor %s not found exactly once (found %d) - refusing to patch"
                          % (label, n))


# =============================================================================================
# 1) THE SHARED NULL BLOCK EXTENSION (byte-identical in all FIVE script copies:
#    job2_load.py, job3_validate.py, glue_cdc_continuous.py, glue_cdc_composite.py, and
#    manual_range_pipeline/scripts/job2_load_manual_range.py).
# =============================================================================================

# The unique token that proves a file already carries the feature (idempotency check).
_MARK = "NULL_NOTNULL_TEXT"

# --- (1a) New globals, inserted right after the NULL_RULES global line. --------------------
A_GLOBALS = (
    "NULL_RULES = None    # None | dict[(schema,table,col) -> list[str]]  ([] == 'none': no markers)\n"
)
A_GLOBALS_NEW = (
    "NULL_RULES = None    # None | dict[(schema,table,col) -> list[str]]  ([] == 'none': no markers)\n"
    "# --- OPT-IN automatic NOT NULL char/text keep-text rule (--null_notnull_text). ----------\n"
    '# "off" (default) == byte-identical to today. "keep_text": for a NOT NULL char/text column\n'
    "# NO text value (endpoint marker OR a null_values entry) becomes SQL NULL - it is stored as\n"
    "# the literal text. An explicit null_rules entry for that exact column STILL wins. Empty\n"
    "# field stays NULL in every mode. Nullable columns and non-text types are unchanged.\n"
    'NULL_NOTNULL_TEXT = "off"          # "off" | "keep_text"\n'
    "# Per-table nullability the rule needs: {(schema_l, table_l): {col_l: 'YES'|'NO'}}. Filled\n"
    "# by _nn_register_nullability() from the Job 1 mapping (is_nullable per column) or, for an\n"
    "# OLDER mapping without it, from ONE catalog query the caller runs. A registered table whose\n"
    "# column is absent here is treated as UNKNOWN -> the rule FAILS LOUD (never silently skips).\n"
    "_NN_NULLABILITY = {}\n"
    "# The char/text categories the rule applies to. categorize_type() maps character varying,\n"
    "# character and length-limited varchar (DSQL varchar(n)) all to 'varchar', and text to\n"
    "# 'text'; every other category (int/numeric/date/timestamp/uuid/bool/bytea/json...) is\n"
    "# excluded, so only these two are protected.\n"
    '_NN_TEXT_CATEGORIES = frozenset({"varchar", "text"})\n'
)

# --- (1b) Rewrite _effective_null_markers to take an optional category and apply the rule. --
A_EFF = (
    "def _effective_null_markers(schema, table, column):\n"
    '    """The ORDERED list of exact text values that mean SQL NULL for this one column, applying\n'
    "    precedence: a null_rules entry for the column (which may be [] for 'none'), THEN null_values\n"
    "    (replaces for all columns), THEN the endpoint marker CSV_NULL_VALUE. Empty-field handling is\n"
    "    separate (always NULL) and NOT included here. Names are matched case-insensitively. With both\n"
    "    settings unset this returns [CSV_NULL_VALUE] (if non-empty) or [] — i.e. today's behaviour.\"\"\"\n"
    "    if NULL_RULES is not None:\n"
    "        key = (str(schema).lower(), str(table).lower(), str(column).lower())\n"
    "        if key in NULL_RULES:\n"
    "            return list(NULL_RULES[key])\n"
    "    if NULL_VALUES is not None:\n"
    "        return list(NULL_VALUES)\n"
    "    return [CSV_NULL_VALUE] if CSV_NULL_VALUE else []\n"
)
A_EFF_NEW = (
    "def _effective_null_markers(schema, table, column, category=None):\n"
    '    """The ORDERED list of exact text values that mean SQL NULL for this one column, applying\n'
    "    precedence: an explicit null_rules entry for the column (which may be [] for 'none'), THEN\n"
    "    the automatic NOT NULL char/text keep-text rule (--null_notnull_text=keep_text), THEN\n"
    "    null_values (replaces for all columns), THEN the endpoint marker CSV_NULL_VALUE. Empty-field\n"
    "    handling is separate (always NULL) and NOT included here. Names are matched case-\n"
    "    insensitively. With every setting unset/off this returns [CSV_NULL_VALUE] (if non-empty) or\n"
    "    [] — i.e. today's behaviour. `category` is the column's type_category; it is only consulted\n"
    "    for the automatic rule (which applies to 'varchar'/'text' only). When it is None the\n"
    "    automatic rule is inert, so an un-wired caller keeps today's behaviour exactly.\"\"\"\n"
    "    if NULL_RULES is not None:\n"
    "        key = (str(schema).lower(), str(table).lower(), str(column).lower())\n"
    "        if key in NULL_RULES:\n"
    "            return list(NULL_RULES[key])   # explicit per-column rule wins over everything\n"
    '    if NULL_NOTNULL_TEXT == "keep_text" and category in _NN_TEXT_CATEGORIES:\n'
    "        if _nn_is_notnull(schema, table, column):\n"
    "            return []   # NOT NULL char/text: no text value becomes NULL (keep as data)\n"
    "    if NULL_VALUES is not None:\n"
    "        return list(NULL_VALUES)\n"
    "    return [CSV_NULL_VALUE] if CSV_NULL_VALUE else []\n"
    "\n"
    "\n"
    "def _nn_register_nullability(schema, table, colmap):\n"
    '    """Record this table\'s column nullability for the automatic NOT NULL char/text rule.\n'
    "    `colmap` is {column_name: 'YES'|'NO'} (information_schema.columns.is_nullable). Names are\n"
    "    lower-cased. Idempotent; call once per table at startup (loader/validate) or when the CDC\n"
    "    worker builds/refreshes its table context. A no-op unless null_notnull_text=keep_text.\"\"\"\n"
    "    if not colmap:\n"
    "        return\n"
    "    key = (str(schema).lower(), str(table).lower())\n"
    '    _NN_NULLABILITY[key] = {str(c).lower(): str(n).upper() for c, n in colmap.items()}\n'
    "\n"
    "\n"
    "def _nn_is_notnull(schema, table, column):\n"
    '    """True iff the column is known NOT NULL. Raises NullRulesError (fail loud, never silently\n'
    "    skip) if the table was registered but this column's nullability is unknown — the caller\n"
    "    must register from the Job 1 mapping or, for an OLDER mapping, from one catalog query.\"\"\"\n"
    "    key = (str(schema).lower(), str(table).lower())\n"
    "    tbl = _NN_NULLABILITY.get(key)\n"
    "    if tbl is None:\n"
    "        raise NullRulesError(\n"
    '            f"null_notnull_text=keep_text needs column nullability for {key[0]}.{key[1]} but it "\n'
    '            f"was never registered. The job must call _nn_register_nullability() for the table "\n'
    '            f"(from the Job 1 mapping is_nullable, or one information_schema query for an older "\n'
    '            f"mapping). Refusing to guess.")\n'
    "    col = str(column).lower()\n"
    "    if col not in tbl:\n"
    "        raise NullRulesError(\n"
    '            f"null_notnull_text=keep_text: nullability of column {col!r} in {key[0]}.{key[1]} is "\n'
    '            f"unknown (not in the registered mapping/catalog). Refusing to guess whether it is "\n'
    '            f"NOT NULL; re-run Job 1 discovery for this table or check the column name.")\n'
    '    return tbl[col] == "NO"\n'
    "\n"
    "\n"
    "def _nn_register_from_mapping(schema, table, config):\n"
    '    """Register nullability from the Job 1 mapping if it carries is_nullable per column.\n'
    "    The current discovery writes is_nullable on each column_mapping entry (action==\'map\').\n"
    "    Returns True if it registered (new mapping), False if NO mapped entry carried is_nullable\n"
    "    (an OLDER mapping) — in which case the caller MUST register from one catalog query (or\n"
    "    fail loud). A no-op that returns True when null_notnull_text=off (nothing to do).\"\"\"\n"
    '    if NULL_NOTNULL_TEXT != "keep_text":\n'
    "        return True\n"
    "    colmap = {}\n"
    "    saw_flag = False\n"
    '    for m in (config.get("column_mapping") or []):\n'
    '        if isinstance(m, dict) and m.get("action") == "map":\n'
    '            tgt = m.get("target_column")\n'
    '            nn = m.get("is_nullable")\n'
    "            if tgt and nn is not None:\n"
    "                saw_flag = True\n"
    "                colmap[tgt] = nn\n"
    "    if not saw_flag:\n"
    "        return False\n"
    "    _nn_register_nullability(schema, table, colmap)\n"
    "    return True\n"
)

# --- (1c) Extend _apply_null_settings to also parse the new arg. ---------------------------
A_APPLY = (
    "def _apply_null_settings(null_values_arg, null_rules_arg):\n"
    '    """Parse the --null_values / --null_rules Glue args into the NULL_VALUES / NULL_RULES\n'
    "    globals. The sentinel '__NULL_UNSET__' (and blank/absent) means \"unset\" (Glue cannot pass an\n"
    "    empty arg value, so create_glue_jobs sends the sentinel for a blank setting). Raises\n"
    "    NullRulesError on a malformed setting. Returns a short human summary for the startup log.\"\"\"\n"
    "    global NULL_VALUES, NULL_RULES\n"
    "    NULL_VALUES = _parse_null_values(null_values_arg)\n"
    "    NULL_RULES = _parse_null_rules(null_rules_arg)\n"
)
A_APPLY_NEW = (
    "def _apply_null_notnull_text(raw):\n"
    '    """Parse the optional --null_notnull_text arg into the NULL_NOTNULL_TEXT global.\n'
    "    off (default) | keep_text. Blank / absent / the '__NULL_UNSET__' sentinel -> \"off\"\n"
    "    (byte-identical to today). An invalid value -> WARN and use \"off\" (same contract as other\n"
    "    optional args). Returns the resolved value.\"\"\"\n"
    "    global NULL_NOTNULL_TEXT\n"
    '    val = str(raw).strip().lower() if isinstance(raw, str) else ""\n'
    '    if val in ("", "__null_unset__", "off"):\n'
    '        NULL_NOTNULL_TEXT = "off"\n'
    '    elif val == "keep_text":\n'
    '        NULL_NOTNULL_TEXT = "keep_text"\n'
    "    else:\n"
    '        print(f"  WARNING ignoring invalid null_notnull_text={raw!r} (use off|keep_text); "\n'
    '              f"keeping off")\n'
    '        NULL_NOTNULL_TEXT = "off"\n'
    "    return NULL_NOTNULL_TEXT\n"
    "\n"
    "\n"
    "def _apply_null_settings(null_values_arg, null_rules_arg, null_notnull_text_arg=None):\n"
    '    """Parse the --null_values / --null_rules Glue args into the NULL_VALUES / NULL_RULES\n'
    "    globals. The sentinel '__NULL_UNSET__' (and blank/absent) means \"unset\" (Glue cannot pass an\n"
    "    empty arg value, so create_glue_jobs sends the sentinel for a blank setting). Raises\n"
    "    NullRulesError on a malformed setting. Returns a short human summary for the startup log.\n"
    "    null_notnull_text_arg (optional) sets the automatic NOT NULL char/text keep-text rule;\n"
    "    absent/None leaves it off (today's behaviour).\"\"\"\n"
    "    global NULL_VALUES, NULL_RULES\n"
    "    NULL_VALUES = _parse_null_values(null_values_arg)\n"
    "    NULL_RULES = _parse_null_rules(null_rules_arg)\n"
    "    if null_notnull_text_arg is not None:\n"
    "        _apply_null_notnull_text(null_notnull_text_arg)\n"
)

# --- (1d) Extend the summary return so keep_text is visible in the startup log. ------------
A_SUMMARY = (
    '    return f"null_values={nv}; null_rules={nr}; endpoint CSV_NULL_VALUE={CSV_NULL_VALUE!r}"\n'
)
A_SUMMARY_NEW = (
    '    return (f"null_values={nv}; null_rules={nr}; null_notnull_text={NULL_NOTNULL_TEXT}; "\n'
    '            f"endpoint CSV_NULL_VALUE={CSV_NULL_VALUE!r}")\n'
)


def patch_shared_block(src):
    """Apply the shared-block edits (1a..1d) to one script copy. Idempotent + fail-closed."""
    for anc, label in ((A_GLOBALS, "shared/globals"), (A_EFF, "shared/_effective_null_markers"),
                       (A_APPLY, "shared/_apply_null_settings"), (A_SUMMARY, "shared/summary")):
        _require_once(src, anc, label)
    src = src.replace(A_GLOBALS, A_GLOBALS_NEW)
    src = src.replace(A_EFF, A_EFF_NEW)
    src = src.replace(A_APPLY, A_APPLY_NEW)
    src = src.replace(A_SUMMARY, A_SUMMARY_NEW)
    return src


# =============================================================================================
# 2) CALL-SITE WIRING shared by all FIVE scripts.
# =============================================================================================

# (2a) optional-arg list: add the new arg name right after null_rules (works whether the list
#      continues with ',' or ends with ']').
A_OPTLIST = '"null_values", "null_rules"'
A_OPTLIST_NEW = '"null_values", "null_rules", "null_notnull_text"'

# (2b) arg read + pass to _apply_null_settings. The _summary= line is byte-identical in all five.
A_ARGREAD = "        _summary = _apply_null_settings(_nvl, _nrl)\n"
A_ARGREAD_NEW = (
    "        _nnt = ov.get(\"null_notnull_text\") if \"null_notnull_text\" in ov else None\n"
    "        _summary = _apply_null_settings(_nvl, _nrl, _nnt)\n"
)
# The guard that decides whether the null block runs must also fire when ONLY the new arg is
# set. Original guard: `if _nvl is not None or _nrl is not None:`
A_ARGGUARD = "    if _nvl is not None or _nrl is not None:\n"
A_ARGGUARD_NEW = ("    _nnt_present = \"null_notnull_text\" in ov\n"
                  "    if _nvl is not None or _nrl is not None or _nnt_present:\n")


def patch_arg_read(src):
    """(2a)+(2b): wire the new arg through the optional list and _apply_null_settings call."""
    _require_once(src, A_OPTLIST, "optlist")
    _require_once(src, A_ARGREAD, "argread")
    _require_once(src, A_ARGGUARD, "argguard")
    src = src.replace(A_OPTLIST, A_OPTLIST_NEW)
    src = src.replace(A_ARGGUARD, A_ARGGUARD_NEW)
    src = src.replace(A_ARGREAD, A_ARGREAD_NEW)
    return src


# =============================================================================================
# 3) LOADER / VALIDATE consumer: pass `category` into _effective_null_markers (inside
#    null_marker_expr) so the automatic rule can see the type, and register nullability before
#    the transform runs.
# =============================================================================================

A_NME_CALL = "        for _m in _effective_null_markers(schema, table, c):\n"
A_NME_CALL_NEW = "        for _m in _effective_null_markers(schema, table, c, category):\n"


def patch_null_marker_expr(src):
    """Make null_marker_expr pass the column category into _effective_null_markers."""
    _require_once(src, A_NME_CALL, "null_marker_expr/category")
    return src.replace(A_NME_CALL, A_NME_CALL_NEW)


# Loader (job2 / MRL): register nullability right before the null_marker_expr loop, where
# `config`, `dsql_schema`, `dsql_table` are all in scope. Catalog fallback via connect_dsql().
A_LOADER_REG = (
    "    for _w in _null_rules_unknown_warnings(dsql_schema, dsql_table, df.columns):\n"
    "        print(f\"    \u26a0\ufe0f {_w}\")\n"
    "    for c in df.columns:\n"
    "        df = df.withColumn(c, null_marker_expr(c, type_categories.get(c), dsql_schema, dsql_table))\n"
)
A_LOADER_REG_NEW = (
    "    for _w in _null_rules_unknown_warnings(dsql_schema, dsql_table, df.columns):\n"
    "        print(f\"    \u26a0\ufe0f {_w}\")\n"
    "    _nn_register_for_table(dsql_schema, dsql_table, config)\n"
    "    for c in df.columns:\n"
    "        df = df.withColumn(c, null_marker_expr(c, type_categories.get(c), dsql_schema, dsql_table))\n"
)

# The per-script nullability-registration helper (loader/validate flavour): tries the Job 1
# mapping first, else runs ONE information_schema query via the script's own connect_dsql().
# connect_dsql has a different signature across scripts, so this helper is injected per script
# (NOT part of the byte-identical shared block) just before `def null_marker_expr(`.
_LOADER_REG_HELPER = (
    "def _nn_register_for_table(dsql_schema, dsql_table, config):\n"
    "    \"\"\"Register this table's column nullability for --null_notnull_text=keep_text. Uses the\n"
    "    Job 1 mapping's per-column is_nullable when present; for an OLDER mapping without it,\n"
    "    runs ONE information_schema.columns query (cached by _nn_register_nullability). A no-op\n"
    "    when the arg is off. Fails loud later (in _nn_is_notnull) only if nullability is truly\n"
    "    unobtainable.\"\"\"\n"
    "    if NULL_NOTNULL_TEXT != \"keep_text\":\n"
    "        return\n"
    "    if _nn_register_from_mapping(dsql_schema, dsql_table, config):\n"
    "        return\n"
    "    print(f\"    \u21aa null_notnull_text=keep_text: {dsql_schema}.{dsql_table} mapping predates \"\n"
    "          f\"is_nullable; reading nullability from information_schema (one query).\")\n"
    "    _conn = connect_dsql()\n"
    "    try:\n"
    "        _cur = _conn.cursor()\n"
    "        _cur.execute(\n"
    "            \"SELECT column_name, is_nullable FROM information_schema.columns \"\n"
    "            \"WHERE table_schema = %s AND table_name = %s\",\n"
    "            (dsql_schema, dsql_table))\n"
    "        _nn_register_nullability(dsql_schema, dsql_table,\n"
    "                                 {r[0]: r[1] for r in _cur.fetchall()})\n"
    "        _cur.close()\n"
    "    finally:\n"
    "        try:\n"
    "            _conn.close()\n"
    "        except Exception:\n"
    "            pass\n"
    "\n"
    "\n"
)
A_NME_DEF = "def null_marker_expr(c, category, schema=None, table=None):\n"


def patch_loader(src):
    """job2_load.py / MRL loader: inject the registration helper, register before the transform
    loop, and pass category into null_marker_expr."""
    _require_once(src, A_NME_DEF, "loader/null_marker_expr def")
    _require_once(src, A_LOADER_REG, "loader/register-site")
    src = src.replace(A_NME_DEF, _LOADER_REG_HELPER + A_NME_DEF)
    src = src.replace(A_LOADER_REG, A_LOADER_REG_NEW)
    src = patch_null_marker_expr(src)
    return src


def patch_validate(src):
    """job3_validate.py: inject the registration helper, register in validate_one_table before
    apply_transform, and pass category into null_marker_expr."""
    _require_once(src, A_NME_DEF, "validate/null_marker_expr def")
    _require_once(src, A_VAL_REG, "validate/register-site")
    src = src.replace(A_NME_DEF, _LOADER_REG_HELPER + A_NME_DEF)
    src = src.replace(A_VAL_REG, A_VAL_REG_NEW)
    src = patch_null_marker_expr(src)
    return src


# Validate: register just before apply_transform (config + schema/table in scope there).
A_VAL_REG = (
    "    df_t = apply_transform(df, target_columns, type_categories, dsql_schema, dsql_table)\n"
)
A_VAL_REG_NEW = (
    "    _nn_register_for_table(dsql_schema, dsql_table, config)\n"
    "    df_t = apply_transform(df, target_columns, type_categories, dsql_schema, dsql_table)\n"
)


# =============================================================================================
# 4) CDC consumer: pass the column category into every _effective_null_markers call site and
#    register nullability in build_table_context (one query fallback for an old mapping).
# =============================================================================================

A_CDC_COL = '_effective_null_markers(ctx["dsql_schema"], ctx["dsql_table"], c)'
A_CDC_COL_NEW = ('_effective_null_markers(ctx["dsql_schema"], ctx["dsql_table"], c, '
                 "col_category.get(c, 'varchar'))")
A_CDC_PK = '_effective_null_markers(ctx["dsql_schema"], ctx["dsql_table"], pk_col)'
A_CDC_PK_NEW = '_effective_null_markers(ctx["dsql_schema"], ctx["dsql_table"], pk_col, pk_category)'
A_CDC_KCOL = '_effective_null_markers(ctx["dsql_schema"], ctx["dsql_table"], _kcol)'
A_CDC_KCOL_NEW = '_effective_null_markers(ctx["dsql_schema"], ctx["dsql_table"], _kcol, kcat)'

# Register nullability in build_table_context, right before it returns the ctx dict. `config`
# is in scope. Old-mapping fallback = ONE information_schema query via connect_dsql().
A_CDC_REG = "    label = f\"{dsql_schema}.{dsql_table}\"\n    for _w in _null_rules_unknown_warnings(\n"
A_CDC_REG_NEW = (
    "    label = f\"{dsql_schema}.{dsql_table}\"\n"
    "    _nn_register_cdc(dsql_schema, dsql_table, config)\n"
    "    for _w in _null_rules_unknown_warnings(\n"
)

_CDC_REG_HELPER = (
    "def _nn_register_cdc(dsql_schema, dsql_table, config):\n"
    "    \"\"\"Register a CDC table's column nullability for --null_notnull_text=keep_text (once per\n"
    "    table, at context build). Mapping first; OLDER mapping -> ONE information_schema query via\n"
    "    connect_dsql(). No-op when the arg is off. Re-registers after DDL via the same catalog read\n"
    "    the CDC job already does (handle_schema_changes -> _nn_register_nullability on refresh).\"\"\"\n"
    "    if NULL_NOTNULL_TEXT != \"keep_text\":\n"
    "        return\n"
    "    if _nn_register_from_mapping(dsql_schema, dsql_table, config):\n"
    "        return\n"
    "    print(f\"  \u21aa null_notnull_text=keep_text: {dsql_schema}.{dsql_table} mapping predates \"\n"
    "          f\"is_nullable; reading nullability from information_schema (one query).\")\n"
    "    _conn = connect_dsql()\n"
    "    try:\n"
    "        _cur = _conn.cursor()\n"
    "        _cur.execute(\n"
    "            \"SELECT column_name, is_nullable FROM information_schema.columns \"\n"
    "            \"WHERE table_schema = %s AND table_name = %s\",\n"
    "            (dsql_schema, dsql_table))\n"
    "        _nn_register_nullability(dsql_schema, dsql_table,\n"
    "                                 {r[0]: r[1] for r in _cur.fetchall()})\n"
    "        _cur.close()\n"
    "    finally:\n"
    "        try:\n"
    "            _conn.close()\n"
    "        except Exception:\n"
    "            pass\n"
    "\n"
    "\n"
)
A_CDC_BTC_DEF = "def build_table_context(entry):\n"


def patch_cdc(src):
    """glue_cdc_continuous.py / glue_cdc_composite.py: inject the CDC registration helper, call it
    in build_table_context, and pass the category into every _effective_null_markers call site."""
    _require_once(src, A_CDC_BTC_DEF, "cdc/build_table_context def")
    _require_once(src, A_CDC_REG, "cdc/register-site")
    if src.count(A_CDC_COL) != 2:
        raise AnchorError("cdc per-column _effective_null_markers site not found exactly twice "
                          "(found %d)" % src.count(A_CDC_COL))
    # PK-column site: pk_col (continuous) or _kcol (composite) — exactly one of the two.
    has_pk = src.count(A_CDC_PK) == 1
    has_kcol = src.count(A_CDC_KCOL) == 1
    if has_pk == has_kcol:
        raise AnchorError("cdc key-column _effective_null_markers site ambiguous "
                          "(pk_col=%d, _kcol=%d)" % (src.count(A_CDC_PK), src.count(A_CDC_KCOL)))
    src = src.replace(A_CDC_BTC_DEF, _CDC_REG_HELPER + A_CDC_BTC_DEF)
    src = src.replace(A_CDC_REG, A_CDC_REG_NEW)
    src = src.replace(A_CDC_COL, A_CDC_COL_NEW)   # both per-column sites
    if has_pk:
        src = src.replace(A_CDC_PK, A_CDC_PK_NEW)
    else:
        src = src.replace(A_CDC_KCOL, A_CDC_KCOL_NEW)
    return src


# =============================================================================================
# 5) Top-level per-file dispatch for the four/five Glue scripts.
# =============================================================================================

def patch_script(kind, src):
    """kind in {'loader','validate','cdc'}; applies shared block + arg wiring + consumer edits.
    Idempotent (returns src unchanged if already patched)."""
    if _MARK in src:
        return src
    src = patch_shared_block(src)
    src = patch_arg_read(src)
    if kind == "loader":
        src = patch_loader(src)
    elif kind == "validate":
        src = patch_validate(src)
    elif kind == "cdc":
        src = patch_cdc(src)
    else:
        raise ValueError("unknown kind %r" % kind)
    return src


# =============================================================================================
# 6) JOB 1 DISCOVERY: carry is_nullable into each mapped column_mapping entry (clean route).
#    Old mappings (built before this) simply lack the key; the jobs fall back to one catalog
#    query (see _nn_register_for_table / _nn_register_cdc).
# =============================================================================================

A_JOB1 = (
    '            column_mapping.append({\n'
    '                "dms_position": dms_pos,\n'
    '                "dms_column_name": dms_original_name,\n'
    '                "action": "map",\n'
    '                "target_column": target_name,\n'
    '                "target_type": target_col[\'data_type\'],\n'
    '                "type_category": type_categories[target_name],\n'
    '                "ordinal_position": target_col[\'ordinal_position\'],\n'
    '                "max_length": target_col[\'max_length\'],\n'
    '            })\n'
)
A_JOB1_NEW = (
    '            column_mapping.append({\n'
    '                "dms_position": dms_pos,\n'
    '                "dms_column_name": dms_original_name,\n'
    '                "action": "map",\n'
    '                "target_column": target_name,\n'
    '                "target_type": target_col[\'data_type\'],\n'
    '                "type_category": type_categories[target_name],\n'
    '                "ordinal_position": target_col[\'ordinal_position\'],\n'
    '                "max_length": target_col[\'max_length\'],\n'
    '                # is_nullable (\'YES\'/\'NO\') lets the load/validate/CDC jobs apply the optional\n'
    '                # --null_notnull_text rule without a catalog query. Older mappings omit it and\n'
    '                # the jobs fall back to one information_schema read per table.\n'
    '                "is_nullable": target_col.get(\'is_nullable\'),\n'
    '            })\n'
)


def patch_job1(src):
    if "is_nullable" in src and '"is_nullable": target_col' in src:
        return src
    _require_once(src, A_JOB1, "job1/column_mapping.append")
    return src.replace(A_JOB1, A_JOB1_NEW)


# =============================================================================================
# 7) PLUMBING — end to end, mirroring null_values/null_rules exactly.
#    params_csv.py (+ MRL copy), resolve_task.py, create_glue_jobs.py, startup.asl.json,
#    mrl_params_csv.py, mrl_ensure_job.py, mrl_plan.py, mrl_settings_classification.py.
# =============================================================================================

# ---- params_csv.py / mrl_params_csv.py (byte-identical copies) ----
# (7a) OPTIONAL_DEFAULTS: add null_notnull_text after null_rules.
A_PC_DEFAULTS = '    "null_values": "",\n    "null_rules": "",\n}\n'
A_PC_DEFAULTS_NEW = (
    '    "null_values": "",\n    "null_rules": "",\n'
    '    # Optional automatic NOT NULL char/text keep-text rule. "off" (default) = today\'s\n'
    '    # behaviour EXACTLY. "keep_text": a NOT NULL char/varchar/text column never converts a\n'
    '    # text value (endpoint marker or a null_values entry) to SQL NULL - it stores the text.\n'
    '    # Precedence: null_rules > this > null_values > endpoint. Set BEFORE the full load.\n'
    '    "null_notnull_text": "off",\n}\n'
)
# (7b) PIPELINE_KEYS tail.
A_PC_KEYS = '                 "null_values", "null_rules")\n'
A_PC_KEYS_NEW = '                 "null_values", "null_rules", "null_notnull_text")\n'


def patch_params_csv(src):
    if '"null_notnull_text"' in src:
        return src
    _require_once(src, A_PC_DEFAULTS, "params_csv/OPTIONAL_DEFAULTS")
    _require_once(src, A_PC_KEYS, "params_csv/PIPELINE_KEYS")
    src = src.replace(A_PC_DEFAULTS, A_PC_DEFAULTS_NEW)
    src = src.replace(A_PC_KEYS, A_PC_KEYS_NEW)
    return src


# ---- resolve_task.py ----
# (7c) SETTINGS_DEFAULTS tail.
A_RT_DEFAULTS = '    "null_values": "",\n    "null_rules": "",\n}\n'
A_RT_DEFAULTS_NEW = (
    '    "null_values": "",\n    "null_rules": "",\n'
    '    # Optional automatic NOT NULL char/text keep-text rule (--null_notnull_text). "off"\n'
    '    # (default) = today\'s behaviour. "keep_text" keeps a text value in a NOT NULL char/text\n'
    '    # column as data. Validated below (invalid -> warn and treat as off).\n'
    '    "null_notnull_text": "off",\n}\n'
)
# (7d) resolved output dict (the keys resolve_task emits for Step Functions).
A_RT_RESOLVED = (
    '        "nullValues": cfg["null_values"],\n'
    '        "nullRules": cfg["null_rules"],\n'
)
A_RT_RESOLVED_NEW = (
    '        "nullValues": cfg["null_values"],\n'
    '        "nullRules": cfg["null_rules"],\n'
    '        "nullNotnullText": cfg["null_notnull_text"],\n'
)
# (7e) _validate_null_settings: normalise + validate the new enum (invalid -> warn, use off).
#   The function gains an optional `warnings` param (default None) so existing one-arg callers
#   (e.g. MRL test_nulls, mrl_preflight) keep working; a bad value is a WARNING, never a hard
#   fail (off is always safe). Also pass the resolver's warnings list in at the call site.
A_RT_VDEF = "def _validate_null_settings(cfg):\n"
A_RT_VDEF_NEW = "def _validate_null_settings(cfg, warnings=None):\n"
A_RT_VCALL = "    _validate_null_settings(cfg)\n"
A_RT_VCALL_NEW = "    _validate_null_settings(cfg, warnings)\n"
A_RT_VALIDATE = (
    '    nr = str(cfg.get("null_rules", "") or "").strip()\n'
    '    cfg["null_rules"] = nr\n'
)
A_RT_VALIDATE_NEW = (
    '    nr = str(cfg.get("null_rules", "") or "").strip()\n'
    '    cfg["null_rules"] = nr\n'
    '    # Optional automatic NOT NULL char/text keep-text rule. "off" (default) | "keep_text".\n'
    '    # Blank / the __NULL_UNSET__ sentinel -> off. An invalid value is a WARNING + off (never\n'
    '    # a hard preflight failure, since off is always safe and byte-identical to today).\n'
    '    nnt = str(cfg.get("null_notnull_text", "") or "").strip().lower()\n'
    '    if nnt in ("", "__null_unset__"):\n'
    '        nnt = "off"\n'
    '    if nnt not in ("off", "keep_text"):\n'
    '        _msg = (f"null_notnull_text {cfg.get(\'null_notnull_text\')!r} is not off|keep_text; '
    'using off")\n'
    '        if warnings is not None:\n'
    '            warnings.append(_msg)\n'
    '        else:\n'
    '            print(f"(warn) {_msg}")\n'
    '        nnt = "off"\n'
    '    cfg["null_notnull_text"] = nnt\n'
)


def patch_resolve_task(src):
    if '"null_notnull_text"' in src or "nullNotnullText" in src:
        return src
    _require_once(src, A_RT_DEFAULTS, "resolve_task/SETTINGS_DEFAULTS")
    _require_once(src, A_RT_RESOLVED, "resolve_task/resolved")
    _require_once(src, A_RT_VALIDATE, "resolve_task/_validate_null_settings")
    _require_once(src, A_RT_VDEF, "resolve_task/_validate_null_settings def")
    _require_once(src, A_RT_VCALL, "resolve_task/_validate_null_settings call")
    src = src.replace(A_RT_DEFAULTS, A_RT_DEFAULTS_NEW)
    src = src.replace(A_RT_RESOLVED, A_RT_RESOLVED_NEW)
    src = src.replace(A_RT_VDEF, A_RT_VDEF_NEW)
    src = src.replace(A_RT_VCALL, A_RT_VCALL_NEW)
    src = src.replace(A_RT_VALIDATE, A_RT_VALIDATE_NEW)
    return src


# ---- create_glue_jobs.py ----
# (7f) documented keys block.
A_CGJ_DOC = (
    '  "null_rules",        # optional: per-column NULL overrides, \'schema.table.column=none|V1|V2\'\n'
    '                       # entries joined by \';\'; set as --null_rules ("" -> "__NULL_UNSET__").\n'
    '                       # Blank = today. (Both set on the SAME jobs as --csv_null_value.)\n'
)
A_CGJ_DOC_NEW = (
    '  "null_rules",        # optional: per-column NULL overrides, \'schema.table.column=none|V1|V2\'\n'
    '                       # entries joined by \';\'; set as --null_rules ("" -> "__NULL_UNSET__").\n'
    '                       # Blank = today. (Both set on the SAME jobs as --csv_null_value.)\n'
    '  "null_notnull_text", # optional: "off" (default) | "keep_text". keep_text makes a NOT NULL\n'
    '                       # char/varchar/text column keep a text value as data (never SQL NULL).\n'
    '                       # Set on the SAME jobs as --null_values ("" -> "__NULL_UNSET__").\n'
)
# (7g) the arg-emit block (blank -> __NULL_UNSET__, same jobs as null_values/null_rules).
A_CGJ_EMIT = (
    '            _nrv = event.get("null_rules")\n'
    '            if _nrv is not None:\n'
    '                _nrv = str(_nrv)\n'
    '                args["--null_rules"] = _nrv if _nrv != "" else "__NULL_UNSET__"\n'
)
A_CGJ_EMIT_NEW = (
    '            _nrv = event.get("null_rules")\n'
    '            if _nrv is not None:\n'
    '                _nrv = str(_nrv)\n'
    '                args["--null_rules"] = _nrv if _nrv != "" else "__NULL_UNSET__"\n'
    '            _nnt = event.get("null_notnull_text")\n'
    '            if _nnt is not None:\n'
    '                _nnt = str(_nnt)\n'
    '                args["--null_notnull_text"] = _nnt if _nnt != "" else "__NULL_UNSET__"\n'
)


def patch_create_glue_jobs(src):
    if "null_notnull_text" in src:
        return src
    _require_once(src, A_CGJ_DOC, "create_glue_jobs/doc")
    _require_once(src, A_CGJ_EMIT, "create_glue_jobs/emit")
    src = src.replace(A_CGJ_DOC, A_CGJ_DOC_NEW)
    src = src.replace(A_CGJ_EMIT, A_CGJ_EMIT_NEW)
    return src


# ---- startup.asl.json (and mrl-task.asl.json): add null_notnull_text wherever null_rules is
#      passed into a CreateGlueJobs payload. Each occurrence is the same 1-line pattern. ----
A_ASL = '          "null_rules.$": "$.resolved.nullRules",\n'
A_ASL_NEW = ('          "null_rules.$": "$.resolved.nullRules",\n'
             '          "null_notnull_text.$": "$.resolved.nullNotnullText",\n')
A_ASL_MRL = '     "null_rules.$": "$.resolved.nullRules",\n'
A_ASL_MRL_NEW = ('     "null_rules.$": "$.resolved.nullRules",\n'
                 '     "null_notnull_text.$": "$.resolved.nullNotnullText",\n')


def patch_asl(src, style):
    """Add the new key beside EVERY null_rules passthrough in a Step Functions definition AND in
    the ResultSelector that PRODUCES $.resolved from the resolver Payload (so the static path
    audit sees it produced on the route in). style: 'startup' (startup.asl.json) or 'mrl'
    (mrl-task.asl.json). Idempotent; requires at least one passthrough present."""
    if "nullNotnullText" in src:
        return src
    anc, new = (A_ASL, A_ASL_NEW) if style == "startup" else (A_ASL_MRL, A_ASL_MRL_NEW)
    if src.count(anc) == 0:
        raise AnchorError("ASL(%s): no null_rules passthrough found" % style)
    src = src.replace(anc, new)
    # Producer (ResultSelector mapping $.Payload.nullRules -> resolved.nullRules). Indent is
    # 8 spaces in startup.asl.json, 4 in mrl-task.asl.json.
    prod, prod_new = (A_ASL_PROD8, A_ASL_PROD8_NEW) if style == "startup" else \
                     (A_ASL_PROD4, A_ASL_PROD4_NEW)
    _require_once(src, prod, "ASL(%s)/resolved-producer" % style)
    src = src.replace(prod, prod_new)
    return src


# ResultSelector producer lines (map resolver Payload -> $.resolved). startup = 8-space indent;
# mrl-task = 4-space indent.
A_ASL_PROD8 = '        "nullRules.$": "$.Payload.nullRules",\n'
A_ASL_PROD8_NEW = ('        "nullRules.$": "$.Payload.nullRules",\n'
                   '        "nullNotnullText.$": "$.Payload.nullNotnullText",\n')
A_ASL_PROD4 = '    "nullRules.$": "$.Payload.nullRules",\n'
A_ASL_PROD4_NEW = ('    "nullRules.$": "$.Payload.nullRules",\n'
                   '    "nullNotnullText.$": "$.Payload.nullNotnullText",\n')


# ---- MRL mrl_ensure_job.py: add the arg to the range-load + count job arguments (USED). ----
A_MRL_EJ = (
    '    if event.get("null_rules") is not None:\n'
    '        v = str(event["null_rules"])\n'
    '        args["--null_rules"] = v if v != "" else "__NULL_UNSET__"\n'
)
A_MRL_EJ_NEW = (
    '    if event.get("null_rules") is not None:\n'
    '        v = str(event["null_rules"])\n'
    '        args["--null_rules"] = v if v != "" else "__NULL_UNSET__"\n'
    '    if event.get("null_notnull_text") is not None:\n'
    '        v = str(event["null_notnull_text"])\n'
    '        args["--null_notnull_text"] = v if v != "" else "__NULL_UNSET__"\n'
)


def patch_mrl_ensure_job(src):
    if "null_notnull_text" in src:
        return src
    _require_once(src, A_MRL_EJ, "mrl_ensure_job/emit")
    return src.replace(A_MRL_EJ, A_MRL_EJ_NEW)


# ---- MRL mrl_plan.py: forward the new key in the "null" block it passes to the jobs. ----
A_MRL_PLAN = (
    '                 "null_rules": event.get("null_rules")},\n'
)
A_MRL_PLAN_NEW = (
    '                 "null_rules": event.get("null_rules"),\n'
    '                 "null_notnull_text": event.get("null_notnull_text")},\n'
)


def patch_mrl_plan(src):
    if "null_notnull_text" in src:
        return src
    _require_once(src, A_MRL_PLAN, "mrl_plan/null-block")
    return src.replace(A_MRL_PLAN, A_MRL_PLAN_NEW)


# ---- MRL mrl_preflight.py: carry null_notnull_text through like null_values/null_rules. ----
A_MRL_PF_READ = (
    '    null_rules = str(event.get("nullRules") if event.get("nullRules") is not None\n'
    '                     else cfg.get("null_rules", "") or "")\n'
)
A_MRL_PF_READ_NEW = (
    '    null_rules = str(event.get("nullRules") if event.get("nullRules") is not None\n'
    '                     else cfg.get("null_rules", "") or "")\n'
    '    null_notnull_text = str(event.get("nullNotnullText") if event.get("nullNotnullText") '
    'is not None\n'
    '                            else cfg.get("null_notnull_text", "") or "")\n'
)
A_MRL_PF_OUT = (
    '        "nullValues": null_values,\n'
    '        "nullRules": null_rules,\n'
)
A_MRL_PF_OUT_NEW = (
    '        "nullValues": null_values,\n'
    '        "nullRules": null_rules,\n'
    '        "nullNotnullText": null_notnull_text,\n'
)


def patch_mrl_preflight(src):
    if "nullNotnullText" in src or "null_notnull_text" in src:
        return src
    _require_once(src, A_MRL_PF_READ, "mrl_preflight/read")
    _require_once(src, A_MRL_PF_OUT, "mrl_preflight/out")
    src = src.replace(A_MRL_PF_READ, A_MRL_PF_READ_NEW)
    src = src.replace(A_MRL_PF_OUT, A_MRL_PF_OUT_NEW)
    return src


# ---- MRL classification table: classify the new key as USED (reaches MRL range-load + count
#      jobs just like null_values/null_rules). ----
A_MRL_CLASS = (
    '    "null_rules": (USED, "range-load + count job arg --null_rules (NULL handling)"),\n'
)
A_MRL_CLASS_NEW = (
    '    "null_rules": (USED, "range-load + count job arg --null_rules (NULL handling)"),\n'
    '    "null_notnull_text": (USED, "range-load + count job arg --null_notnull_text (NULL '
    'handling)"),\n'
)


def patch_mrl_classification(src):
    if "null_notnull_text" in src:
        return src
    _require_once(src, A_MRL_CLASS, "mrl_classification")
    return src.replace(A_MRL_CLASS, A_MRL_CLASS_NEW)


# ---- config/params.example.csv : add a null_notnull_text,off row + a short comment. ----
A_CFG_CSV = "null_values,\nnull_rules,\n"
A_CFG_CSV_NEW = (
    "null_values,\nnull_rules,\n"
    "# null_notnull_text: off (default) | keep_text. keep_text makes a NOT NULL char/varchar/text\n"
    "#   column keep a text value (the endpoint marker or a null_values entry) AS DATA instead of\n"
    "#   converting it to SQL NULL - for data that legitimately contains the text 'NULL','NA',...\n"
    "#   in NOT NULL columns. Nullable columns and non-text types are unchanged. Precedence:\n"
    "#   a null_rules entry > null_notnull_text > null_values > the endpoint marker. SET BEFORE\n"
    "#   THE FULL LOAD. Off = byte-identical to today.\n"
    "null_notnull_text,off\n"
)
# ---- config/pipeline.example.json : add the key after null_rules. ----
A_CFG_JSON = '  "null_values": "",\n  "null_rules": ""\n}\n'
A_CFG_JSON_NEW = '  "null_values": "",\n  "null_rules": "",\n  "null_notnull_text": "off"\n}\n'


def patch_params_example_csv(src):
    if "null_notnull_text" in src:
        return src
    _require_once(src, A_CFG_CSV, "params.example.csv")
    return src.replace(A_CFG_CSV, A_CFG_CSV_NEW)


def patch_pipeline_example_json(src):
    if "null_notnull_text" in src:
        return src
    _require_once(src, A_CFG_JSON, "pipeline.example.json")
    return src.replace(A_CFG_JSON, A_CFG_JSON_NEW)


# ---- manual_range_pipeline/README.md : the settings-source table is generated from the
#      classification data and verified verbatim; insert the new row in alpha order (it sorts
#      before null_rules). Also one short sentence in the NULL section (do not rewrite docs). ----
A_MRL_README_ROW = "| `null_rules` | USED | range-load + count job arg --null_rules (NULL handling) |\n"
A_MRL_README_ROW_NEW = (
    "| `null_notnull_text` | USED | range-load + count job arg --null_notnull_text "
    "(NULL handling) |\n"
    "| `null_rules` | USED | range-load + count job arg --null_rules (NULL handling) |\n"
)
A_MRL_README_PARA = (
    "`null_rules=<schema>.<table>.<column>=none` to keep the text as data for that column.\n"
)
A_MRL_README_PARA_NEW = (
    "`null_rules=<schema>.<table>.<column>=none` to keep the text as data for that column.\n"
    "\n"
    "- `null_notnull_text` (`off` default | `keep_text`): a fleet-wide switch — with `keep_text`,\n"
    "  any NOT NULL `char`/`varchar`/`text` column keeps a text value (the endpoint marker or a\n"
    "  `null_values` entry) as data instead of converting it to SQL NULL, so you need not list\n"
    "  hundreds of columns in `null_rules`. Precedence: an explicit `null_rules` entry > this >\n"
    "  `null_values` > the endpoint marker. Reaches the MRL range-load and count jobs like the\n"
    "  other NULL settings.\n"
)


def patch_mrl_readme(src):
    if "null_notnull_text" in src:
        return src
    _require_once(src, A_MRL_README_ROW, "mrl_readme/table-row")
    _require_once(src, A_MRL_README_PARA, "mrl_readme/para")
    src = src.replace(A_MRL_README_ROW, A_MRL_README_ROW_NEW)
    src = src.replace(A_MRL_README_PARA, A_MRL_README_PARA_NEW)
    return src


# ---- main README.md : one bullet under the existing NULL-marker note (CDC-safety limit noted). --
A_README = (
    "- **NULL marker:** only an empty field or the DMS endpoint's `CsvNullValue` (default `NULL`) is\n"
    "  stored as NULL; `NA`, `NONE`, `N/A` are stored as text. Don't change `CsvNullValue` mid-migration.\n"
)
A_README_NEW = (
    "- **NULL marker:** only an empty field or the DMS endpoint's `CsvNullValue` (default `NULL`) is\n"
    "  stored as NULL; `NA`, `NONE`, `N/A` are stored as text. Don't change `CsvNullValue` mid-migration.\n"
    "- **Optional `null_notnull_text=keep_text`** (default `off`): for a NOT NULL `char`/`varchar`/\n"
    "  `text` column, a text value (the endpoint marker or a `null_values` entry) is kept AS DATA\n"
    "  rather than converted to SQL NULL — so data that legitimately holds the text `NULL`/`NA` in\n"
    "  NOT NULL columns loads as-is without listing every column in `null_rules`. Precedence: a\n"
    "  `null_rules` entry > `null_notnull_text` > `null_values` > the endpoint marker. It is SAFE on\n"
    "  CDC because DMS S3 CSV (`AddColumnName=true`) sends a FULL after-image on every change row —\n"
    "  an unchanged column in an UPDATE carries its real value, never the marker — so it cannot\n"
    "  overwrite a NOT NULL text column with the literal `NULL`. Empty field is always NULL. `off`\n"
    "  is byte-identical to today.\n"
)
A_RUNBOOK = (
    "- **`null_values` / `null_rules` (customer NULL handling)** are also baked into the load/load-big/\n"
    "  validate/CDC (incl. `ck`/`bg` fork) job args as `--null_values` / `--null_rules` at job creation\n"
)
A_RUNBOOK_NEW = (
    "- **`null_notnull_text` (optional automatic NOT NULL char/text keep-text rule)** is baked into the\n"
    "  SAME jobs as `--null_notnull_text` (`off` default | `keep_text`). `keep_text` stops a NOT NULL\n"
    "  char/varchar/text column from converting any text value (endpoint marker or `null_values` entry)\n"
    "  to NULL — it stores the text. Precedence: a `null_rules` entry > `null_notnull_text` >\n"
    "  `null_values` > endpoint. CDC-safe: DMS S3 CSV sends a full after-image per change row\n"
    "  (`AddColumnName=true`), so an unchanged UPDATE column carries its real value, never the marker.\n"
    "- **`null_values` / `null_rules` (customer NULL handling)** are also baked into the load/load-big/\n"
    "  validate/CDC (incl. `ck`/`bg` fork) job args as `--null_values` / `--null_rules` at job creation\n"
)


def patch_readme(src):
    if "null_notnull_text" in src:
        return src
    _require_once(src, A_README, "README/null-marker")
    return src.replace(A_README, A_README_NEW)


def patch_runbook(src):
    if "null_notnull_text" in src:
        return src
    _require_once(src, A_RUNBOOK, "RUNBOOK/null-handling")
    return src.replace(A_RUNBOOK, A_RUNBOOK_NEW)


# =============================================================================================
# 8) The ONE manifest: repo-relative path -> patcher. Both deliverables iterate THIS list, so
#    the repo integration and the hand-patch folder apply the identical set of edits.
# =============================================================================================

def _p_loader(s):
    return patch_script("loader", s)


def _p_validate(s):
    return patch_script("validate", s)


def _p_cdc(s):
    return patch_script("cdc", s)


def _p_asl_startup(s):
    return patch_asl(s, "startup")


def _p_asl_mrl(s):
    return patch_asl(s, "mrl")


# (repo-relative path, patcher). Order is deterministic.
FILES = [
    # --- the four byte-identical Glue script copies + MRL copy (shared null block) ---
    ("scripts/job2_load.py", _p_loader),
    ("scripts/job3_validate.py", _p_validate),
    ("scripts/glue_cdc_continuous.py", _p_cdc),
    ("scripts/glue_cdc_composite.py", _p_cdc),
    ("manual_range_pipeline/scripts/job2_load_manual_range.py", _p_loader),
    # --- discovery: carry is_nullable into the mapping ---
    ("scripts/job1_discovery.py", patch_job1),
    # --- plumbing (original pipeline) ---
    ("lambdas/params_csv.py", patch_params_csv),
    ("lambdas/resolve_task.py", patch_resolve_task),
    ("lambdas/create_glue_jobs.py", patch_create_glue_jobs),
    ("stepfunctions/startup.asl.json", _p_asl_startup),
    ("config/params.example.csv", patch_params_example_csv),
    ("config/pipeline.example.json", patch_pipeline_example_json),
    # --- plumbing (manual_range_pipeline: sync copy + classification + wiring) ---
    ("manual_range_pipeline/lambdas/mrl_params_csv.py", patch_params_csv),
    ("manual_range_pipeline/lambdas/mrl_ensure_job.py", patch_mrl_ensure_job),
    ("manual_range_pipeline/lambdas/mrl_plan.py", patch_mrl_plan),
    ("manual_range_pipeline/lambdas/mrl_preflight.py", patch_mrl_preflight),
    ("manual_range_pipeline/stepfunctions/mrl-task.asl.json", _p_asl_mrl),
    ("manual_range_pipeline/tools/mrl_settings_classification.py", patch_mrl_classification),
    ("manual_range_pipeline/README.md", patch_mrl_readme),
    # --- docs (one short paragraph each; do not rewrite) ---
    ("README.md", patch_readme),
    ("RUNBOOK.md", patch_runbook),
]

# Files whose patched output must compile as Python (sanity check after patching).
_PY_SUFFIX = ".py"


def apply_all(read_fn, write_fn, log=lambda *a: None):
    """Apply every edit in FILES. read_fn(relpath)->text, write_fn(relpath, text).
    Returns a list of (relpath, old_sha, new_sha, changed:bool). Idempotent and fail-closed:
    an anchor problem raises AnchorError before any file in that call is written if the caller
    uses the two-phase helper below; this simple form writes as it goes."""
    results = []
    for rel, fn in FILES:
        old = read_fn(rel)
        new = fn(old)
        if rel.endswith(_PY_SUFFIX):
            compile(new, rel, "exec")
        changed = new != old
        if changed:
            write_fn(rel, new)
        results.append((rel, sha256(old), sha256(new), changed))
        log("%-62s %s  %s -> %s" % (rel, "CHANGED" if changed else "nochange",
                                    sha256(old)[:12], sha256(new)[:12]))
    return results


def transform_all(read_fn):
    """Phase 1 (no writes): compute {relpath: (old, new)} for every file, raising AnchorError /
    SyntaxError before anything is written. Lets a caller patch atomically (all-or-nothing)."""
    out = {}
    for rel, fn in FILES:
        old = read_fn(rel)
        new = fn(old)
        if rel.endswith(_PY_SUFFIX):
            compile(new, rel, "exec")
        out[rel] = (old, new)
    return out
