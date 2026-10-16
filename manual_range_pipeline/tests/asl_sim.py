#!/usr/bin/env python3
"""asl_sim.py - a faithful, OFFLINE Amazon States Language interpreter for testing the MRL
state machines (and for validating itself by replaying the existing startup.asl.json happy
path). No AWS, no network.

Supported (exactly what the MRL machines + the startup happy-path replay use):
  States: Pass, Task, Choice, Wait (skipped - 0s), Map (inline Iterator, ItemsPath,
          Parameters/ItemSelector, MaxConcurrency[Path] - run sequentially), Succeed, Fail.
  Task fields: Resource (routed to a pluggable handler by resource string), Parameters,
          ResultSelector, ResultPath, Retry, Catch, (End/Next).
  Choice: And/Or/Not, BooleanEquals, IsNull, IsPresent, IsString, NumericEquals,
          NumericGreaterThan, NumericLessThan, StringEquals, StringMatches (glob with '*').
  Intrinsics: States.Format('{} ...', a, b), States.MathAdd(a, b), States.ArrayContains(arr,v).
  JSONPath: $, $.a.b, $.a[0].b (numeric index), $.a[*].b (projection -> list), $$ context,
          and literal values (non-'$' Parameters values pass through).
  Error names in Retry/Catch: exact match, or "States.ALL" wildcard. Retries are honoured
          (MaxAttempts) with zero real wait.

A Task handler is `fn(resource, parameters, context) -> result` (the post-Parameters input).
It may raise ASLError(error_name, cause) to simulate a task failure that Retry/Catch see.
"""

import json
import re


class ASLError(Exception):
    def __init__(self, error, cause=""):
        super().__init__("%s: %s" % (error, cause))
        self.error = error
        self.cause = cause


class ASLRuntimeError(Exception):
    """A States.Runtime-equivalent: a path read that does not resolve (the footgun the audit
    guards against). Surfaced so interpreter tests can assert a machine never hits it."""


# --------------------------------------------------------------------------- JSONPath
_TOKEN_RE = re.compile(r"""
    \.([A-Za-z0-9_\-]+)       |   # .key
    \['([^']*)'\]             |   # ['key']
    \[(\d+)\]                 |   # [index]
    (\[\*\])                      # [*] projection
""", re.VERBOSE)


def _tokens(path):
    body = path
    out = []
    i = 0
    # normalize a leading $.key / $['key'] uniformly
    while i < len(body):
        m = _TOKEN_RE.match(body, i)
        if not m:
            break
        if m.group(1) is not None:
            out.append(("key", m.group(1)))
        elif m.group(2) is not None:
            out.append(("key", m.group(2)))
        elif m.group(3) is not None:
            out.append(("index", int(m.group(3))))
        elif m.group(4) is not None:
            out.append(("star", None))
        i = m.end()
    if i != len(body):
        raise ASLRuntimeError("unparseable JSONPath tail %r in %r" % (body[i:], path))
    return out


def resolve_path(path, data, context):
    """Resolve a $ / $$ JSONPath against data (or the context object for $$). Returns the value.
    Raises ASLRuntimeError if any segment is missing (States.Runtime equivalent)."""
    if path == "$":
        return data
    if path == "$$":
        return context
    if path.startswith("$$"):
        root = context
        rest = path[2:]
    elif path.startswith("$"):
        root = data
        rest = path[1:]
    else:
        raise ASLRuntimeError("not a path: %r" % path)
    cur = root
    for kind, val in _tokens(rest):
        if kind == "key":
            if not isinstance(cur, dict) or val not in cur:
                raise ASLRuntimeError("path %r: missing key %r" % (path, val))
            cur = cur[val]
        elif kind == "index":
            if not isinstance(cur, list) or val >= len(cur):
                raise ASLRuntimeError("path %r: bad index %d" % (path, val))
            cur = cur[val]
        elif kind == "star":
            if not isinstance(cur, list):
                raise ASLRuntimeError("path %r: [*] on non-list" % path)
            # projection: the remaining tokens apply to each element
            return cur  # callers that use [*] always finish with a trailing key handled below
    return cur


def _resolve_projection(path, data, context):
    """Handle `$.a[*].b` -> [elem['b'] for elem in $.a]. Splits at the first [*]."""
    idx = path.find("[*]")
    if idx < 0:
        return resolve_path(path, data, context)
    head = path[:idx]
    tail = path[idx + 3:]            # e.g. ".status" or ""
    arr = resolve_path(head, data, context)
    if not isinstance(arr, list):
        raise ASLRuntimeError("[*] on non-list at %r" % head)
    out = []
    for el in arr:
        if tail == "":
            out.append(el)
        else:
            out.append(resolve_path("$" + tail, el, context))
    return out


def _is_path(v):
    return isinstance(v, str) and (v.startswith("$.") or v == "$" or v.startswith("$$")
                                   or v.startswith("$["))


# --------------------------------------------------------------------------- intrinsics
_INTRINSIC_RE = re.compile(r"^States\.(\w+)\((.*)\)$", re.DOTALL)


def _split_args(s):
    """Split intrinsic args on top-level commas (handles quoted strings)."""
    args, depth, cur, q = [], 0, "", None
    i = 0
    while i < len(s):
        ch = s[i]
        if q:
            cur += ch
            if ch == q:
                q = None
        elif ch in ("'", '"'):
            q = ch
            cur += ch
        elif ch in "([":
            depth += 1
            cur += ch
        elif ch in ")]":
            depth -= 1
            cur += ch
        elif ch == "," and depth == 0:
            args.append(cur.strip())
            cur = ""
        else:
            cur += ch
        i += 1
    if cur.strip():
        args.append(cur.strip())
    return args


def _eval_arg(a, data, context):
    a = a.strip()
    if a.startswith("'") and a.endswith("'"):
        return a[1:-1]
    if _is_path(a):
        if "[*]" in a:
            return _resolve_projection(a, data, context)
        return resolve_path(a, data, context)
    if a.startswith("States."):
        return eval_intrinsic(a, data, context)
    # numeric literal
    try:
        return int(a)
    except ValueError:
        try:
            return float(a)
        except ValueError:
            return a


def eval_intrinsic(expr, data, context):
    m = _INTRINSIC_RE.match(expr.strip())
    if not m:
        raise ASLRuntimeError("unsupported intrinsic %r" % expr)
    name, argstr = m.group(1), m.group(2)
    args = [_eval_arg(a, data, context) for a in _split_args(argstr)] if argstr.strip() else []
    if name == "Format":
        tmpl = args[0]
        rest = args[1:]
        out, ri = [], 0
        i = 0
        while i < len(tmpl):
            if tmpl[i:i + 2] == "{}":
                v = rest[ri]
                ri += 1
                out.append(v if isinstance(v, str) else json.dumps(v)
                           if isinstance(v, (dict, list)) else str(v))
                i += 2
            else:
                out.append(tmpl[i])
                i += 1
        return "".join(out)
    if name == "MathAdd":
        return int(args[0]) + int(args[1])
    if name == "ArrayContains":
        return args[1] in (args[0] or [])
    if name == "JsonToString":
        return json.dumps(args[0])
    if name == "StringToJson":
        return json.loads(args[0])
    if name == "Array":
        return list(args)
    raise ASLRuntimeError("unsupported intrinsic States.%s" % name)


# --------------------------------------------------------------------------- Parameters
def build_payload(spec, data, context):
    """Build a Parameters/ItemSelector/ResultSelector object: keys ending in '.$' resolve a
    path/intrinsic; other keys pass through (recursively for dict/list)."""
    if isinstance(spec, dict):
        out = {}
        for k, v in spec.items():
            if k.endswith(".$"):
                kk = k[:-2]
                if isinstance(v, str) and v.startswith("States."):
                    out[kk] = eval_intrinsic(v, data, context)
                elif isinstance(v, str) and "[*]" in v:
                    out[kk] = _resolve_projection(v, data, context)
                elif _is_path(v):
                    out[kk] = resolve_path(v, data, context)
                else:
                    out[kk] = v
            else:
                out[k] = build_payload(v, data, context)
        return out
    if isinstance(spec, list):
        return [build_payload(x, data, context) for x in spec]
    return spec


def _set_path(data, result_path, value):
    """Apply ResultPath. '$' replaces; None discards; '$.a.b' sets nested (creating dicts)."""
    if result_path is None:
        return data
    if result_path == "$":
        return value
    assert result_path.startswith("$.")
    parts = result_path[2:].split(".")
    cur = data
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value
    return data


# --------------------------------------------------------------------------- Choice
def _cmp_value(cond, data, context):
    var = cond["Variable"]
    try:
        actual = resolve_path(var, data, context)
        present = True
    except ASLRuntimeError:
        actual = None
        present = False
    return actual, present


def eval_choice_rule(rule, data, context):
    if "And" in rule:
        return all(eval_choice_rule(r, data, context) for r in rule["And"])
    if "Or" in rule:
        return any(eval_choice_rule(r, data, context) for r in rule["Or"])
    if "Not" in rule:
        return not eval_choice_rule(rule["Not"], data, context)
    actual, present = _cmp_value(rule, data, context)
    if "IsPresent" in rule:
        return present == bool(rule["IsPresent"])
    if "IsNull" in rule:
        return (present and actual is None) == bool(rule["IsNull"])
    if not present:
        return False
    if "IsString" in rule:
        return isinstance(actual, str) == bool(rule["IsString"])
    if "StringEquals" in rule:
        return actual == rule["StringEquals"]
    if "StringMatches" in rule:
        pat = rule["StringMatches"]
        regex = "^" + ".*".join(re.escape(p) for p in pat.split("*")) + "$"
        return isinstance(actual, str) and re.match(regex, actual) is not None
    if "BooleanEquals" in rule:
        return actual == rule["BooleanEquals"]
    if "NumericEquals" in rule:
        return actual == rule["NumericEquals"]
    if "NumericGreaterThan" in rule:
        return actual is not None and actual > rule["NumericGreaterThan"]
    if "NumericLessThan" in rule:
        return actual is not None and actual < rule["NumericLessThan"]
    raise ASLRuntimeError("unsupported Choice operator in %r" % rule)


# --------------------------------------------------------------------------- Interpreter
class Interpreter:
    def __init__(self, definition, handlers, context=None, max_steps=100000):
        self.sm = definition
        self.states = definition["States"]
        self.handlers = handlers          # {resource_string: fn(resource, params, context)}
        self.context = context or {"Execution": {"Id":
                                   "arn:aws:states:::execution:sm:exec1", "Name": "exec1"},
                                   "StateMachine": {"Id": "arn:aws:states:::stateMachine:sm"},
                                   "Map": {"Item": {}}}
        self.max_steps = max_steps
        self.trace = []

    def run(self, input_data):
        return self._run_scope(self.states, self.sm["StartAt"], input_data)

    def _run_scope(self, states, start, data):
        name = start
        steps = 0
        while True:
            steps += 1
            if steps > self.max_steps:
                raise ASLRuntimeError("step budget exceeded (possible infinite loop) at %s" % name)
            st = states[name]
            t = st["Type"]
            self.trace.append(name)
            if t == "Succeed":
                return {"status": "SUCCEEDED", "state": name, "output": data}
            if t == "Fail":
                return {"status": "FAILED", "state": name, "error": st.get("Error"),
                        "output": data}
            if t == "Pass":
                data = self._do_pass(st, data)
                if st.get("End"):
                    return {"status": "SUCCEEDED", "state": name, "output": data}
                name = st["Next"]
                continue
            if t == "Wait":
                if st.get("End"):
                    return {"status": "SUCCEEDED", "state": name, "output": data}
                name = st["Next"]
                continue
            if t == "Choice":
                nxt = None
                for rule in st.get("Choices", []):
                    if eval_choice_rule(rule, data, self.context):
                        nxt = rule["Next"]
                        break
                if nxt is None:
                    nxt = st.get("Default")
                if nxt is None:
                    raise ASLRuntimeError("Choice %s: no match and no Default" % name)
                name = nxt
                continue
            if t in ("Task", "Map"):
                try:
                    data = self._do_task_or_map(st, name, data)
                except ASLError as e:
                    caught = self._handle_catch(st, data, e)
                    if caught is None:
                        return {"status": "FAILED", "state": name, "error": e.error,
                                "output": data}
                    nxt, data = caught
                    name = nxt
                    continue
                if st.get("End"):
                    return {"status": "SUCCEEDED", "state": name, "output": data}
                name = st["Next"]
                continue
            raise ASLRuntimeError("unsupported state Type %r in %s" % (t, name))

    def _do_pass(self, st, data):
        # InputPath selects the effective input for Result/Parameters/ResultPath; ResultPath
        # merges back into the ORIGINAL data; OutputPath (if any) filters the final output.
        eff = data
        if "InputPath" in st and st["InputPath"] is not None:
            eff = resolve_path(st["InputPath"], data, self.context)
        if "Parameters" in st:
            result = build_payload(st["Parameters"], eff, self.context)
        elif "Result" in st:
            result = st["Result"]
        else:
            result = eff
        data = _set_path(data, st.get("ResultPath", "$"), result)
        if "OutputPath" in st and st["OutputPath"] is not None:
            data = resolve_path(st["OutputPath"], data, self.context)
        return data

    def _run_with_retry(self, st, fn):
        retries = st.get("Retry", [])
        attempt_counts = [0] * len(retries)
        while True:
            try:
                return fn()
            except ASLError as e:
                matched = None
                for i, r in enumerate(retries):
                    errs = r.get("ErrorEquals", [])
                    if e.error in errs or "States.ALL" in errs:
                        if attempt_counts[i] < r.get("MaxAttempts", 3):
                            attempt_counts[i] += 1
                            matched = i
                            break
                if matched is None:
                    raise
                # zero real wait in the simulator
                continue

    def _do_task_or_map(self, st, name, data):
        if st["Type"] == "Map":
            # Map's Parameters IS the per-item ItemSelector (evaluated inside _run_map with the
            # Map.Item context); it is NOT a state-level Parameters, so do not evaluate it here.
            result = self._run_map(st, data)
        else:
            params = build_payload(st["Parameters"], data, self.context) \
                if "Parameters" in st else data
            resource = st["Resource"]
            handler = self.handlers.get(resource)
            if handler is None:
                raise ASLRuntimeError("no fake handler for Resource %r (state %s)"
                                      % (resource, name))
            result = self._run_with_retry(st, lambda: handler(resource, params, self.context))
        if "ResultSelector" in st:
            result = build_payload(st["ResultSelector"], result, self.context)
        return _set_path(data, st.get("ResultPath", "$"), result)

    def _run_map(self, st, data):
        items_path = st.get("ItemsPath", "$")
        items = resolve_path(items_path, data, self.context) if items_path != "$" else data
        if not isinstance(items, list):
            raise ASLRuntimeError("Map ItemsPath %r is not a list" % items_path)
        it = st.get("Iterator") or st.get("ItemProcessor")
        results = []
        for idx, item in enumerate(items):
            ctx = dict(self.context)
            ctx = {**self.context, "Map": {"Item": {"Value": item, "Index": idx}}}
            sel = st.get("Parameters") or st.get("ItemSelector")
            item_input = build_payload(sel, data, ctx) if sel else item
            sub = Interpreter(it if "StartAt" in it else {"StartAt": it["StartAt"],
                              "States": it["States"]}, self.handlers, ctx, self.max_steps)
            sub.sm = {"StartAt": it["StartAt"], "States": it["States"]}
            sub.states = it["States"]
            r = sub._run_scope(it["States"], it["StartAt"], item_input)
            self.trace.extend("  " + t for t in sub.trace)
            if r["status"] == "FAILED":
                # A Map iteration that reaches a Fail state raises States.ALL to the Map (so a
                # Catch on the Map can handle it), matching real ASL.
                raise ASLError(r.get("error") or "States.TaskFailed",
                               "map iteration %d failed at %s" % (idx, r["state"]))
            results.append(r["output"])
        return results

    def _handle_catch(self, st, data, err):
        for c in st.get("Catch", []):
            errs = c.get("ErrorEquals", [])
            if err.error in errs or "States.ALL" in errs:
                rp = c.get("ResultPath", "$")
                data = _set_path(data, rp, {"Error": err.error, "Cause": err.cause})
                return c["Next"], data
        return None
