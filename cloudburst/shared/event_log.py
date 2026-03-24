import json
import os
import threading
import time


_init_lock = threading.Lock()
_write_lock = threading.Lock()
_initialized = False
_event_path = ''
_run_id = ''
_events_enabled = False
_allowed_events = set()

_RESERVED_FIELDS = {
    'schema_version',
    'run_id',
    'system',
    'ts_ms',
    'event_type',
    'ok',
}

_DEFAULT_ALLOWED_EVENTS = {
    'invoke_end',
    'state_read',
    'state_write',
    'crdt_divergence_detected',
    'crdt_merge_applied',
    'crdt_converged',
    'worker_start',
    'worker_ready',
    'scale_up_start',
    'scale_up_end',
    'scale_down_start',
    'scale_down_end',
    'placement_change',
}


def _first_non_empty(*values):
    for value in values:
        if value is None:
            continue
        stripped = value.strip()
        if stripped:
            return stripped
    return ''


def _parse_allowed_events(raw):
    if not raw:
        return set(_DEFAULT_ALLOWED_EVENTS)
    parsed = set()
    for token in raw.split(','):
        token = token.strip()
        if token:
            parsed.add(token)
    if not parsed:
        return set(_DEFAULT_ALLOWED_EVENTS)
    return parsed


def _init():
    global _initialized
    global _event_path
    global _run_id
    global _events_enabled
    global _allowed_events
    with _init_lock:
        if _initialized:
            return
        _event_path = _first_non_empty(
            os.getenv('CB_EVENTS_JSONL_PATH', ''),
            os.getenv('CB_EVENTS_JSONL', ''),
            os.getenv('CLOUDBURST_EVENTS_JSONL', '')
        )
        _run_id = _first_non_empty(
            os.getenv('CB_RUN_ID', ''),
            os.getenv('CLOUDBURST_RUN_ID', ''),
            os.getenv('RUN_ID', ''),
        )
        _allowed_events = _parse_allowed_events(
            _first_non_empty(
                os.getenv('CB_EVENT_TYPES', ''),
                os.getenv('CLOUDBURST_EVENT_TYPES', ''),
            )
        )
        _events_enabled = bool(_event_path and _run_id and _allowed_events)
        _initialized = True


def _enabled():
    if not _initialized:
        _init()
    return _events_enabled


def emit_event(event_type, ok=True, **fields):
    if not _enabled():
        return
    event_type = (event_type or '').strip()
    if not event_type:
        return
    if event_type not in _allowed_events:
        return

    event = {
        'schema_version': '1.0',
        'run_id': _run_id,
        'system': 'cloudburst',
        'ts_ms': int(time.time() * 1000),
        'event_type': event_type,
        'ok': bool(ok),
    }
    for key, value in fields.items():
        if key in _RESERVED_FIELDS:
            continue
        event[key] = value
    if 'attributes' not in event or not isinstance(event['attributes'], dict):
        event['attributes'] = {}

    line = (json.dumps(event, separators=(',', ':'), sort_keys=False) + '\n').encode('utf-8')
    with _write_lock:
        try:
            fd = os.open(_event_path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
        except OSError:
            return
        try:
            offset = 0
            while offset < len(line):
                written = os.write(fd, line[offset:])
                if written <= 0:
                    break
                offset += written
        except OSError:
            return
        finally:
            os.close(fd)
