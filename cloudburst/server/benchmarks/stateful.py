"""
Stateful benchmark for SeBS comparison.

Registers a single function that does a KVS put + get of configurable
size, then lightweight compute.  Returns per-invocation timing dicts
whose fields match the SeBS ExecutionResult contract.

Usage via run_benchmark.py:
    python3 cloudburst/client/run_benchmark.py stateful <elb> <num_requests> <ip>

Environment variables (optional):
    STATE_SIZE_KB   — size of KVS blob in KB  (default: 64)
    STATE_KEY       — KVS key name            (default: bench:state)
    OPS             — compute loop iterations  (default: 1)
"""

import os
import sys
import time
import uuid

from cloudburst.server.benchmarks import utils


def _make_function(state_key, state_size_kb, ops):
    """Build the Cloudburst function closure with baked-in parameters."""

    def stateful_bench(cloudburst, request_id):
        begin = time.time()
        size = max(1, state_size_kb)
        blob = os.urandom(size * 1024)

        t0 = time.time()
        cloudburst.put(state_key, blob)
        write_us = int((time.time() - t0) * 1_000_000)

        t1 = time.time()
        _ = cloudburst.get(state_key)
        read_us = int((time.time() - t1) * 1_000_000)

        t2 = time.time()
        acc = 0
        for idx in range(min(ops * 64, 20000)):
            acc = (acc + idx + size) % 1000003
        compute_us = int((time.time() - t2) * 1_000_000)

        end = time.time()
        return {
            "request_id": request_id,
            "is_cold": False,
            "begin": begin,
            "end": end,
            "measurement": {
                "compute_time_us": compute_us,
                "state_read_lat_us": read_us,
                "state_write_lat_us": write_us,
                "state_size_kb": size,
                "state_ops": ops,
                "accumulator": acc,
            },
        }

    return stateful_bench


def run(cloudburst_client, num_requests, sckt):
    """Entry point called by run_benchmark.py."""
    state_size_kb = int(os.environ.get("STATE_SIZE_KB", "64"))
    state_key = os.environ.get("STATE_KEY", "bench:state")
    ops = int(os.environ.get("OPS", "1"))

    bench_fn = _make_function(state_key, state_size_kb, ops)
    cloud_fn = cloudburst_client.register(bench_fn, "stateful_bench")
    if not cloud_fn:
        print("Failed to register stateful_bench function.")
        sys.exit(1)
    print("Registered stateful_bench (state_size_kb=%d, key=%s)" % (state_size_kb, state_key))

    # Warm-up: single invocation to prime executor cache / connections.
    warmup = cloud_fn(uuid.uuid4().hex).get()
    if not isinstance(warmup, dict) or "request_id" not in warmup:
        print("Warm-up invocation failed: %s" % str(warmup))
        sys.exit(1)
    print("Warm-up OK (request_id=%s)" % warmup["request_id"])

    total_time = []
    scheduler_time = []
    kvs_time = []
    retries = 0

    for _ in range(num_requests):
        rid = uuid.uuid4().hex

        start = time.time()
        future = cloud_fn(rid)
        sched_end = time.time()

        result = future.get()
        end = time.time()

        stime = sched_end - start
        ktime = end - sched_end

        total_time.append(stime + ktime)
        scheduler_time.append(stime)
        kvs_time.append(ktime)

    if sckt:
        import cloudpickle as cp
        sckt.send(cp.dumps(total_time))

    return total_time, scheduler_time, kvs_time, retries
