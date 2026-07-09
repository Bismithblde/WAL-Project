#!/usr/bin/env python3
"""
End-to-end claim verification for the TCP key-value database.

The suite builds and starts the real C++ server, talks to it through TCP, and
validates the numeric claims that are easy to exaggerate accidentally:

* 500 concurrent client connections
* fixed-size worker behavior through bounded process thread count
* WAL recovery after a crash-like process termination
* compaction reducing WAL size by at least 40 percent
* 120,000+ operations/sec with p99 latency below 2.5 ms
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path


HOST = "127.0.0.1"
PORT = 6379


@dataclass
class BenchmarkResult:
    total_ops: int
    elapsed_seconds: float
    ops_per_second: float
    p99_ms: float


class ServerProcess:
    def __init__(self, exe_path: Path, workdir: Path) -> None:
        self.exe_path = exe_path
        self.workdir = workdir
        self.process: subprocess.Popen[str] | None = None

    @property
    def pid(self) -> int:
        if self.process is None:
            raise RuntimeError("server is not running")
        return self.process.pid

    def start(self) -> None:
        if self.process is not None:
            raise RuntimeError("server is already running")
        self.process = subprocess.Popen(
            [str(self.exe_path)],
            cwd=self.workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        wait_for_port()

    def stop(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.process = None
        wait_for_port_closed()

    def assert_running(self) -> None:
        if self.process is None:
            raise AssertionError("server process was not started")
        exit_code = self.process.poll()
        if exit_code is not None:
            output = ""
            if self.process.stdout is not None:
                try:
                    output = self.process.stdout.read()
                except Exception:
                    output = ""
            raise AssertionError(f"server exited with {exit_code}\n{output}")

    def __enter__(self) -> "ServerProcess":
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()


def build_server(repo_root: Path) -> Path:
    exe_path = repo_root / "main.exe"
    compile_cmd = ["g++", "main.cpp", "-O2", "-DNDEBUG", "-o", str(exe_path), "-lws2_32"]
    subprocess.run(compile_cmd, cwd=repo_root, check=True)
    return exe_path


def wait_for_port(timeout_seconds: float = 10.0) -> None:
    deadline = time.perf_counter() + timeout_seconds
    last_error: OSError | None = None
    while time.perf_counter() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=0.25):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
    raise TimeoutError(f"server did not open {HOST}:{PORT}: {last_error}")


def wait_for_port_closed(timeout_seconds: float = 10.0) -> None:
    deadline = time.perf_counter() + timeout_seconds
    while time.perf_counter() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=0.25):
                time.sleep(0.05)
        except OSError:
            return
    raise TimeoutError(f"server did not release {HOST}:{PORT}")


def request(command: str, timeout: float = 2.0) -> str:
    with socket.create_connection((HOST, PORT), timeout=timeout) as sock:
        return send_command(sock, command, timeout)


def send_command(sock: socket.socket, command: str, timeout: float = 2.0) -> str:
    sock.sendall((command + "\n").encode("utf-8"))
    return recv_some(sock, timeout)


def recv_some(sock: socket.socket, timeout: float = 2.0) -> str:
    sock.settimeout(timeout)
    data = bytearray()
    while b"\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data.extend(chunk)
    return data.decode("utf-8", errors="replace")


def open_idle_connection() -> socket.socket:
    deadline = time.perf_counter() + 10.0
    last_error: OSError | None = None
    while time.perf_counter() < deadline:
        try:
            sock = socket.create_connection((HOST, PORT), timeout=5.0)
            sock.settimeout(5.0)
            return sock
        except OSError as exc:
            last_error = exc
            time.sleep(0.025)
    raise TimeoutError(f"could not establish client connection: {last_error}")


def get_windows_thread_count(pid: int) -> int:
    if os.name != "nt":
        status_path = Path(f"/proc/{pid}/status")
        for line in status_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("Threads:"):
                return int(line.split(":", 1)[1].strip())
        raise RuntimeError("could not read Linux thread count")

    command = f"(Get-Process -Id {pid}).Threads.Count"
    output = subprocess.check_output(
        ["powershell", "-NoProfile", "-Command", command],
        text=True,
    )
    return int(output.strip())


def test_concurrent_connections(server: ServerProcess, clients: int) -> None:
    sockets: list[socket.socket] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(clients, 128)) as pool:
            for sock in pool.map(lambda _: open_idle_connection(), range(clients)):
                sockets.append(sock)

        server.assert_running()
        if len(sockets) != clients:
            raise AssertionError(f"accepted {len(sockets)} clients, expected {clients}")

        for index, sock in enumerate(sockets[: min(32, clients)]):
            sock.sendall(f"SET conn_{index} ok\n".encode("utf-8"))
            response = recv_some(sock)
            if response != "OK\n":
                raise AssertionError(f"client {index} SET failed: {response!r}")
    finally:
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass


def test_thread_pool_bound(server: ServerProcess, clients: int, max_threads: int) -> None:
    sockets: list[socket.socket] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(clients, 128)) as pool:
            for sock in pool.map(lambda _: open_idle_connection(), range(clients)):
                sockets.append(sock)
        time.sleep(0.5)
        thread_count = get_windows_thread_count(server.pid)
        if thread_count > max_threads:
            raise AssertionError(
                f"process used {thread_count} threads with {clients} clients; "
                f"expected <= {max_threads} for fixed-size thread-pool behavior"
            )
    finally:
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass


def test_wal_recovery(server: ServerProcess, workdir: Path, exe_path: Path) -> ServerProcess:
    with socket.create_connection((HOST, PORT), timeout=5.0) as sock:
        for index in range(100):
            response = send_command(sock, f"SET wal_{index} value_{index}")
            if response != "OK\n":
                raise AssertionError(f"WAL SET {index} failed: {response!r}")

    start = time.perf_counter()
    response = request("SET wal_guard survived")
    acknowledged_ms = (time.perf_counter() - start) * 1000.0
    if response != "OK\n":
        raise AssertionError(f"WAL guard SET failed: {response!r}")
    if acknowledged_ms > 100.0:
        raise AssertionError(f"acknowledged write took {acknowledged_ms:.2f} ms, expected <= 100 ms")

    server.stop()
    restarted = ServerProcess(exe_path, workdir)
    restarted.start()

    response = request("GET wal_guard")
    if response != "survived\n":
        raise AssertionError(f"WAL recovery lost acknowledged write: {response!r}")

    response = request("GET wal_42")
    if response != "value_42\n":
        raise AssertionError(f"WAL recovery lost earlier write: {response!r}")

    return restarted


def test_compaction_reduction(workdir: Path, reduction_percent: float) -> None:
    wal_path = workdir / "wal.txt"
    with socket.create_connection((HOST, PORT), timeout=5.0) as sock:
        for index in range(2000):
            response = send_command(sock, f"SET compact_key value_{index}")
            if response != "OK\n":
                raise AssertionError(f"compaction setup SET {index} failed: {response!r}")

    before = wal_path.stat().st_size
    response = request("COMPACT")
    if response != "OK\n":
        raise AssertionError(f"COMPACT failed: {response!r}")
    after = wal_path.stat().st_size
    response = request("GET compact_key")
    if response != "value_1999\n":
        raise AssertionError(f"compaction changed latest value: {response!r}")

    reduction = ((before - after) / before) * 100.0 if before else 0.0
    if reduction < reduction_percent:
        raise AssertionError(
            f"compaction reduced WAL by {reduction:.2f} percent; "
            f"expected >= {reduction_percent:.2f} percent"
        )


def benchmark(total_ops: int, workers: int) -> BenchmarkResult:
    setup_response = request("SET benchmark_key value")
    if setup_response != "OK\n":
        raise AssertionError(f"benchmark setup failed: {setup_response!r}")

    latencies_ns: list[int] = []
    latency_lock = threading.Lock()
    per_worker = total_ops // workers
    remainder = total_ops % workers

    def run_worker(worker_index: int) -> int:
        count = per_worker + (1 if worker_index < remainder else 0)
        local_latencies: list[int] = []
        completed = 0
        with socket.create_connection((HOST, PORT), timeout=5.0) as sock:
            sock.settimeout(5.0)
            for _ in range(count):
                start = time.perf_counter_ns()
                sock.sendall(b"GET benchmark_key\n")
                response = recv_some(sock)
                elapsed = time.perf_counter_ns() - start
                if response != "value\n":
                    raise AssertionError(f"unexpected benchmark response: {response!r}")
                local_latencies.append(elapsed)
                completed += 1
        with latency_lock:
            latencies_ns.extend(local_latencies)
        return completed

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        completed = sum(pool.map(run_worker, range(workers)))
    elapsed = time.perf_counter() - started

    if completed != total_ops:
        raise AssertionError(f"completed {completed} ops, expected {total_ops}")

    sorted_latencies = sorted(latencies_ns)
    p99_index = max(0, min(len(sorted_latencies) - 1, int(len(sorted_latencies) * 0.99) - 1))
    p99_ms = sorted_latencies[p99_index] / 1_000_000.0
    return BenchmarkResult(
        total_ops=completed,
        elapsed_seconds=elapsed,
        ops_per_second=completed / elapsed,
        p99_ms=p99_ms,
    )


def test_benchmark(total_ops: int, workers: int, min_ops_per_sec: float, max_p99_ms: float) -> BenchmarkResult:
    result = benchmark(total_ops, workers)
    if result.ops_per_second < min_ops_per_sec:
        raise AssertionError(
            f"throughput was {result.ops_per_second:,.0f} ops/sec; "
            f"expected >= {min_ops_per_sec:,.0f} ops/sec"
        )
    if result.p99_ms > max_p99_ms:
        raise AssertionError(
            f"p99 latency was {result.p99_ms:.3f} ms; expected <= {max_p99_ms:.3f} ms"
        )
    return result


def run_test(name: str, fn) -> None:
    print(f"[ RUN      ] {name}", flush=True)
    start = time.perf_counter()
    fn()
    elapsed = time.perf_counter() - start
    print(f"[       OK ] {name} ({elapsed:.2f}s)", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify key-value database resume claims end to end.")
    parser.add_argument("--clients", type=int, default=500)
    parser.add_argument("--max-server-threads", type=int, default=128)
    parser.add_argument("--min-compaction-reduction", type=float, default=40.0)
    parser.add_argument("--benchmark-ops", type=int, default=120_000)
    parser.add_argument("--benchmark-workers", type=int, default=64)
    parser.add_argument("--min-ops-per-sec", type=float, default=120_000.0)
    parser.add_argument("--max-p99-ms", type=float, default=2.5)
    parser.add_argument("--skip-benchmark", action="store_true")
    parser.add_argument("--keep-workdir", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    exe_path = build_server(repo_root)

    temp_context = tempfile.TemporaryDirectory(prefix="kvdb-e2e-")
    workdir = Path(temp_context.name)
    copied_exe = workdir / exe_path.name
    shutil.copy2(exe_path, copied_exe)

    print(f"server workdir: {workdir}")

    def start_server() -> ServerProcess:
        started_server = ServerProcess(copied_exe, workdir)
        started_server.start()
        return started_server

    def with_fresh_server(fn) -> None:
        fresh_server = start_server()
        try:
            fn(fresh_server)
        finally:
            fresh_server.stop()

    server: ServerProcess | None = None
    try:
        run_test(
            f"accepts {args.clients}+ concurrent TCP clients",
            lambda: with_fresh_server(lambda fresh: test_concurrent_connections(fresh, args.clients)),
        )
        run_test(
            f"uses <= {args.max_server_threads} server threads with {args.clients} clients",
            lambda: with_fresh_server(lambda fresh: test_thread_pool_bound(fresh, args.clients, args.max_server_threads)),
        )
        print("[ RUN      ] recovers acknowledged WAL writes after restart", flush=True)
        wal_start = time.perf_counter()
        server = start_server()
        server = test_wal_recovery(server, workdir, copied_exe)
        server.stop()
        server = None
        print(
            f"[       OK ] recovers acknowledged WAL writes after restart ({time.perf_counter() - wal_start:.2f}s)",
            flush=True,
        )
        run_test(
            "compacts obsolete WAL entries by target percentage",
            lambda: with_fresh_server(lambda _: test_compaction_reduction(workdir, args.min_compaction_reduction)),
        )
        if not args.skip_benchmark:
            result_holder: dict[str, BenchmarkResult] = {}

            def run_bench() -> None:
                with_fresh_server(
                    lambda _: result_holder.update(
                        result=test_benchmark(
                            args.benchmark_ops,
                            args.benchmark_workers,
                            args.min_ops_per_sec,
                            args.max_p99_ms,
                        )
                    )
                )

            run_test("sustains target throughput and p99 latency", run_bench)
            result = result_holder["result"]
            print(
                "benchmark: "
                f"{result.total_ops:,} ops in {result.elapsed_seconds:.3f}s, "
                f"{result.ops_per_second:,.0f} ops/sec, p99 {result.p99_ms:.3f} ms",
                flush=True,
            )
        return 0
    except Exception as exc:
        print(f"[  FAILED  ] {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        if server is not None:
            server.stop()
        if args.keep_workdir:
            print(f"kept workdir: {workdir}")
        else:
            temp_context.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
