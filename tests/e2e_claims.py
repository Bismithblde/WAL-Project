#!/usr/bin/env python3
"""
End-to-end claim verification for the TCP key-value database.

The suite builds and starts the real C++ server, talks to it through TCP, and
validates the numeric claims that are easy to exaggerate accidentally:

* 500 concurrent client connections
* fixed-size worker behavior through bounded process thread count
* WAL recovery after a crash-like process termination
* compaction reducing WAL size by at least 40 percent
* line and RESP2 pipelining behavior
* throughput measurements at fixed client counts
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import random
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
    clients: int
    requested_ops: int
    total_ops: int
    writes: int
    elapsed_seconds: float
    ops_per_second: float
    errors: int


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
    compile_cmd = ["g++", "src/main.cpp", "src/ThreadSafeQueue.cpp", "-O2", "-DNDEBUG", "-Iinclude", "-Ivcpkg_installed/x64-mingw-dynamic/include", "-o", str(exe_path), "-lws2_32"]
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


def recv_exact(sock: socket.socket, size: int, timeout: float = 5.0) -> bytes:
    sock.settimeout(timeout)
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise AssertionError(f"connection closed after {len(data)} of {size} expected bytes")
        data.extend(chunk)
    return bytes(data)


def resp_array(*parts: str) -> bytes:
    encoded = f"*{len(parts)}\r\n".encode("utf-8")
    for part in parts:
        value = part.encode("utf-8")
        encoded += f"${len(value)}\r\n".encode("utf-8") + value + b"\r\n"
    return encoded


class RespReader:
    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.buffer = bytearray()

    def read_exact(self, size: int) -> bytes:
        while len(self.buffer) < size:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise AssertionError("connection closed while reading RESP response")
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:size])
        del self.buffer[:size]
        return result

    def read_line(self) -> bytes:
        while True:
            newline = self.buffer.find(b"\r\n")
            if newline != -1:
                result = bytes(self.buffer[:newline])
                del self.buffer[: newline + 2]
                return result
            chunk = self.sock.recv(4096)
            if not chunk:
                raise AssertionError("connection closed while reading RESP line")
            self.buffer.extend(chunk)

    def read_response(self) -> tuple[bytes, bytes]:
        prefix = self.read_exact(1)
        if prefix in (b"+", b"-", b":"):
            return prefix, self.read_line()
        if prefix == b"$":
            length = int(self.read_line())
            if length == -1:
                return prefix, b""
            value = self.read_exact(length)
            if self.read_exact(2) != b"\r\n":
                raise AssertionError("malformed RESP bulk string terminator")
            return prefix, value
        raise AssertionError(f"unexpected RESP response prefix: {prefix!r}")


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


def test_line_batching_and_buffered_output(server: ServerProcess) -> None:
    with socket.create_connection((HOST, PORT), timeout=5.0) as sock:
        request_count = 256
        sock.sendall(b"SET batch_key value\n" + b"GET batch_key\n" * request_count)
        expected = b"OK\n" + b"value\n" * request_count
        response = recv_exact(sock, len(expected))
        if response != expected:
            raise AssertionError(f"line batching returned unexpected response: {response[:128]!r}")


def test_resp2_pipelining_and_fragmentation(server: ServerProcess) -> None:
    with socket.create_connection((HOST, PORT), timeout=5.0) as sock:
        request = resp_array("SET", "resp_key", "value") + resp_array("GET", "resp_key")
        midpoint = len(request) // 2
        sock.sendall(request[:midpoint])
        time.sleep(0.05)
        sock.sendall(request[midpoint:])

        expected = b"+OK\r\n$5\r\nvalue\r\n"
        response = recv_exact(sock, len(expected))
        if response != expected:
            raise AssertionError(f"RESP2 pipeline returned {response!r}")


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

    response = request("SET wal_deleted temporary")
    if response != "OK\n":
        raise AssertionError(f"WAL delete setup failed: {response!r}")
    response = request("DELETE wal_deleted")
    if response != "OK\n":
        raise AssertionError(f"WAL delete failed: {response!r}")
    restarted.stop()
    restarted = ServerProcess(exe_path, workdir)
    restarted.start()
    response = request("GET wal_deleted")
    if response != "(nil)\n":
        raise AssertionError(f"WAL recovery restored deleted key: {response!r}")

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


def test_compaction_with_writes() -> None:
    failures: list[str] = []

    def writer() -> None:
        try:
            with socket.create_connection((HOST, PORT), timeout=5.0) as sock:
                for index in range(200):
                    response = send_command(sock, f"SET live_key value_{index}")
                    if response != "OK\n":
                        failures.append(response)
                        return
        except OSError as exc:
            failures.append(str(exc))

    writing = threading.Thread(target=writer)
    writing.start()
    response = request("COMPACT", timeout=10.0)
    writing.join(timeout=10.0)
    if writing.is_alive() or failures:
        raise AssertionError(f"writes did not complete during compaction: {failures}")
    if response != "OK\n":
        raise AssertionError(f"COMPACT failed during writes: {response!r}")
    if request("GET live_key") == "(nil)\n":
        raise AssertionError("compaction lost live write")


def benchmark(total_ops: int, workers: int, pipeline_depth: int, keyspace: int, write_percent: int) -> BenchmarkResult:
    value = "benchmark-value-32-bytes-long!!!"
    keys = [f"benchmark_key_{index}" for index in range(keyspace)]

    with socket.create_connection((HOST, PORT), timeout=5.0) as setup_socket:
        setup_socket.settimeout(5.0)
        reader = RespReader(setup_socket)
        for start in range(0, keyspace, pipeline_depth):
            batch = keys[start : start + pipeline_depth]
            setup_socket.sendall(b"".join(resp_array("SET", key, value) for key in batch))
            for _ in batch:
                prefix, payload = reader.read_response()
                if prefix != b"+" or payload != b"OK":
                    raise AssertionError(f"benchmark setup failed: {prefix!r} {payload!r}")

    per_worker = total_ops // workers
    remainder = total_ops % workers

    def run_worker(worker_index: int) -> tuple[int, int]:
        count = per_worker + (1 if worker_index < remainder else 0)
        completed = 0
        writes = 0
        rng = random.Random(10_000 + worker_index)
        with socket.create_connection((HOST, PORT), timeout=5.0) as sock:
            sock.settimeout(5.0)
            reader = RespReader(sock)

            warmup = [resp_array("GET", keys[rng.randrange(keyspace)]) for _ in range(pipeline_depth)]
            sock.sendall(b"".join(warmup))
            for _ in warmup:
                prefix, payload = reader.read_response()
                if prefix != b"$" or payload != value.encode("utf-8"):
                    raise AssertionError(f"unexpected warmup response: {prefix!r} {payload!r}")

            while completed < count:
                batch_size = min(pipeline_depth, count - completed)
                expected: list[bool] = []
                requests: list[bytes] = []
                for _ in range(batch_size):
                    key = keys[rng.randrange(keyspace)]
                    is_write = rng.randrange(100) < write_percent
                    if is_write:
                        requests.append(resp_array("SET", key, value))
                        writes += 1
                    else:
                        requests.append(resp_array("GET", key))
                    expected.append(is_write)

                sock.sendall(b"".join(requests))
                for is_write in expected:
                    prefix, payload = reader.read_response()
                    if is_write:
                        if prefix != b"+" or payload != b"OK":
                            raise AssertionError(f"unexpected SET response: {prefix!r} {payload!r}")
                    elif prefix != b"$" or payload != value.encode("utf-8"):
                        raise AssertionError(f"unexpected GET response: {prefix!r} {payload!r}")
                    completed += 1
        return completed, writes

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        worker_results = list(pool.map(run_worker, range(workers)))
    elapsed = time.perf_counter() - started
    completed = sum(result[0] for result in worker_results)
    writes = sum(result[1] for result in worker_results)

    if completed != total_ops:
        raise AssertionError(f"completed {completed} ops, expected {total_ops}")

    return BenchmarkResult(
        clients=workers,
        requested_ops=total_ops,
        total_ops=completed,
        writes=writes,
        elapsed_seconds=elapsed,
        ops_per_second=completed / elapsed,
        errors=0,
    )


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
    parser.add_argument("--benchmark-clients", type=int, nargs="+", default=[64, 512, 2048])
    parser.add_argument("--benchmark-pipeline-depth", type=int, default=16)
    parser.add_argument("--benchmark-keyspace", type=int, default=1_000)
    parser.add_argument("--benchmark-write-percent", type=int, default=5)
    parser.add_argument("--benchmark-trials", type=int, default=3)
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
        run_test(
            "processes pipelined line commands with buffered output",
            lambda: with_fresh_server(test_line_batching_and_buffered_output),
        )
        run_test(
            "processes fragmented RESP2 pipelines",
            lambda: with_fresh_server(test_resp2_pipelining_and_fragmentation),
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
        run_test(
            "compacts while writes are active",
            lambda: with_fresh_server(lambda _: test_compaction_with_writes()),
        )
        if not args.skip_benchmark:
            for benchmark_clients in args.benchmark_clients:
                results: list[BenchmarkResult] = []
                for trial in range(args.benchmark_trials):
                    result_holder: dict[str, BenchmarkResult] = {}

                    def run_bench() -> None:
                        with_fresh_server(
                            lambda _: result_holder.update(
                                result=benchmark(
                                    args.benchmark_ops,
                                    benchmark_clients,
                                    args.benchmark_pipeline_depth,
                                    args.benchmark_keyspace,
                                    args.benchmark_write_percent,
                                )
                            )
                        )

                    run_test(
                        f"measures throughput with {benchmark_clients} clients (trial {trial + 1}/{args.benchmark_trials})",
                        run_bench,
                    )
                    results.append(result_holder["result"])

                throughputs = [result.ops_per_second for result in results]
                completed = sum(result.total_ops for result in results)
                writes = sum(result.writes for result in results)
                print(
                    "benchmark: "
                    f"protocol=RESP2, clients={benchmark_clients:,}, pipeline={args.benchmark_pipeline_depth}, "
                    f"keyspace={args.benchmark_keyspace:,}, read_write={100 - args.benchmark_write_percent}/{args.benchmark_write_percent}, "
                    f"trials={args.benchmark_trials}, completed={completed:,}, writes={writes:,}, errors=0, "
                    f"ops_sec_median={statistics.median(throughputs):,.0f}, "
                    f"ops_sec_min={min(throughputs):,.0f}, ops_sec_max={max(throughputs):,.0f}",
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
