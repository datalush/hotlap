#!/usr/bin/env python3
"""Reproducible direct-binary filter/project timing, memory, and allocation smoke runner."""

import argparse
import hashlib
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEST = "core::tests::filter_project_bench::filter_project_measurement_sample"
MEMORY_TEST = "core::tests::filter_project_bench::filter_project_memory_sample"
ALLOC_TEST = "core::tests::filter_project_bench::filter_project_operator_allocation_sample"
OPERATOR_TEST = "core::tests::filter_project_bench::filter_project_operator_wall_sample"
CASES = (
    "tiny-1-alltrue", "narrow-mixed", "wide-utf8-discard-mixed",
    "wide-utf8-discard-alltrue", "wide-utf8-discard-allfalse",
    "repeated-projection", "wide-distinct-256", "group-float-sum-avg",
    "narrow-allfalse-control", "dictionary-discard-fallback",
)
FUSED_CASES = frozenset(CASES) - {"dictionary-discard-fallback"}
PREFIX = "filter_project_single_sample"
DIMENSIONS = {
    "tiny-1-alltrue": (1, 3, 0),
    "narrow-mixed": (2048, 3, 0),
    "wide-utf8-discard-mixed": (4096, 4, 16_777_216),
    "wide-utf8-discard-alltrue": (4096, 4, 16_777_216),
    "wide-utf8-discard-allfalse": (4096, 4, 16_777_216),
    "repeated-projection": (4096, 3, 0),
    "wide-distinct-256": (512, 258, 0),
    "group-float-sum-avg": (12, 4, 0),
    "narrow-allfalse-control": (2048, 3, 0),
    "dictionary-discard-fallback": (512, 4, 0),
}
MAIN_PHASES = ("excluded-warmup", "timing", "excluded-operator-warmup", "operator-wall", "allocation")


def planned_phases(smoke: bool) -> tuple[str, ...]:
    return (*MAIN_PHASES, "memory")


def child_argv(binary: str, test: str, cpu: int | None) -> list[str]:
    argv = [binary, "--ignored", "--exact", test, "--nocapture", "--test-threads=1"]
    if cpu is None:
        return argv
    taskset = shutil.which("taskset")
    if taskset is None:
        raise RuntimeError("--cpu requires taskset to pin only the measurement child")
    return [taskset, "-c", str(cpu), *argv]


def child_affinity_when_pinned(pid: int, cpu: int) -> list[int]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            affinity = sorted(os.sched_getaffinity(pid))
        except ProcessLookupError as error:
            raise RuntimeError("pinned child exited before its affinity could be verified") from error
        if affinity == [cpu]:
            return affinity
        time.sleep(0.001)
    raise RuntimeError(f"pinned child affinity did not become [{cpu}]; observed {affinity}")


def parse_measurement_output(output: str, expected_case: str, expected_mode: str) -> dict:
    found = []
    for line in output.splitlines():
        start = line.find("{")
        while start >= 0:
            decoder = json.JSONDecoder()
            try:
                item, end = decoder.raw_decode(line[start:])
            except json.JSONDecodeError:
                break
            if isinstance(item, dict) and item.get("record") == PREFIX:
                found.append(item)
            next_start = line.find("{", start + end)
            start = next_start
    if len(found) != 1:
        raise ValueError(f"expected exactly one {PREFIX} record; found {len(found)}")
    record = found[0]
    if record.get("case") != expected_case or record.get("mode") != expected_mode:
        raise ValueError("measurement case or mode does not match requested child")
    positive = ("elapsed_push_ns", "rows_seed", "source_input_rows_seed", "rows_change", "source_columns")
    nonnegative = (
        "selected_rows_seed", "selected_rows_change", "source_payload_bytes",
        "change_diff_negative_rows", "change_diff_zero_rows", "change_diff_positive_rows",
        "actual_fused_kernel_entries_continuation_before",
        "actual_fused_kernel_entries_seed", "actual_fused_kernel_entries_before",
        "actual_fused_kernel_entries_after", "actual_fused_kernel_entries_after_continuation",
        "changelog_rows", "snapshot_rows", "continuation_changelog_rows",
        "continuation_snapshot_rows", "checkpoint_bytes",
    )
    for key in positive:
        if type(record.get(key)) is not int or record[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    for key in nonnegative:
        if type(record.get(key)) is not int or record[key] < 0:
            raise ValueError(f"{key} must be a nonnegative integer")
    if record["actual_fused_kernel_entries_before"] != 0:
        raise ValueError("kernel counter must be reset before measured push")
    if record["source_input_rows_seed"] != record["rows_seed"]:
        raise ValueError("source input row count mismatch")
    expected_rows, expected_columns, expected_payload = DIMENSIONS[expected_case]
    if (record["rows_seed"], record["rows_change"], record["source_columns"], record["source_payload_bytes"]) != (
        expected_rows, expected_rows, expected_columns, expected_payload
    ):
        raise ValueError("measurement dimensions differ from the declared case spec")
    if record["selected_rows_seed"] > record["rows_seed"]:
        raise ValueError("seed selected row count exceeds source rows")
    if record["selected_rows_change"] > record["rows_change"]:
        raise ValueError("selected row count exceeds raw change rows")
    if sum(record[key] for key in ("change_diff_negative_rows", "change_diff_zero_rows", "change_diff_positive_rows")) != record["rows_change"]:
        raise ValueError("diff sign counts do not describe the raw change batch")
    if expected_mode == "force-eager" and (record["actual_fused_kernel_entries_seed"] or record["actual_fused_kernel_entries_after"] or record["actual_fused_kernel_entries_after_continuation"]):
        raise ValueError("eager baseline entered fused kernel")
    if expected_mode == "candidate" and expected_case in FUSED_CASES:
        if record["actual_fused_kernel_entries_seed"] != 1 or record["actual_fused_kernel_entries_after"] != 1:
            raise ValueError("eligible candidate must enter kernel once on seed and measured push")
        if record["actual_fused_kernel_entries_after_continuation"] != 2:
            raise ValueError("eligible candidate must enter kernel once on continuation")
    elif expected_mode == "force-eager" or expected_case == "dictionary-discard-fallback":
        if any(record[key] for key in (
        "actual_fused_kernel_entries_seed", "actual_fused_kernel_entries_after",
        "actual_fused_kernel_entries_after_continuation",
        )):
            raise ValueError("eager/fallback mode entered the fused kernel")
    if type(record.get("view_noop")) is not bool:
        raise ValueError("view_noop must be boolean")
    if expected_case in {"wide-utf8-discard-allfalse", "narrow-allfalse-control"}:
        if record["selected_rows_change"] == 0 and (not record["view_noop"] or record["changelog_rows"] != 0):
            raise ValueError("all-false measured push must have no view changelog")
    elif record["selected_rows_change"] > 0 and (record["view_noop"] or record["changelog_rows"] == 0):
        raise ValueError("selected source change must emit a nonempty view changelog")
    if record["continuation_changelog_rows"] == 0:
        raise ValueError("continuation must emit a nonempty view changelog")
    if expected_case == "dictionary-discard-fallback" and expected_mode == "candidate" and record["actual_fused_kernel_entries_after"] != 0:
        raise ValueError("dictionary fallback must remain eager")
    if record.get("allocation_observer_feature") is not False:
        raise ValueError("timing and memory binaries must have allocation observer off")
    if expected_case == "group-float-sum-avg":
        histories = record.get("float_group_push_history_bits")
        sequence = record.get("float_group_reference_sequence_bits")
        if not histories or len(histories) != 3 or len(sequence or []) != 3:
            raise ValueError("float history does not include the ordered M,1,-M pushes")
        sums = [entry[0][0] for entry in histories if entry]
        if len(sums) != 3 or sums != sequence:
            raise ValueError("float SUM history bits differ from ordered fixture sequence")
    return record


def parse_json_record(output: str, name: str) -> dict:
    records = []
    for line in output.splitlines():
        start = line.find("{")
        while start >= 0:
            try:
                value, end = json.JSONDecoder().raw_decode(line[start:])
            except json.JSONDecodeError:
                break
            if isinstance(value, dict) and value.get("record") == name:
                records.append(value)
            start = line.find("{", start + end)
    if len(records) != 1:
        raise ValueError(f"expected one {name} record; found {len(records)}")
    return records[0]


def validate_memory_record(record: dict, expected_case: str, expected_mode: str) -> dict:
    if record.get("record") != "filter_project_memory_sample":
        raise ValueError("memory record kind mismatch")
    if record.get("case") != expected_case or record.get("mode") != expected_mode:
        raise ValueError("memory case or mode does not match requested child")
    rows, columns, payload_bytes = DIMENSIONS[expected_case]
    if (record.get("source_input_rows_seed"), record.get("source_columns"), record.get("source_payload_bytes_seed")) != (rows, columns, payload_bytes):
        raise ValueError("memory seed dimensions differ from the declared case")
    if (record.get("source_input_rows_change"), record.get("source_payload_bytes_change")) != (rows, payload_bytes):
        raise ValueError("memory change dimensions differ from the declared case")
    selected = record.get("selected_rows_seed")
    selected_change = record.get("selected_rows_change")
    if type(selected) is not int or type(selected_change) is not int or not (0 <= selected <= rows and 0 <= selected_change <= rows):
        raise ValueError("memory selected row counts must describe the raw input bounds")
    schema = record.get("source_schema_seed")
    if not isinstance(schema, list) or len(schema) != columns or any(
        not isinstance(field, dict) or not isinstance(field.get("name"), str) or not field["name"]
        or not isinstance(field.get("data_type"), str) or not field["data_type"]
        for field in schema
    ):
        raise ValueError("memory record must include the actual source schema")
    hwm = record.get("post_exec_observed_peak_rss_kib")
    if type(hwm) is not int or hwm <= 0:
        raise ValueError("memory record must include a positive in-process VmHWM observation")
    return record


def source_freeze() -> dict:
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT
    ).split(b"\0")
    files = {}
    for raw in names:
        if raw:
            name = raw.decode()
            path = ROOT / name
            if path.is_file():
                files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    vendor = subprocess.check_output(
        ["git", "status", "--short", "vendor/datafusion-55.1.0"], cwd=ROOT, text=True
    ).splitlines()
    return {
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "files_sha256": files,
        "vendor_datafusion_status": vendor,
    }


def enriched_freeze(local_hashes: dict) -> dict:
    result = source_freeze()
    result["local_benchmark_source_hashes"] = local_hashes
    return result


def build_binary(archive: Path, feature: str | None, log_dir: Path) -> dict:
    archive.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.update({"CARGO_BUILD_JOBS": "1", "RUST_TEST_THREADS": "1", "RUSTFLAGS": "", "CARGO_ENCODED_RUSTFLAGS": ""})
    for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"):
        env[key] = ""
    args = ["cargo", "test", "-p", "hotlap-engine", "--release", "--locked", "--lib", "--no-run", "--message-format=json"]
    if feature:
        args += ["--features", feature]
    completed = subprocess.run(args, cwd=ROOT, env=env, text=True, capture_output=True)
    feature_label = (feature or "default").replace("/", "-")
    (log_dir / f"build-{feature_label}.stdout.log").write_text(completed.stdout)
    (log_dir / f"build-{feature_label}.stderr.log").write_text(completed.stderr)
    if completed.returncode:
        raise RuntimeError(f"cargo build failed ({feature or 'default'}): {completed.returncode}")
    artifacts = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith("{")]
    executable = next(
        (Path(message["executable"]) for message in artifacts
         if message.get("reason") == "compiler-artifact" and message.get("executable")),
        None,
    )
    if executable is None or not executable.is_file():
        raise RuntimeError("cargo compiler-artifact did not report a test executable")
    observed_features = next(
        (message.get("features", []) for message in artifacts
         if message.get("reason") == "compiler-artifact" and message.get("executable")),
        [],
    )
    expected_observer = feature is not None
    if ("bench-allocation-observer" in observed_features) != expected_observer:
        raise RuntimeError(f"compiled observer feature mismatch: {observed_features}")
    archived = archive / executable.name
    shutil.copy2(executable, archived)
    digest = hashlib.sha256(archived.read_bytes()).hexdigest()
    return {
        "cargo_argv": args,
        "cargo_profile": "release",
        "features": feature or "default (observer off)",
        "environment": {key: env.get(key, "") for key in ("CARGO_BUILD_JOBS", "RUST_TEST_THREADS", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS")},
        "returncode": completed.returncode,
        "compiler_artifact": str(executable),
        "archived_executable": str(archived),
        "binary_sha256": digest,
        "features_observed": [item.get("features", []) for item in artifacts if item.get("reason") == "compiler-artifact" and item.get("executable")],
        "stdout_sha256": hashlib.sha256(completed.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(completed.stderr.encode()).hexdigest(),
    }


def run_direct(binary: Path, test: str, case: str, mode: str, out_path: Path, err_path: Path, cpu: int | None = None) -> dict:
    argv = child_argv(str(binary), test, cpu)
    env = os.environ.copy()
    env.update({"FILTER_PROJECT_CASE": case, "FILTER_PROJECT_MODE": mode})
    for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"):
        env[key] = ""
    if test in {TEST, MEMORY_TEST, OPERATOR_TEST} and "observer-on" in str(binary):
        raise RuntimeError("observer-on binary prohibited for timing and memory samples")
    if test == ALLOC_TEST and "observer-on" not in str(binary):
        raise RuntimeError("allocation sample requires separately archived observer-on binary")
    if test in {TEST, MEMORY_TEST, OPERATOR_TEST} and "observer-on" in str(binary):
        raise RuntimeError("observer-on binary prohibited for wall and memory samples")
    started = time.monotonic_ns()
    with out_path.open("w") as stdout, err_path.open("w") as stderr:
        process = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=stdout, stderr=stderr)
        child_affinity = child_affinity_when_pinned(process.pid, cpu) if cpu is not None else None
        _pid, wait_status, usage = os.wait4(process.pid, 0)
        returncode = os.waitstatus_to_exitcode(wait_status)
        process.returncode = returncode
        process.wait()
    elapsed_ns = time.monotonic_ns() - started
    if returncode:
        raise RuntimeError(f"direct test binary failed: {argv}; see {err_path}")
    output = out_path.read_text()
    record_name = {
        TEST: PREFIX,
        MEMORY_TEST: "filter_project_memory_sample",
        ALLOC_TEST: "filter_project_operator_allocation",
        OPERATOR_TEST: "filter_project_operator_wall_sample",
    }.get(test, "filter_project_operator_wall_sample")
    parsed = parse_measurement_output(output, case, mode) if test == TEST else parse_json_record(output, record_name)
    if parsed.get("case") != case:
        raise ValueError("child output case mismatch")
    if test == ALLOC_TEST and ("old_filter_then_project" not in parsed or "fused_filter_project" not in parsed):
        raise ValueError("allocation record is missing one operator mode")
    if test == OPERATOR_TEST and (parsed.get("observer_feature") is not False or parsed.get("iterations", 0) <= 0):
        raise ValueError("operator wall record must be an observer-off loop sample")
    if test == MEMORY_TEST and (parsed.get("observer_feature") is not False or not parsed.get("engine_state_retained")):
        raise ValueError("memory record must retain engine state with observer off")
    if test == MEMORY_TEST:
        validate_memory_record(parsed, case, mode)
    if test != ALLOC_TEST and parsed.get("mode") != mode:
        raise ValueError("child output mode mismatch")
    return {
        "argv": argv,
        "launcher_argv": argv[:-6] if cpu is not None else None,
        "requested_cpu": cpu,
        "child_affinity": child_affinity,
        "environment": {key: env.get(key, "") for key in ("FILTER_PROJECT_CASE", "FILTER_PROJECT_MODE", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS")},
        "returncode": returncode,
        "child_elapsed_ns": elapsed_ns,
        "child_user_s": usage.ru_utime,
        "child_system_s": usage.ru_stime,
        "child_max_rss_kib": usage.ru_maxrss,
        "wait4_peak_rss_including_preexec_kib": usage.ru_maxrss,
        "wait4_cpu_rss_scope": "whole child including process startup, fixture/input construction, seed, measured push, drain, and report",
        "stdout": str(out_path),
        "stderr": str(err_path),
        "stdout_sha256": hashlib.sha256(out_path.read_bytes()).hexdigest(),
        "stderr_sha256": hashlib.sha256(err_path.read_bytes()).hexdigest(),
        "measurement": parsed,
    }


def run_timing(binary: Path, case: str, mode: str, label: str, outdir: Path, cpu: int | None = None) -> dict:
    return run_direct(binary, TEST, case, mode, outdir / f"{label}.stdout", outdir / f"{label}.stderr", cpu)


def run_operator(binary: Path, test: str, case: str, mode: str, label: str, outdir: Path, cpu: int | None = None) -> dict:
    return run_direct(binary, test, case, mode, outdir / f"{label}.stdout", outdir / f"{label}.stderr", cpu)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=CASES, action="append")
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu", type=int, help="pin each launched measurement child to this allowed logical CPU")
    parser.add_argument("--smoke", action="store_true", help="one candidate/eager timing pair, one memory mode pair, and allocation smoke")
    args = parser.parse_args()
    if not args.output.is_absolute() or not str(args.output).startswith("/tmp/opencode/"):
        parser.error("--output must be under /tmp/opencode/")
    if args.output.exists():
        parser.error("--output already exists; choose a new evidence path")
    if args.samples < 1:
        parser.error("--samples must be positive")
    if args.cpu is not None:
        if args.cpu < 0:
            parser.error("--cpu must be a nonnegative CPU id")
        try:
            allowed_cpus = os.sched_getaffinity(0)
        except AttributeError:
            parser.error("--cpu is unsupported on this platform")
        if args.cpu not in allowed_cpus:
            parser.error(f"--cpu {args.cpu} is not in this process's allowed affinity {sorted(allowed_cpus)}")
    cases = args.case or list(CASES)
    if args.smoke and not set(cases).issubset({"tiny-1-alltrue", "wide-utf8-discard-mixed", "group-float-sum-avg", "wide-utf8-discard-allfalse"}):
        parser.error("--smoke accepts only tiny, wide mixed, grouped float, and wide all-false cases")
    outdir = args.output.parent / (args.output.stem + "-artifacts")
    if outdir.exists():
        parser.error("artifact directory already exists; choose a new output stem")
    logs = outdir / "build-logs"
    logs.mkdir(parents=True, exist_ok=False)
    initial = source_freeze()
    local_inputs = ["Cargo.toml", "Cargo.lock", "crates/hotlap-engine", "tools/filter_project_measure.py", "tools/test_filter_project_measure.py"]
    local_hashes = {}
    for entry in local_inputs:
        path = ROOT / entry
        candidates = [path] if path.is_file() else sorted(item for item in path.rglob("*") if item.is_file())
        for item in candidates:
            local_hashes[str(item.relative_to(ROOT))] = hashlib.sha256(item.read_bytes()).hexdigest()
    initial["local_benchmark_source_hashes"] = local_hashes
    (outdir / "source-freeze-before.json").write_text(json.dumps(initial, indent=2, sort_keys=True) + "\n")
    binaries = {
        "observer-off": build_binary(outdir / "observer-off", None, logs),
        "observer-on": build_binary(outdir / "observer-on", "hotlap-engine/bench-allocation-observer", logs),
    }
    for mode_name, metadata in binaries.items():
        executable = Path(metadata["archived_executable"])
        if not executable.is_file() or hashlib.sha256(executable.read_bytes()).hexdigest() != metadata["binary_sha256"]:
            raise RuntimeError(f"archived {mode_name} executable hash verification failed")
    if enriched_freeze(local_hashes) != initial:
        raise RuntimeError("source changed while compiling archived binaries")
    records = []
    rss_observations = []
    hwm_observations = []
    repeats = 1 if args.smoke else args.samples
    for case in cases:
        for mode in ("candidate", "force-eager"):
            label = f"warmup-{case}-{mode}"
            records.append({"record": "smoke" if args.smoke else "measurement", "phase": "excluded-warmup", "case": case, "mode": mode,
                            "binary": binaries["observer-off"],
                            "sample": run_timing(Path(binaries["observer-off"]["archived_executable"]), case, mode, label, outdir, args.cpu)})
        for trial in range(repeats):
            modes = ("candidate", "force-eager") if trial % 2 == 0 else ("force-eager", "candidate")
            for mode in modes:
                label = f"timing-{case}-{trial}-{mode}"
                records.append({"record": "smoke" if args.smoke else "timing", "phase": "timing", "case": case,
                                "trial": trial, "mode": mode, "binary": binaries["observer-off"],
                                "sample": run_timing(Path(binaries["observer-off"]["archived_executable"]), case, mode, label, outdir, args.cpu)})
        if args.smoke:
            timing_samples = [r["sample"]["measurement"] for r in records if r["case"] == case and r["phase"] == "timing"]
            expected_modes = {"candidate", "force-eager"}
            if {sample["mode"] for sample in timing_samples} != expected_modes:
                raise RuntimeError("smoke timing phase missing a candidate/eager result")
            if any(sample["continuation_changelog_rows"] == 0 for sample in timing_samples):
                raise RuntimeError("smoke timing phase produced an empty continuation changelog")
            if case == "wide-utf8-discard-allfalse":
                if any(sample["selected_rows_change"] != 0 or sample["changelog_rows"] != 0 or not sample["view_noop"] for sample in timing_samples):
                    raise RuntimeError("all-false timed input was not empty")
            elif any(sample["selected_rows_change"] > 0 and sample["changelog_rows"] == 0 for sample in timing_samples):
                raise RuntimeError("selected timed input produced an empty changelog")
        for mode in ("candidate", "force-eager"):
            records.append({"record": "measurement", "phase": "excluded-operator-warmup", "case": case, "mode": mode,
                            "binary": binaries["observer-off"],
                            "sample": run_operator(Path(binaries["observer-off"]["archived_executable"]), OPERATOR_TEST, case, mode,
                                                   f"operator-warmup-{case}-{mode}", outdir, args.cpu)})
        for trial in range(repeats):
            modes = ("candidate", "force-eager") if trial % 2 == 0 else ("force-eager", "candidate")
            for mode in modes:
                records.append({"record": "measurement", "phase": "operator-wall", "case": case, "mode": mode, "trial": trial,
                                "binary": binaries["observer-off"],
                                "sample": run_operator(Path(binaries["observer-off"]["archived_executable"]), OPERATOR_TEST, case, mode,
                                                       f"operator-wall-{case}-{trial}-{mode}", outdir, args.cpu)})
        records.append({"record": "measurement", "phase": "allocation", "case": case, "mode": "candidate",
                        "binary": binaries["observer-on"],
                        "sample": run_operator(Path(binaries["observer-on"]["archived_executable"]), ALLOC_TEST, case, "candidate",
                                               f"allocation-{case}", outdir, args.cpu)})
        if case in {"tiny-1-alltrue", "wide-utf8-discard-mixed", "repeated-projection"}:
            memory_pairs = 1 if args.smoke else 3
            for trial in range(memory_pairs):
                modes = ("candidate", "force-eager") if trial % 2 == 0 else ("force-eager", "candidate")
                for mode in modes:
                    label = f"memory-{case}-{trial}-{mode}"
                    records.append({"record": "measurement", "phase": "memory", "case": case, "mode": mode, "trial": trial,
                                    "binary": binaries["observer-off"],
                                    "sample": run_direct(Path(binaries["observer-off"]["archived_executable"]), MEMORY_TEST, case, mode,
                                                         outdir / f"{label}.stdout", outdir / f"{label}.stderr", args.cpu)})
                    memsample = records[-1]["sample"]["measurement"]
                    rss_observations.extend((
                        memsample["process_rss_kib_after_seed_drain"],
                        memsample["process_rss_kib_after_measured_push_and_drain_engine_alive"],
                        memsample["process_rss_kib_after_dropping_raw_inputs_engine_alive"],
                    ))
                    hwm_observations.append(memsample["post_exec_observed_peak_rss_kib"])
    final = enriched_freeze(local_hashes)
    (outdir / "source-freeze-after.json").write_text(json.dumps(final, indent=2, sort_keys=True) + "\n")
    if initial != final:
        raise RuntimeError("source changed between freeze and completion")
    result = {"kind": "smoke-only" if args.smoke else "measurement", "timing_conclusion": None if args.smoke else "pending independent harness review",
              "requested_cpu": args.cpu, "parent_allowed_cpus": sorted(os.sched_getaffinity(0)) if args.cpu is not None else None,
              "source_freeze_before": initial, "source_freeze_after": final, "binaries": binaries, "records": records,
              "case_summaries": [],
              "memory_child_rss_peak_kib": max(rss_observations, default=None),
              "memory_child_rss_peak_scope": "sampled VmRSS phase values; whole-child wait4 peak includes pre-exec launcher history",
              "post_exec_observed_peak_rss_kib": max(hwm_observations, default=None),
              "post_exec_observed_peak_rss_scope": "maximum in-process VmHWM sampled after input owners are dropped with engine alive; includes Rust exec through input construction, seed, measured push and drain, excludes pre-exec launcher and final reporting/teardown"}
    if not args.smoke:
        for case in cases:
            candidates = [r["sample"]["measurement"]["elapsed_push_ns"] for r in records if r["case"] == case and r["mode"] == "candidate" and r["phase"] == "timing"]
            eager = [r["sample"]["measurement"]["elapsed_push_ns"] for r in records if r["case"] == case and r["mode"] == "force-eager" and r["phase"] == "timing"]
            operator_candidates = [r["sample"]["measurement"]["elapsed_loop_ns"] for r in records if r["case"] == case and r["mode"] == "candidate" and r["phase"] == "operator-wall"]
            operator_eager = [r["sample"]["measurement"]["elapsed_loop_ns"] for r in records if r["case"] == case and r["mode"] == "force-eager" and r["phase"] == "operator-wall"]
            result["case_summaries"].append({
                "case": case,
                "pairs": len(candidates),
                "candidate_push_ns_median": statistics.median(candidates),
                "force_eager_push_ns_median": statistics.median(eager),
                "eager_over_candidate_ratio": statistics.median(eager) / statistics.median(candidates),
                "operator_candidate_loop_ns_median": statistics.median(operator_candidates),
                "operator_force_eager_loop_ns_median": statistics.median(operator_eager),
            })
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
