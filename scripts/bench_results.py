#!/usr/bin/env python3
"""Turn a benchmark run into the published results document.

`benches/paired.rs` reports the scenarios it measured and nothing else, because the machine and
the build are not its to describe. This script reads that summary, adds the environment the run
was taken in and the versions it was taken against, and writes the document the documentation
site serves at `benchmarks/results.json`.

A scenario carries three runs - the raw client, this crate's own consumer and publisher, and the
whole service - and the two differences between them. The core's cross-broker table reads `raw`
and `framework`; `adapter` and `adapter_overhead_percent` are this crate's own column.

The schema is the core's, declared at
https://powersemmi.github.io/ruststream/latest/benchmarks/#publishing-results: schema 3, each
loop of the comparison as its best, median and worst round, and the `code` section.

`--code` reads the other run instead: the summary `cargo bench -- --output-format=json` writes for
the code-cost benches under `crates/ruststream-lapin-bench/benches`, one JSON object per
benchmark, in the summary layout gungraun 0.20 writes (its version 7). It writes the `code`
section, one entry per scenario with instructions and allocations per message plus what starting
the service cost once, by the core's method: every scenario is measured over one delivery, over
MESSAGES and over twice MESSAGES, the slope between the last two is the steady state, and the
one-delivery run is the cold start. Either run keeps the section the other one wrote.

A code benchmark that breaches one of its limits fails the run, and in this output format the
runner says nothing more about it: what went over is recorded in the summary alone. So `--code`
prints every breach under the table, the value the run was compared against next to the new one,
and a summary that cannot be converted still prints its breaches before it stops.

The environment carries one figure the benchmark measured rather than the machine reported: the
transport's round-trip time. It is what decides whether a row is marked `broker_bound`, so a
reader can redo that arithmetic.

A field the machine does not publish is written as `unknown` rather than guessed: memory speed
comes from the DMI tables, which most systems only let root read.

    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json
    python3 scripts/bench_results.py --code target/bench-code.json docs/benchmarks/results.json
"""

import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
COMPOSE = REPO / "docker-compose.test.yml"
MANIFEST = REPO / "Cargo.toml"
LOCK = REPO / "Cargo.lock"

# What `just bench` builds the benchmark with. Both are recipe decisions rather than machine
# facts, so they are stated here next to the recipe rather than sniffed.
PROFILE = "bench, inheriting release (opt-level = 3, lto = false, codegen-units = 16)"
FEATURES = "ruststream-lapin default (none), ruststream macros,json"
RUSTFLAGS = "none (the recipe clears RUSTFLAGS, so the numbers are not tied to this CPU)"


def run(*args: str) -> str:
    try:
        return subprocess.run(args, check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return ""


def proc_field(path: str, key: str) -> str:
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        name, _, value = line.partition(":")
        if name.strip() == key:
            return value.strip()
    return ""


def lscpu() -> dict[str, str]:
    fields = {}
    for line in run("lscpu").splitlines():
        name, _, value = line.partition(":")
        fields[name.strip()] = value.strip()
    return fields


def cores(cpu: dict[str, str]) -> str:
    physical = cpu.get("Core(s) per socket", "")
    sockets = cpu.get("Socket(s)", "1")
    logical = cpu.get("CPU(s)", "")
    if not physical or not logical:
        return "unknown"
    return f"{int(physical) * int(sockets)} physical, {logical} logical"


def frequency(cpu: dict[str, str]) -> str:
    low, high = cpu.get("CPU min MHz", ""), cpu.get("CPU max MHz", "")
    if not low or not high:
        return "unknown"
    return f"{float(low.replace(',', '.')):.0f}-{float(high.replace(',', '.')):.0f} MHz"


def memory() -> str:
    total = proc_field("/proc/meminfo", "MemTotal")
    if not total.endswith(" kB"):
        return "unknown"
    return f"{int(total[:-3]) / (1024 * 1024):.1f} GiB"


def broker_image() -> str:
    """The image of the first service in the compose file: the stand `just bench` starts.

    The second service there is the plugin-enabled node, which no scenario measures.
    """
    match = re.search(r"^\s+image:\s*(\S+)", COMPOSE.read_text(encoding="utf-8"), re.M)
    return f"{match.group(1)} in Docker on localhost" if match else "unknown"


def crate_version() -> str:
    match = re.search(r'^version = "([^"]+)"', MANIFEST.read_text(encoding="utf-8"), re.M)
    return match.group(1) if match else "unknown"


def core_version() -> str:
    match = re.search(
        r'^name = "ruststream"\nversion = "([^"]+)"', LOCK.read_text(encoding="utf-8"), re.M
    )
    return match.group(1) if match else "unknown"


def environment(round_trip_us: float) -> dict[str, str]:
    cpu = lscpu()
    return {
        "cpu": proc_field("/proc/cpuinfo", "model name") or cpu.get("Model name", "unknown"),
        "architecture": cpu.get("Architecture", "unknown"),
        "cpu_frequency": frequency(cpu),
        "cores": cores(cpu),
        "memory": memory(),
        "memory_speed": "unknown",
        "os": f"Linux {run('uname', '-r').strip()}",
        "broker": broker_image(),
        # The probe the benchmark ran outside every pair. A row is marked `broker_bound` by
        # arithmetic over this figure, so it is published with the numbers it decided.
        "round_trip": f"{round_trip_us:.1f} us for one basic.qos answered by the server",
        "rustc": run("rustc", "--version").replace("rustc", "").strip().split()[0],
        "profile": PROFILE,
        "features": FEATURES,
        "rustflags": RUSTFLAGS,
    }


# The summary layout the code section is read from. Every summary states its layout in `version`,
# and a gungraun release that changes the layout changes the number, so a summary of another
# version stops the conversion with a message naming both rather than with a missing field.
SUMMARY_VERSION = "7"

# Deliveries per measured run of the code-cost benches, the default of their `MESSAGES`.
CODE_MESSAGES = 1000

# An instruction count below this on a code run means the measured region stopped matching its
# frame and the run reported the process exit, not that the code got faster. The cold run handles
# one delivery, so it is held to a lower floor.
CODE_FLOOR = 100_000
CODE_COLD_FLOOR = 1_000

# The code table, in reading order: the published name, the benchmark as `file/function`, and
# whether the benchmark's hard limit holds its allocation floor.
CODE_SCENARIOS = [
    ("classic queue, JSON decode into a small struct, ack each", "consume/service", True),
    ("classic queue, reply published through LapinPublish", "reply/service", True),
    ("classic queue, batches of 64 assembled on the client", "batch/service", True),
]


def code_metric(summary: dict, tool: str, name: str) -> int | None:
    """The new value of one metric: the total of one tool's run, as the runner reports it."""
    for profile in summary["profiles"]:
        if profile["tool"] != tool:
            continue
        values = profile["data"]["total"]["metrics"].get(name, {}).get("values", {})
        # A run compared against a baseline carries the old value next to the new one.
        new = values.get("new")
        return None if new is None else int(new)
    return None


def code_summaries(path: Path) -> list[dict]:
    """Every benchmark summary the run wrote, one per line, in the layout this script reads."""
    found = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        summary = json.loads(line)
        version = summary.get("version")
        if version != SUMMARY_VERSION:
            sys.exit(
                f"the benchmark summary has layout version {version}, and this script reads "
                f"version {SUMMARY_VERSION}: read the new layout in `code_metric` and "
                "`code_breaches` and raise SUMMARY_VERSION"
            )
        found.append(summary)
    return found


def benchmark(summary: dict) -> str:
    """The `file/function` a summary belongs to, which is how a scenario names its benchmark."""
    return f"{Path(summary['benchmark_file']).stem}/{summary['function_name']}"


def code_runs(summaries: list[dict]) -> dict[str, dict]:
    """Every benchmark in the run, keyed by `file/function/id`."""
    return {
        f"{benchmark(summary)}/{summary['id']}": {
            "instructions": code_metric(summary, "Callgrind", "Ir"),
            "allocations": code_metric(summary, "DHAT", "TotalBlocks"),
        }
        for summary in summaries
    }


# The two metrics the code table reads, by the names it gives them. A limit on any other metric is
# reported under the runner's own name for it.
METRIC_NAMES = {("Callgrind", "Ir"): "instructions", ("Dhat", "TotalBlocks"): "allocations"}


def run_name(run: str) -> str:
    """A benchmark id as the number of deliveries its run handled."""
    counts = {"first": 1, "base": CODE_MESSAGES, "twice": 2 * CODE_MESSAGES}
    if run not in counts:
        return run
    return "one delivery" if counts[run] == 1 else f"{counts[run]} deliveries"


def as_text(value: float) -> str:
    """A metric value as a breach line writes it: a count as it is, a fraction in short form."""
    return str(value) if isinstance(value, int) else f"{value:g}"


def breach(regression: dict, metrics: dict) -> str:
    """One limit a run went over: the metric, the value it was compared against, the new one.

    A limit in percent holds the run to the one it is compared against, and the regression
    carries both values. A plain number is a ceiling the run is held to on its own, and the value
    it was compared against is the one the metric records next to the new one, where there is one.
    """
    [(kind, detail)] = regression.items()
    [(tool, name)] = detail["metric"].items()
    label = METRIC_NAMES.get((tool, name), f"{tool} {name}")
    if kind == "Soft":
        return (
            f"{label} {as_text(detail['old'])} -> {as_text(detail['new'])}, "
            f"{float(detail['diff_pct']):+.2f}% against a limit of +{float(detail['limit']):g}%"
        )
    old = metrics.get(name, {}).get("values", {}).get("old")
    change = "" if old is None else f"{as_text(old)} -> "
    return f"{label} {change}{as_text(detail['new'])} against a limit of {as_text(detail['limit'])}"


def code_breaches(summaries: list[dict]) -> list[str]:
    """Every limit the run breached, one line each, named by its scenario and its run."""
    names = {key: name for name, key, _ in CODE_SCENARIOS}
    found = []
    for summary in summaries:
        where = f"{names.get(benchmark(summary), benchmark(summary))}, {run_name(summary['id'])}"
        for profile in summary["profiles"]:
            total = profile["data"]["total"]
            for regression in total["regressions"]:
                found.append(f"{where}: {breach(regression, total['metrics'])}")
    return found


def report_breaches(lines: list[str]) -> None:
    """The limits the run breached, which is why it fails, each with both values it compared."""
    if not lines:
        return
    print()
    print("limits breached (totals of one run, old -> new):")
    for line in lines:
        print(f"  {line}")


def code_total(found: dict, key: str, floor: int) -> dict:
    """One run's totals, checked for the two ways this measurement fails silently."""
    if key not in found:
        sys.exit(
            f"benchmark {key} is not in the run: it failed before it wrote a summary, or it was "
            "renamed (then rename it here or in benches/)"
        )
    measured = found[key]
    if measured["instructions"] is None or measured["instructions"] < floor:
        sys.exit(
            f"benchmark {key} reports {measured['instructions']} instructions, below the floor of "
            f"{floor}: collection did not cover the measured region"
        )
    return measured


def per_message(figure: float) -> float:
    """Three places below one, so one allocation for the whole run does not read as zero."""
    return round(figure, 3) if abs(figure) < 1 else round(figure, 1)


def code_section(found: dict[str, dict]) -> list[dict]:
    rows = []
    for name, key, gated in CODE_SCENARIOS:
        base = code_total(found, f"{key}/base", CODE_FLOOR)
        twice = code_total(found, f"{key}/twice", CODE_FLOOR)
        if twice["instructions"] <= base["instructions"]:
            sys.exit(f"benchmark {key} does not grow with the message count: no slope to read")
        first = code_total(found, f"{key}/first", CODE_COLD_FLOOR)
        rows.append(
            {
                "name": name,
                "messages": CODE_MESSAGES,
                "framework": {
                    metric: per_message((twice[metric] - base[metric]) / CODE_MESSAGES)
                    for metric in ("instructions", "allocations")
                },
                "cold": {metric: first[metric] for metric in ("instructions", "allocations")},
                "gated": gated,
            }
        )
    return rows


def valgrind() -> str:
    return run("valgrind", "--version").strip().removeprefix("valgrind-") or "unknown"


def main() -> int:
    args = sys.argv[1:]
    code = bool(args) and args[0] == "--code"
    if code:
        args = args[1:]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    source, out = Path(args[0]), Path(args[1])
    previous = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    breached = []
    if code:
        summaries = code_summaries(source)
        breached = code_breaches(summaries)
        try:
            rows = code_section(code_runs(summaries))
        except SystemExit:
            # One failure does not hide another: a summary that cannot be converted still shows
            # what the run breached.
            report_breaches(breached)
            sys.stdout.flush()
            raise
        document = previous
        document["schema"] = 3
        document["code"] = rows
        # The code costs carry their own provenance: the paired numbers beside them may come
        # from another run, on another version, on another day.
        document["code_measured"] = {
            "crate_version": crate_version(),
            "core_version": core_version(),
            "measured_at": date.today().isoformat(),
        }
        document.setdefault("environment", {})["valgrind"] = valgrind()
    else:
        summary = json.loads(source.read_text(encoding="utf-8"))
        document = {
            "schema": 3,
            "crate": "ruststream-lapin",
            "crate_version": crate_version(),
            "core_version": core_version(),
            "measured_at": date.today().isoformat(),
            "environment": environment(summary["round_trip_us"]),
            "scenarios": summary["scenarios"],
        }
        if "code" in previous:
            document["code"] = previous["code"]
            if "code_measured" in previous:
                document["code_measured"] = previous["code_measured"]
            if "valgrind" in previous.get("environment", {}):
                document["environment"]["valgrind"] = previous["environment"]["valgrind"]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out}")
    if code:
        for row in document["code"]:
            print(
                f"  {row['name']}: {row['framework']['instructions']} instructions, "
                f"{row['framework']['allocations']} allocations per message; cold "
                f"{row['cold']['instructions']} instructions, {row['cold']['allocations']} "
                "allocations"
            )
    report_breaches(breached)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
