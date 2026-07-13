#!/usr/bin/env python3
"""Generate PDF figures for a real 3-GPU Kubernetes experiment result."""

from __future__ import annotations

import csv
import datetime as dt
import json
import math
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path


PALETTE = [
    (0.10, 0.32, 0.55),
    (0.80, 0.28, 0.20),
    (0.13, 0.55, 0.36),
    (0.55, 0.30, 0.65),
    (0.82, 0.55, 0.12),
    (0.25, 0.50, 0.70),
]

SLO_MS = {"llama": 180.0, "gpt2": 50.0, "resnet50": 100.0}


def _pdf_text(s: object) -> str:
    return str(s).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


class Pdf:
    def __init__(self, path: Path, width: int = 720, height: int = 470) -> None:
        self.path = path
        self.width = width
        self.height = height
        self.ops: list[str] = []

    def color(self, rgb: tuple[float, float, float]) -> None:
        r, g, b = rgb
        self.ops.append(f"{r:.3f} {g:.3f} {b:.3f} RG {r:.3f} {g:.3f} {b:.3f} rg")

    def line_width(self, w: float) -> None:
        self.ops.append(f"{w:.3f} w")

    def line(self, x1: float, y1: float, x2: float, y2: float) -> None:
        self.ops.append(f"{x1:.2f} {y1:.2f} m {x2:.2f} {y2:.2f} l S")

    def polyline(self, pts: list[tuple[float, float]]) -> None:
        if len(pts) < 2:
            return
        parts = [f"{pts[0][0]:.2f} {pts[0][1]:.2f} m"]
        parts.extend(f"{x:.2f} {y:.2f} l" for x, y in pts[1:])
        parts.append("S")
        self.ops.append(" ".join(parts))

    def rect(self, x: float, y: float, w: float, h: float, fill: bool = True) -> None:
        self.ops.append(f"{x:.2f} {y:.2f} {w:.2f} {h:.2f} re {'f' if fill else 'S'}")

    def text(self, x: float, y: float, s: object, size: int = 10, align: str = "left") -> None:
        text = _pdf_text(s)
        # Helvetica average width is close enough for chart labels.
        dx = 0.0
        if align == "center":
            dx = -0.26 * size * len(str(s))
        elif align == "right":
            dx = -0.52 * size * len(str(s))
        self.ops.append(f"BT /F1 {size} Tf {x + dx:.2f} {y:.2f} Td ({text}) Tj ET")

    def write(self) -> None:
        stream = "\n".join(self.ops).encode("latin-1", "replace")
        objects = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {self.width} {self.height}] "
                "/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
            ).encode(),
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
            b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        ]
        out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
        offsets = [0]
        for i, obj in enumerate(objects, 1):
            offsets.append(len(out))
            out.extend(f"{i} 0 obj\n".encode())
            out.extend(obj)
            out.extend(b"\nendobj\n")
        xref = len(out)
        out.extend(f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode())
        for off in offsets[1:]:
            out.extend(f"{off:010d} 00000 n \n".encode())
        out.extend(f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode())
        self.path.write_bytes(out)


class Chart:
    def __init__(self, pdf: Pdf, title: str, xlabel: str, ylabel: str) -> None:
        self.pdf = pdf
        self.left, self.bottom, self.right, self.top = 72, 68, 690, 412
        self.title, self.xlabel, self.ylabel = title, xlabel, ylabel

    def frame(self, xmin: float, xmax: float, ymin: float, ymax: float) -> tuple:
        if xmin == xmax:
            xmax = xmin + 1
        if ymin == ymax:
            ymax = ymin + 1
        pad = (ymax - ymin) * 0.08
        ymin = min(0, ymin) if ymin >= 0 else ymin - pad
        ymax = ymax + pad
        self.bounds = xmin, xmax, ymin, ymax
        p = self.pdf
        p.color((0, 0, 0))
        p.line_width(0.8)
        p.line(self.left, self.bottom, self.right, self.bottom)
        p.line(self.left, self.bottom, self.left, self.top)
        p.text((self.left + self.right) / 2, 440, self.title, 14, "center")
        p.text((self.left + self.right) / 2, 28, self.xlabel, 10, "center")
        p.text(15, (self.bottom + self.top) / 2, self.ylabel, 10)
        for i in range(6):
            tx = xmin + (xmax - xmin) * i / 5
            ty = ymin + (ymax - ymin) * i / 5
            x = self.x(tx)
            y = self.y(ty)
            p.color((0.86, 0.86, 0.86))
            p.line_width(0.4)
            p.line(x, self.bottom, x, self.top)
            p.line(self.left, y, self.right, y)
            p.color((0, 0, 0))
            p.text(x, self.bottom - 18, _fmt(tx), 8, "center")
            p.text(self.left - 10, y - 3, _fmt(ty), 8, "right")
        return self.bounds

    def x(self, v: float) -> float:
        xmin, xmax, _, _ = self.bounds
        return self.left + (v - xmin) / (xmax - xmin) * (self.right - self.left)

    def y(self, v: float) -> float:
        _, _, ymin, ymax = self.bounds
        return self.bottom + (v - ymin) / (ymax - ymin) * (self.top - self.bottom)


def _fmt(v: float) -> str:
    if abs(v) >= 100:
        return f"{v:.0f}"
    if abs(v) >= 10:
        return f"{v:.1f}"
    return f"{v:.2f}".rstrip("0").rstrip(".")


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def slo_wall_clock(row: dict[str, str]) -> float:
    by_model = json.loads(row.get("sloByModel") or "{}")
    intervals: list[tuple[float, float]] = []
    for stats in by_model.values():
        if not isinstance(stats, dict):
            continue
        start = parse_rfc3339(stats.get("firstViolationAt"))
        if start is None:
            continue
        if stats.get("latencySLOViolationWallSeconds") is not None:
            intervals.append((start, start + float(stats["latencySLOViolationWallSeconds"])))
            continue
        end = parse_rfc3339(stats.get("lastViolationAt"))
        if end is not None and end >= start:
            intervals.append((start, end))
    return union_seconds(intervals)


def slo_excess(row: dict[str, str]) -> float:
    if row.get("sloViolationExcessSec"):
        return float(row["sloViolationExcessSec"])
    return float(row.get("sloViolationDurationSec") or 0.0)


def stage_name(row: dict[str, str]) -> str:
    plan = row.get("plan") or ""
    return plan[5:] if plan.startswith("plan-") else plan


def slo_p95_duration(row: dict[str, str], request_p95: dict[str, dict[str, object]]) -> float:
    if row.get("sloViolationP95BucketSec"):
        return float(row["sloViolationP95BucketSec"])
    stage = stage_name(row)
    if stage in request_p95:
        return float(request_p95[stage]["duration"])
    return slo_wall_clock(row)


def p95_from_requests(
    path: Path,
    stage_windows: dict[str, tuple[float | None, float | None]],
    bucket_seconds: float = 1.0,
) -> dict[str, dict[str, object]]:
    buckets: dict[str, dict[str, dict[int, list[float]]]] = defaultdict(lambda: defaultdict(dict))
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            if row.get("phase") != "transition":
                continue
            stage = row.get("stage") or ""
            model = row.get("model") or ""
            if model not in SLO_MS:
                continue
            try:
                sent_at = float(row.get("sentAt") or 0)
                latency = float(row.get("latencyMs") or 0)
            except ValueError:
                continue
            window_start, window_end = stage_windows.get(stage, (None, None))
            if window_start is not None and sent_at < window_start:
                continue
            if window_end is not None and sent_at > window_end:
                continue
            bucket = int(math.floor(sent_at / bucket_seconds))
            buckets[stage][model].setdefault(bucket, []).append(latency)

    out: dict[str, dict[str, object]] = {}
    for stage, by_model in buckets.items():
        intervals = []
        model_summary = {}
        for model, model_buckets in by_model.items():
            violating = 0
            max_p95 = 0.0
            for bucket, values in model_buckets.items():
                p95 = percentile(values, 95.0)
                max_p95 = max(max_p95, p95)
                if p95 > SLO_MS[model]:
                    violating += 1
                    start = bucket * bucket_seconds
                    end = start + bucket_seconds
                    window_start, window_end = stage_windows.get(stage, (None, None))
                    clipped_start = max(start, window_start) if window_start is not None else start
                    clipped_end = min(end, window_end) if window_end is not None else end
                    if clipped_end > clipped_start:
                        intervals.append((clipped_start, clipped_end))
            model_summary[model] = {
                "bucketCount": len(model_buckets),
                "violatingBucketCount": violating,
                "maxBucketP95LatencyMs": round(max_p95, 3),
                "latencySLOMs": SLO_MS[model],
            }
        out[stage] = {"duration": union_seconds(intervals), "byModel": model_summary}
    return out


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[rank]


def parse_rfc3339(value: object) -> float | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def union_seconds(intervals: list[tuple[float, float]]) -> float:
    if not intervals:
        return 0.0
    intervals.sort()
    merged: list[list[float]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        elif end > merged[-1][1]:
            merged[-1][1] = end
    return sum(end - start for start, end in merged)


def write_line_pdf(path: Path, title: str, xlabel: str, ylabel: str, series: dict[str, list[tuple[float, float]]]) -> None:
    pts = [pt for values in series.values() for pt in values]
    pdf = Pdf(path)
    c = Chart(pdf, title, xlabel, ylabel)
    c.frame(min(x for x, _ in pts), max(x for x, _ in pts), min(y for _, y in pts), max(y for _, y in pts))
    for i, (name, values) in enumerate(series.items()):
        pdf.color(PALETTE[i % len(PALETTE)])
        pdf.line_width(1.6)
        pdf.polyline([(c.x(x), c.y(y)) for x, y in values])
        pdf.rect(500, 390 - i * 16, 10, 8, True)
        pdf.color((0, 0, 0))
        pdf.text(516, 388 - i * 16, name, 9)
    pdf.write()


def write_bar_pdf(path: Path, title: str, xlabel: str, ylabel: str, bars: list[tuple[str, float]], color=(0.10, 0.32, 0.55)) -> None:
    pdf = Pdf(path)
    c = Chart(pdf, title, xlabel, ylabel)
    ymax = max([v for _, v in bars] + [1])
    c.frame(-0.5, len(bars) - 0.5, 0, ymax)
    width = 0.66
    pdf.color(color)
    for i, (label, value) in enumerate(bars):
        x0 = c.x(i - width / 2)
        x1 = c.x(i + width / 2)
        pdf.rect(x0, c.y(0), x1 - x0, c.y(value) - c.y(0), True)
        pdf.color((0, 0, 0))
        pdf.text(c.x(i), c.bottom - 36, label, 8, "center")
        pdf.text(c.x(i), c.y(value) + 4, _fmt(value), 8, "center")
        pdf.color(color)
    pdf.write()


def transition_and_planner_pdf(out: Path, metrics: list[dict[str, str]]) -> None:
    pdf = Pdf(out)
    c = Chart(pdf, "Transition and planner makespan", "epoch", "seconds")
    epochs = [int(r["epoch"]) for r in metrics]
    tvals = [float(r["transitionMakespanSec"]) for r in metrics]
    pvals = [float(r["plannerMakespanSec"]) for r in metrics]
    c.frame(min(epochs) - 0.5, max(epochs) + 0.5, 0, max(tvals + pvals))
    for i, e in enumerate(epochs):
        for offset, val, col in [(-0.17, tvals[i], PALETTE[0]), (0.17, pvals[i], PALETTE[1])]:
            pdf.color(col)
            x0 = c.x(e + offset - 0.13)
            x1 = c.x(e + offset + 0.13)
            pdf.rect(x0, c.y(0), x1 - x0, c.y(val) - c.y(0), True)
    for i, (name, col) in enumerate([("transition", PALETTE[0]), ("planner", PALETTE[1])]):
        pdf.color(col)
        pdf.rect(500, 390 - i * 16, 10, 8, True)
        pdf.color((0, 0, 0))
        pdf.text(516, 388 - i * 16, name, 9)
    pdf.write()


def action_breakdown_pdf(out: Path, action_rows: list[dict[str, str]]) -> None:
    by_epoch: dict[int, Counter] = defaultdict(Counter)
    for r in action_rows:
        by_epoch[int(r["epoch"])][r["type"]] += 1
    action_types = [t for t, _ in Counter(r["type"] for r in action_rows).most_common(6)]
    pdf = Pdf(out)
    c = Chart(pdf, "Transition action breakdown", "epoch", "actions")
    epochs = sorted(by_epoch)
    totals = [sum(by_epoch[e].values()) for e in epochs]
    c.frame(min(epochs) - 0.5, max(epochs) + 0.5, 0, max(totals))
    for e in epochs:
        base = 0
        for i, t in enumerate(action_types):
            v = by_epoch[e][t]
            if not v:
                continue
            pdf.color(PALETTE[i % len(PALETTE)])
            x0, x1 = c.x(e - 0.28), c.x(e + 0.28)
            pdf.rect(x0, c.y(base), x1 - x0, c.y(base + v) - c.y(base), True)
            base += v
    for i, t in enumerate(action_types):
        pdf.color(PALETTE[i % len(PALETTE)])
        pdf.rect(480, 390 - i * 16, 10, 8, True)
        pdf.color((0, 0, 0))
        pdf.text(496, 388 - i * 16, t, 8)
    pdf.write()


def service_rate_pdfs(result_dir: Path, out_dir: Path) -> None:
    data = defaultdict(lambda: defaultdict(list))
    rs = rows(result_dir / "service_rate_samples.csv")
    if not rs:
        return
    t0 = min(float(r["sampledAt"]) for r in rs)
    for r in rs:
        model = r["model"]
        t = (float(r["sampledAt"]) - t0) / 60.0
        for key, col in [
            ("actual", "actualServiceRate"),
            ("capacity", "capacity"),
            ("input", "inputDemandRate"),
            ("target", "targetDemandRate"),
        ]:
            value = maybe_float(r.get(col, ""))
            if value is not None:
                data[model][key].append((t, value))
    for model, series in data.items():
        thin = {k: _thin(v, 900) for k, v in series.items()}
        write_line_pdf(out_dir / f"service_rate_{model}.pdf", f"Service rate: {model}", "minutes", "req/s", thin)


def maybe_float(value: str) -> float | None:
    try:
        if value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _thin(values: list[tuple[float, float]], limit: int) -> list[tuple[float, float]]:
    if len(values) <= limit:
        return values
    step = math.ceil(len(values) / limit)
    return values[::step]


def latency_cdf_pdf(result_dir: Path, out: Path) -> None:
    samples: dict[str, list[float]] = defaultdict(list)
    seen: Counter = Counter()
    limit = 25000
    rng = random.Random(7)
    with (result_dir / "requests.csv").open(newline="") as f:
        for r in csv.DictReader(f):
            if r.get("ok") != "True":
                continue
            model = r["model"]
            seen[model] += 1
            lat = float(r["latencyMs"])
            bucket = samples[model]
            if len(bucket) < limit:
                bucket.append(lat)
            else:
                j = rng.randrange(seen[model])
                if j < limit:
                    bucket[j] = lat
    series = {}
    for model, vals in samples.items():
        vals.sort()
        n = len(vals)
        keep = vals[::_cdf_step(n, 600)] or vals
        series[model] = [(v, (i * _cdf_step(n, 600) + 1) / n) for i, v in enumerate(keep)]
    write_line_pdf(out, "Latency CDF during experiment", "latency ms", "CDF", series)


def _cdf_step(n: int, limit: int) -> int:
    return max(1, math.ceil(n / limit))


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: plot_real_3gpu_results.py RESULT_DIR")
    result_dir = Path(sys.argv[1])
    out_dir = result_dir / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics = rows(result_dir / "transition_metrics.csv")
    stage_windows = {
        stage_name(r): (parse_rfc3339(r.get("transitionStartedAt")), parse_rfc3339(r.get("transitionFinishedAt")))
        for r in metrics
    }
    request_p95 = p95_from_requests(result_dir / "requests.csv", stage_windows)
    gpu = rows(result_dir / "gpu_counts.csv")
    sim = rows(result_dir / "allocation_similarity.csv")
    actions = rows(result_dir / "action_statuses.csv")

    write_line_pdf(
        out_dir / "active_gpu_count.pdf",
        "Active GPU count over time",
        "epoch + relative transition seconds / 100",
        "active GPUs",
        {"active": [(int(r["epoch"]) + float(r["relativeSeconds"]) / 100.0, float(r["active"])) for r in gpu]},
    )
    transition_and_planner_pdf(out_dir / "makespan.pdf", metrics)
    write_bar_pdf(
        out_dir / "slo_violation_duration.pdf",
        "Transition P95 SLO violation duration",
        "epoch",
        "seconds",
        [(r["epoch"], slo_p95_duration(r, request_p95)) for r in metrics],
        PALETTE[1],
    )
    write_bar_pdf(
        out_dir / "slo_violation_excess.pdf",
        "Transition SLO violation latency excess",
        "epoch",
        "seconds",
        [(r["epoch"], slo_excess(r)) for r in metrics],
        PALETTE[4],
    )
    write_bar_pdf(
        out_dir / "transition_action_count.pdf",
        "Transition actions",
        "epoch",
        "actions",
        [(r["epoch"], float(r["actionCount"])) for r in metrics],
        PALETTE[2],
    )
    if sim:
        write_line_pdf(
            out_dir / "allocation_similarity.pdf",
            "Allocation similarity between consecutive targets",
            "transition",
            "Jaccard similarity",
            {"similarity": [(float(r["toEpoch"]), float(r["jaccardSimilarity"])) for r in sim]},
        )
    action_breakdown_pdf(out_dir / "transition_action_breakdown.pdf", actions)
    service_rate_pdfs(result_dir, out_dir)
    latency_cdf_pdf(result_dir, out_dir / "latency_cdf.pdf")

    with (out_dir / "transition_metrics_slo_wall_clock.csv").open("w", newline="") as f:
        fieldnames = [
            "epoch",
            "planner",
            "actionCount",
            "transitionMakespanSec",
            "plannerMakespanSec",
            "sloViolationWallClockSec",
            "sloViolationP95BucketSec",
            "sloViolationExcessSec",
            "sloViolationCount",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in metrics:
            writer.writerow({
                "epoch": r["epoch"],
                "planner": r.get("planner"),
                "actionCount": r.get("actionCount"),
                "transitionMakespanSec": r.get("transitionMakespanSec"),
                "plannerMakespanSec": r.get("plannerMakespanSec"),
                "sloViolationWallClockSec": round(slo_wall_clock(r), 6),
                "sloViolationP95BucketSec": round(slo_p95_duration(r, request_p95), 6),
                "sloViolationExcessSec": round(slo_excess(r), 6),
                "sloViolationCount": r.get("sloViolationCount"),
            })

    manifest = {
        "source": str(result_dir),
        "figures": sorted(p.name for p in out_dir.glob("*.pdf")),
        "slo_by_epoch": [
            {
                "epoch": int(r["epoch"]),
                "violationWallClockSeconds": slo_wall_clock(r),
                "violationP95BucketSeconds": slo_p95_duration(r, request_p95),
                "violationExcessSeconds": slo_excess(r),
                "p95ByModel": request_p95.get(stage_name(r), {}).get("byModel", {}),
                "byModel": json.loads(r["sloByModel"] or "{}"),
            }
            for r in metrics
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"wrote {len(manifest['figures'])} pdf figures to {out_dir}")


if __name__ == "__main__":
    main()
