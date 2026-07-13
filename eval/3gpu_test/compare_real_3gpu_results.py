from __future__ import annotations

import csv
import sys
from pathlib import Path

from plot_real_3gpu_results import Chart, PALETTE, Pdf, rows, write_line_pdf


LABELS = {
    "real3gpu-20260710-1618": "ours",
    "real3gpu-jorm-20260710-2200": "jormungandr",
}


def main() -> None:
    if len(sys.argv) < 4:
        raise SystemExit("usage: compare_real_3gpu_results.py OUT_DIR RUN_DIR RUN_DIR ...")
    out_dir = Path(sys.argv[1])
    out_dir.mkdir(parents=True, exist_ok=True)
    run_dirs = [Path(arg) for arg in sys.argv[2:]]
    labels = [LABELS.get(path.name, path.name) for path in run_dirs]
    transition_rows = [transition_metrics(path) for path in run_dirs]
    raw_transition_rows = [rows(path / "transition_metrics.csv") for path in run_dirs]
    grouped_bar_pdf(out_dir / "compare_transition_makespan.pdf", labels, transition_rows, "transitionMakespanSec", "Transition makespan", "seconds")
    grouped_bar_pdf(out_dir / "compare_planner_makespan.pdf", labels, transition_rows, "plannerMakespanSec", "Planner makespan", "seconds")
    grouped_bar_pdf(out_dir / "compare_action_count.pdf", labels, raw_transition_rows, "actionCount", "Transition action count", "actions")
    grouped_bar_pdf(out_dir / "compare_p95_slo_violation.pdf", labels, transition_rows, "sloViolationP95BucketSec", "P95 SLO violation duration", "seconds")
    active_gpu_pdf(out_dir / "compare_active_gpu_count.pdf", labels, run_dirs)
    print(f"wrote comparison pdfs to {out_dir}")


def transition_metrics(run_dir: Path) -> list[dict[str, str]]:
    candidates = [
        run_dir / "figures" / "transition_metrics_slo_wall_clock.csv",
        run_dir / "transition_metrics_slo_wall_clock.csv",
        run_dir / "transition_metrics.csv",
    ]
    for path in candidates:
        if path.exists():
            return rows(path)
    return []


def grouped_bar_pdf(out: Path, labels: list[str], data: list[list[dict[str, str]]], key: str, title: str, ylabel: str) -> None:
    epochs = sorted({int(r["epoch"]) for rows_ in data for r in rows_})
    max_y = max([float_or_zero(r.get(key, "")) for rows_ in data for r in rows_] or [1.0])
    pdf = Pdf(out)
    c = Chart(pdf, title, "epoch", ylabel)
    c.frame(min(epochs) - 0.6, max(epochs) + 0.6, 0, max_y * 1.15 if max_y else 1.0)
    width = 0.28
    for idx, rows_ in enumerate(data):
        by_epoch = {int(r["epoch"]): r for r in rows_}
        pdf.color(PALETTE[idx % len(PALETTE)])
        offset = (idx - (len(data) - 1) / 2) * width
        for epoch in epochs:
            value = float_or_zero(by_epoch.get(epoch, {}).get(key, ""))
            x0, x1 = c.x(epoch + offset - width * 0.42), c.x(epoch + offset + width * 0.42)
            pdf.rect(x0, c.y(0), x1 - x0, c.y(value) - c.y(0), True)
    legend(pdf, labels)
    pdf.write()


def active_gpu_pdf(out: Path, labels: list[str], run_dirs: list[Path]) -> None:
    series = {}
    for label, run_dir in zip(labels, run_dirs):
        values = []
        offset = 0.0
        for r in rows(run_dir / "gpu_counts.csv"):
            epoch = float_or_zero(r.get("epoch", "0"))
            rel = float_or_zero(r.get("relativeSeconds", "0"))
            values.append((epoch * 5.0 + rel / 60.0 + offset, float_or_zero(r.get("active", "0"))))
        series[label] = values
    write_line_pdf(out, "Active GPU count comparison", "experiment minutes", "active GPUs", series)


def legend(pdf: Pdf, labels: list[str]) -> None:
    for idx, label in enumerate(labels):
        pdf.color(PALETTE[idx % len(PALETTE)])
        pdf.rect(470, 390 - idx * 16, 10, 8, True)
        pdf.color((0, 0, 0))
        pdf.text(486, 388 - idx * 16, label, 9)


def float_or_zero(value: str | None) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


if __name__ == "__main__":
    main()
