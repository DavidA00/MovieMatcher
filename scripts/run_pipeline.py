import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

from tqdm import tqdm


def run_step(step_name: str, script_path: str, log_file: Path):
    cmd = [sys.executable, script_path]

    env = os.environ.copy()
    # Let downstream scripts log to the same file if they support it.
    env["MOVIEMATCHER_LOG_FILE"] = str(log_file)

    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    ) as proc:
        assert proc.stdout is not None
        for line in proc.stdout:
            with log_file.open("a", encoding="utf-8") as f:
                f.write(line)
        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(f"Step failed ({step_name}) with exit code {rc}")


def main():
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"pipeline_{ts}.log"

    steps = [
        # ("fetch_tmdb_metadata", "scripts/fetch_tmdb_metadata.py"),
        #("build_pre_neo4j_tables", "scripts/build_pre_neo4j_tables_-2.py"),
        #("build_graph_dataset", "scripts/build_graph_dataset_-1.py"),
        ("build_neo4j", "scripts/build_neo4j.py"),
    ]

    log_file.write_text(f"MovieMatcher pipeline started at {ts}\n", encoding="utf-8")

    for step_name, script_path in tqdm(steps, desc="Pipeline steps"):
        header = f"\n\n===== STEP: {step_name} ({script_path}) =====\n"
        with log_file.open("a", encoding="utf-8") as f:
            f.write(header)
        run_step(step_name, script_path, log_file)

    with log_file.open("a", encoding="utf-8") as f:
        f.write("\n\nPipeline finished successfully.\n")

    print(f"Done. Logfile: {log_file}")


if __name__ == "__main__":
    main()

