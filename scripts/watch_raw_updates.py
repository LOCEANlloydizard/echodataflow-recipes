import argparse
import time
from pathlib import Path

from echodataflow.utils.raw_monitor import watch_raw_directory


def main():
    parser = argparse.ArgumentParser(
        description="Watch a RAW directory and emit Prefect update events."
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to the RAW directory to watch.",
    )
    args = parser.parse_args()

    target_dir = args.path.resolve()

    if not target_dir.exists():
        raise FileNotFoundError(
            f"RAW directory does not exist: {target_dir}"
        )

    observer = watch_raw_directory(target_dir)

    print(f"Watching {target_dir}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()
