import argparse
import time
from pathlib import Path

from echodataflow.operations.operations_watchdog import watch_transect_file


def main():
    parser = argparse.ArgumentParser(
        description="Watch a transect CSV file and emit Prefect update events."
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to the transect CSV file to watch.",
    )
    args = parser.parse_args()

    target_file = args.path.resolve()

    if not target_file.exists():
        raise FileNotFoundError(f"Transect file does not exist: {target_file}")

    observer = watch_transect_file(target_file)

    print(f"Watching {target_file}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()