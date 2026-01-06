#!/usr/bin/env python3

import shutil
from pathlib import Path

OUTPUT_DIR_NAME = "collected_readmes"


def collect_readmes(project_root: Path):
    output_dir = project_root / OUTPUT_DIR_NAME
    output_dir.mkdir(exist_ok=True)

    seen_names = set()

    for readme in project_root.rglob("README.md"):
        # Skip the output directory itself
        if OUTPUT_DIR_NAME in readme.parts:
            continue

        parent_dir_name = readme.parent.name or "root"
        new_name = f"README_{parent_dir_name}.md"
        target_path = output_dir / new_name

        # Handle name collisions
        counter = 1
        while target_path.name in seen_names or target_path.exists():
            target_path = output_dir / f"README_{parent_dir_name}_{counter}.md"
            counter += 1

        shutil.copy2(readme, target_path)
        seen_names.add(target_path.name)

    print(f"Collected READMEs into: {output_dir}")


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    collect_readmes(project_root)
