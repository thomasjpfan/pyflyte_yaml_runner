from pathlib import Path
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("--data-dir", type=Path, required=True)
parser.add_argument("--results-dir", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, required=True)

args = parser.parse_args()

new_file = args.output_dir / "another.text"
new_file.write_text("helllo there")
