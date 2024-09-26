from statistics import mean, stdev
import json
from pathlib import Path
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("--input-dir", type=Path)
parser.add_argument("--output-dir", type=Path)

args = parser.parse_args()

assert args.input_dir.exists()
assert args.output_dir.exists()

data_file = args.input_dir / "data.json"
with data_file.open("r") as f:
    data = json.load(f)


output = {"mean": mean(data), "std": stdev(data)}

output_file = args.input_dir / "stats.json"
with output_file.open("w") as f:
    json.dump(output, f)
