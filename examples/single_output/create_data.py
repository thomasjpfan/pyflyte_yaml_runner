import json
import random
from pathlib import Path
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("--seed", type=int, required=True)
parser.add_argument("--output-dir", type=Path, required=True)

args = parser.parse_args()


rng = random.Random(args.seed)

data = [rng.gammavariate(alpha=0.2, beta=1.2) for _ in range(200)]

assert args.output_dir.exists()

my_data = args.output_dir / "data.json"
with my_data.open("w") as f:
    json.dump(data, f)
