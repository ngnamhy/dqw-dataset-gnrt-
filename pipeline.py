from __future__ import annotations

import argparse
import json
import math
import random
import re
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse

try:
	import yaml
except ImportError as exc:
	raise SystemExit(
		"Missing dependency: pyyaml. Install it with: pip install pyyaml"
	) from exc


NUMBER_PATTERN = re.compile(r"-?\d+")


def load_config(config_path: Path) -> list[dict]:
	if not config_path.exists():
		raise FileNotFoundError(f"Config file not found: {config_path}")

	with config_path.open("r", encoding="utf-8") as f:
		data = yaml.safe_load(f) or {}

	datasets = data.get("datasets", [])
	if not isinstance(datasets, list):
		raise ValueError("`datasets` must be a list in YAML config")

	return datasets


def infer_filename(dataset_name: str, url: str | None) -> str:
	if url:
		parsed = urlparse(url)
		from_url = Path(parsed.path).name
		if from_url:
			return from_url

	if dataset_name.endswith(".dat"):
		return dataset_name
	return f"{dataset_name}.dat"


def download_with_wget(url: str, output_path: Path) -> None:
	output_path.parent.mkdir(parents=True, exist_ok=True)

	command = ["wget", "-O", str(output_path), url]
	result = subprocess.run(command, check=False, capture_output=True, text=True)
	if result.returncode != 0:
		stderr = result.stderr.strip()
		raise RuntimeError(f"wget failed for {url}. Error: {stderr}")


def compute_file_stats(file_path: Path) -> tuple[int, int, dict]:
	line_count = 0
	unique_items: set[int] = set()

	with file_path.open("r", encoding="utf-8", errors="ignore") as f:
		for line in f:
			line_count += 1
			for token in NUMBER_PATTERN.findall(line):
				unique_items.add(int(token))

	sequence_info = {
		"starts_from": None,
		"is_contiguous": False,
		"min_item": None,
		"max_item": None,
		"missing_item_count": 0,
	}

	if unique_items:
		min_item = min(unique_items)
		max_item = max(unique_items)
		starts_from = min_item if min_item in (0, 1) else None

		if starts_from is not None:
			expected_count = (max_item - starts_from) + 1
			actual_count = sum(1 for x in unique_items if starts_from <= x <= max_item)
			is_contiguous = actual_count == expected_count
			missing_item_count = expected_count - actual_count
		else:
			is_contiguous = False
			missing_item_count = 0

		sequence_info = {
			"starts_from": starts_from,
			"is_contiguous": is_contiguous,
			"min_item": min_item,
			"max_item": max_item,
			"missing_item_count": missing_item_count,
		}

	return line_count, len(unique_items), sequence_info


def write_metadata(output_path: Path, metadata: dict) -> None:
	with output_path.open("w", encoding="utf-8") as f:
		json.dump(metadata, f, indent=2)
		f.write("\n")


def generate_quantity_file(
	source_path: Path,
	quantity_path: Path,
	min_qty: int = 1,
	max_qty: int = 10,
) -> None:
	quantity_path.parent.mkdir(parents=True, exist_ok=True)
	rng = random.Random()

	with source_path.open("r", encoding="utf-8", errors="ignore") as src, quantity_path.open(
		"w", encoding="utf-8"
	) as dst:
		for line in src:
			tokens = line.strip().split()
			if not tokens:
				dst.write("\n")
				continue

			quantities = [str(rng.randint(min_qty, max_qty)) for _ in tokens]
			dst.write(" ".join(quantities) + "\n")


def collect_unique_items(file_path: Path) -> set[int]:
	unique_items: set[int] = set()
	with file_path.open("r", encoding="utf-8", errors="ignore") as src:
		for line in src:
			for token in NUMBER_PATTERN.findall(line):
				unique_items.add(int(token))
	return unique_items


def generate_dynamic_weight_file(
	line_count: int,
	items: set[int],
	dynamic_path: Path,
	min_weight: int = 1,
	max_weight: int = 10,
) -> int:
	dynamic_path.parent.mkdir(parents=True, exist_ok=True)
	rng = random.Random()

	batch_size = max(1, math.isqrt(max(1, line_count)))
	if line_count == 0 or not items:
		dynamic_path.write_text("", encoding="utf-8")
		return batch_size

	with dynamic_path.open("w", encoding="utf-8") as dst:
		for item in sorted(items):
			for start_line in range(1, line_count + 1, batch_size):
				end_line = min(line_count, start_line + batch_size - 1)
				weight = rng.randint(min_weight, max_weight)
				dst.write(f"{item} {start_line} {end_line} {weight}\n")

	return batch_size


def run_pipeline(
	config_path: Path,
	binary_dir: Path,
	local_dir: Path,
	output_dir: Path,
	quantity_dir: Path,
	dynamic_dir: Path,
) -> int:
	datasets = load_config(config_path)
	output_dir.mkdir(parents=True, exist_ok=True)
	binary_dir.mkdir(parents=True, exist_ok=True)
	local_dir.mkdir(parents=True, exist_ok=True)
	quantity_dir.mkdir(parents=True, exist_ok=True)
	dynamic_dir.mkdir(parents=True, exist_ok=True)

	generated = 0

	for entry in datasets:
		if not isinstance(entry, dict):
			print(f"Skip invalid dataset entry: {entry}")
			continue

		dataset_name = str(entry.get("name", "")).strip()
		if not dataset_name:
			print(f"Skip entry without name: {entry}")
			continue

		dataset_url = entry.get("url")
		dataset_url = str(dataset_url).strip() if dataset_url else None

		filename = str(entry.get("file") or infer_filename(dataset_name, dataset_url)).strip()

		downloaded = False
		if dataset_url:
			dataset_path = binary_dir / filename
			print(f"Downloading {dataset_name} from {dataset_url}...")
			download_with_wget(dataset_url, dataset_path)
			downloaded = True
		else:
			local_dataset_path = local_dir / filename
			dataset_path = binary_dir / filename
			if local_dataset_path.exists():
				shutil.copy2(local_dataset_path, dataset_path)
				print(
					f"No URL for {dataset_name}, copied local file to binary: {dataset_path}"
				)
			else:
				print(
					f"No URL for {dataset_name}, local file not found: {local_dataset_path}"
				)

		if not dataset_path.exists():
			print(f"File not found for {dataset_name}: {dataset_path}. Skip metadata.")
			continue

		line_count, unique_item_count, sequence_info = compute_file_stats(dataset_path)

		quantity_path = quantity_dir / f"{dataset_name}_quantity"
		generate_quantity_file(dataset_path, quantity_path, min_qty=1, max_qty=10)

		dynamic_path = dynamic_dir / f"{dataset_name}_dynamic"
		items = collect_unique_items(dataset_path)
		batch_size = generate_dynamic_weight_file(
			line_count=line_count,
			items=items,
			dynamic_path=dynamic_path,
			min_weight=1,
			max_weight=10,
		)

		metadata = {
			"dataset_name": dataset_name,
			"file_path": str(dataset_path),
			"quantity_file_path": str(quantity_path),
			"dynamic_file_path": str(dynamic_path),
			"batch_size": batch_size,
			"line_count": line_count,
			"unique_item_count": unique_item_count,
			"starts_from": sequence_info["starts_from"],
			"is_contiguous": sequence_info["is_contiguous"],
			"min_item": sequence_info["min_item"],
			"max_item": sequence_info["max_item"],
			"missing_item_count": sequence_info["missing_item_count"],
			"url": dataset_url,
			"downloaded": downloaded,
		}

		metadata_path = output_dir / f"{dataset_name}_metadata.json"
		write_metadata(metadata_path, metadata)
		print(f"Wrote metadata: {metadata_path}")
		generated += 1

	print(f"Done. Generated {generated} metadata file(s).")
	return generated


def main() -> int:
	parser = argparse.ArgumentParser(
		description="Download datasets from YAML and generate per-dataset metadata"
	)
	parser.add_argument("--config", default="to-get.yml", help="Path to YAML config")
	parser.add_argument(
		"--binary-dir",
		default="debug/binary",
		help="Directory where dataset files are stored/downloaded",
	)
	parser.add_argument(
		"--local-dir",
		default="local",
		help="Directory where manual datasets are stored when URL is missing",
	)
	parser.add_argument(
		"--output-dir",
		default="debug/metadata",
		help="Directory where <dataset>_metada.json files are written",
	)
	parser.add_argument(
		"--quantity-dir",
		default="debug/quantity",
		help="Directory where <dataset>_quantity files are written",
	)
	parser.add_argument(
		"--dynamic-dir",
		default="debug/dynamic",
		help="Directory where <dataset>_dynamic files are written",
	)

	args = parser.parse_args()

	config_path = Path(args.config)
	binary_dir = Path(args.binary_dir)
	local_dir = Path(args.local_dir)
	output_dir = Path(args.output_dir)
	quantity_dir = Path(args.quantity_dir)
	dynamic_dir = Path(args.dynamic_dir)

	run_pipeline(
		config_path=config_path,
		binary_dir=binary_dir,
		local_dir=local_dir,
		output_dir=output_dir,
		quantity_dir=quantity_dir,
		dynamic_dir=dynamic_dir,
	)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
