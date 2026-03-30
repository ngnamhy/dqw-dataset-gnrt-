# DQW Dataset Generator

A pipeline that processes transaction datasets (.dat) and generates quantity, dynamic weight, and metadata files.

## Input

- Configuration in `to-get.yml` under key `datasets`.
- If dataset has `url`: the `.dat` file is downloaded to `debug/binary/`.
- If dataset has no `url`: the pipeline copies the file from `local/` to `debug/binary/`.

## File Types and Meanings

### 1) `.dat` (Binary Transaction Database)

- Default location: `debug/binary/<dataset>.dat`
- Each line represents one transaction.
- Each number in a line represents an item present in that transaction.
- Binary format: only indicates item presence/absence, no quantities.

Example:

```text
1 5 9
2 5
1 3 4 8
```

### 2) `quantity` (Per-Transaction Item Quantities)

- Default location: `debug/quantity/<dataset>_quantity`
- Same number of lines as the `.dat` file.
- Same number of columns per line as the corresponding `.dat` line.
- Each value is a random quantity in range `[1, 10]`.

Meaning: Column i in a line of `quantity` is the quantity of the item in column i of the corresponding `.dat` line.

Example:

```text
4 1 9
2 10
3 8 6 1
```

### 3) `dynamic` (Dynamic Weight Per Batch)

<<<<<<< Updated upstream
- Vi tri mac dinh: `debug/dynamic/<dataset>_dynamic`
=======
- Default location: `debug/dynamic/<dataset>_dynamic`
- Batch size is calculated as:

$$
\text{batch\_size} = \lfloor \sqrt{\text{line\_count}} \rfloor
$$
>>>>>>> Stashed changes

- Each line in the dynamic file has 4 numbers:

```text
<item> <start_batch_line> <end_batch_line> <weight>
```

- `weight` is randomly generated in `[1, 10]` for each item in each batch.

Example line:

```text
0 1 20 4
```

Meaning: From line 1 to line 20, item `0` has weight `4`.

### 4) `metadata` (Summary Information)

- Default location: `debug/metadata/<dataset>_metada.json`
- Contains key information about the dataset and generated files:

- `dataset_name`
- `file_path`
- `quantity_file_path`
- `dynamic_file_path`
- `batch_size`
- `line_count`
- `unique_item_count`
- `starts_from` (0, 1, or null)
- `is_contiguous` (whether items form a contiguous sequence from 0/1 to max)
- `min_item`
- `max_item`
- `missing_item_count`
- `url`
- `downloaded`

## Running the Pipeline

```bash
python3 pipeline.py
```

Default parameters:

- `--config to-get.yml`
- `--binary-dir debug/binary`
- `--local-dir local`
- `--output-dir debug/metadata`
- `--quantity-dir debug/quantity`
- `--dynamic-dir debug/dynamic`

