# DQW Dataset Generator

Pipeline nay xu ly dataset giao dich (.dat) va sinh them quantity, dynamic, metadata.

## Dau vao

- Cau hinh trong `to-get.yml`, key `datasets`.
- Neu dataset co `url`: file `.dat` duoc tai ve `debug/binary/`.
- Neu dataset khong co `url`: pipeline copy file tu `local/` sang `debug/binary/`.

## Y nghia tung loai file

### 1) `.dat` (binary transaction database)

- Vi tri mac dinh: `debug/binary/<dataset>.dat`
- Moi dong la 1 transaction.
- Moi so trong dong la 1 item xuat hien trong transaction do.
- Day la du lieu nhi phan theo item (co/khong co item), chua co so luong.

Vi du:

```text
1 5 9
2 5
1 3 4 8
```

### 2) `quantity` (so luong theo transaction)

- Vi tri mac dinh: `debug/quantity/<dataset>_quantity`
- So dong giong file `.dat`.
- So cot moi dong cung giong file `.dat` tuong ung.
- Moi gia tri la so luong ngau nhien trong khoang `[1, 10]`.

Y nghia: cot thu i trong 1 dong cua `quantity` la so luong cua item cot thu i trong dong `.dat` tuong ung.

Vi du:

```text
4 1 9
2 10
3 8 6 1
```

### 3) `dynamic` (dynamic weight theo batch)

- Vi tri mac dinh: `debug/dynamic/<dataset>_dynamic`
- Batch size duoc tinh:

$$
	ext{batch\_size} = \lfloor \sqrt{\text{so\_dong}} \rfloor
$$

- Moi dong file dynamic co 4 so:

```text
<item> <start_batch_line> <end_batch_line> <weight>
```

- `weight` duoc sinh ngau nhien trong `[1, 10]` cho tung item trong tung batch.

Vi du dong:

```text
0 1 20 4
```

Nghia la: tu dong 1 den dong 20, item `0` co weight bang `4`.

### 4) `metadata` (thong tin tom tat)

- Vi tri mac dinh: `debug/metadata/<dataset>_metada.json`
- Chua cac thong tin chinh cua dataset va cac file sinh ra, gom:

- `dataset_name`
- `file_path`
- `quantity_file_path`
- `dynamic_file_path`
- `batch_size`
- `line_count`
- `unique_item_count`
- `starts_from` (0, 1 hoac null)
- `is_contiguous` (tap item co lien tuc tu 0/1 den max hay khong)
- `min_item`
- `max_item`
- `missing_item_count`
- `url`
- `downloaded`

## Chay pipeline

```bash
python3 pipeline.py
```

Tham so mac dinh:

- `--config to-get.yml`
- `--binary-dir debug/binary`
- `--local-dir local`
- `--output-dir debug/metadata`
- `--quantity-dir debug/quantity`
- `--dynamic-dir debug/dynamic`

