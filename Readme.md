# Backup Validator

Goal: Ensure no photo or video is lost when consolidating 3 external hard drives into a single disk.

## The Problem

Consolidating ~1.5 TB of files from 3 different HDs (after dedup with czkawka) carries two risks:

- **Missing files** — something was left out during the copy
- **Corrupted files** — the file exists but won't open or play correctly

Backup Validator indexes all disks into a PostgreSQL database, cross-references via SHA-256, and validates the integrity of every media file using ffprobe, exiftool, and PIL.

## How It Works

```
┌─────────┐   ┌─────────┐   ┌─────────┐
│  HD-1   │   │  HD-2   │   │  HD-3   │    (sources)
└────┬────┘   └────┬────┘   └────┬────┘
     │             │             │
     └──────┬──────┴──────┬──────┘
            │  Index (SHA-256)   │
            ▼                    ▼
      ┌──────────┐     ┌──────────────────┐
      │ PostgreSQL│◄────│ Consolidated Disk│
      └─────┬────┘     └──────────────────┘
            │
    ┌───────┼──────────┐
    ▼       ▼          ▼
 Compare  Integrity   Report
 (cross-  (ffprobe,   (CSV/JSON/
  ref)    exiftool,    table)
           PIL)
```

**Pipeline:**

1. **Index** — Discovers files via `os.walk` and computes SHA-256 hashes in parallel (multiprocessing). Fully resumable.
2. **Integrity** — Validates media: `ffprobe` (quick) or `ffmpeg decode` (deep) for videos, `exiftool` + `PIL` for images.
3. **Compare** — Cross-references source HD hashes against the consolidated disk. Identifies missing files, corrupted files, and residual duplicates.
4. **Report** — Generates a filtered report by status and media type in CSV, JSON, or table format.

## Quick Start

### Prerequisites

- Python 3.10+
- Docker (for PostgreSQL)
- ffmpeg, exiftool, libmagic

### Installation

```bash
git clone <repo-url> && cd backups

# Automated setup (installs deps, creates venv, starts PostgreSQL)
./setup.sh
source .venv/bin/activate
```

### Usage

```bash
# 1. Index the source hard drives
python cli.py index /mnt/hd1 --name HD-1 --role source
python cli.py index /mnt/hd2 --name HD-2 --role source
python cli.py index /mnt/hd3 --name HD-3 --role source

# 2. Index the consolidated disk
python cli.py index /mnt/consolidated --name consolidated --role consolidated

# 3. Check integrity (--deep for full video decode)
python cli.py check-integrity --source consolidated --deep

# 4. Compare sources vs consolidated
python cli.py compare --consolidated consolidated

# 5. Generate report
python cli.py report --format csv --media-only -o report.csv
```

## Commands

| Command | Description |
|---|---|
| `index` | Index a disk: discover files and compute SHA-256 hashes |
| `check-integrity` | Validate media file integrity (ffprobe, exiftool, PIL) |
| `validate` | Validate a single file (no database required) |
| `compare` | Cross-reference source HDs against the consolidated disk |
| `find-valid-copy` | Locate a valid copy of a corrupted file in source HDs |
| `report` | Generate validation report (CSV, JSON, table) |

Run `python cli.py <command> --help` to see all available options.

## Project Structure

```
.
├── cli.py                 # CLI entry point (Click)
├── src/
│   ├── config.py          # Configuration and constants
│   ├── db.py              # PostgreSQL connection (psycopg2)
│   ├── indexer.py         # File discovery + SHA-256 hashing
│   ├── integrity.py       # Media validation (ffprobe/exiftool/PIL)
│   ├── comparator.py      # Cross-reference and find-valid-copy
│   ├── reporter.py        # Report generation
│   └── models.py          # Dataclasses
├── init.sql               # Database schema (4 tables)
├── docker-compose.yml     # PostgreSQL 16 Alpine
├── setup.sh               # Automated setup script
└── requirements.txt       # Python dependencies
```

## Database Schema

| Table | Purpose |
|---|---|
| `sources` | Registered disks (name, path, role: source/consolidated) |
| `files` | Indexed files (path, size, SHA-256, media type) |
| `integrity_checks` | Check results per file (ffprobe, exiftool, pil, ffmpeg_deep) |
| `comparison_results` | Status of each source file in the consolidated disk (found/missing/corrupted) |

## Stack

- **Python 3** + Click (CLI) + tqdm (progress bars)
- **PostgreSQL 16** via Docker
- **ffmpeg/ffprobe** — video validation
- **exiftool** — image metadata parsing
- **Pillow** — image decoding

## License

MIT
