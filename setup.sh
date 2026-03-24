#!/usr/bin/env bash
set -euo pipefail

echo "=== Backup Validator Setup ==="

# --- System dependencies ---
echo ""
echo "[1/4] Installing system dependencies..."

if command -v apt-get &> /dev/null; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq python3 python3-pip python3-venv \
        ffmpeg libimage-exiftool-perl libmagic1
elif command -v dnf &> /dev/null; then
    sudo dnf install -y python3 python3-pip ffmpeg perl-Image-ExifTool file-libs
elif command -v pacman &> /dev/null; then
    sudo pacman -S --noconfirm python python-pip ffmpeg perl-image-exiftool file
else
    echo "WARNING: Could not detect package manager. Install manually:"
    echo "  - python3, pip, ffmpeg, exiftool, libmagic"
fi

# --- Python virtual environment ---
echo ""
echo "[2/4] Setting up Python virtual environment..."

VENV_DIR="$(dirname "$0")/.venv"

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install -q --upgrade pip
pip install -q -r "$(dirname "$0")/requirements.txt"

# --- .env file ---
echo ""
echo "[3/4] Checking .env file..."

ENV_FILE="$(dirname "$0")/.env"
if [ ! -f "$ENV_FILE" ]; then
    cp "$(dirname "$0")/.env.example" "$ENV_FILE"
    echo "Created .env from .env.example — edit if needed."
else
    echo ".env already exists, skipping."
fi

# --- Docker (PostgreSQL) ---
echo ""
echo "[4/4] Starting PostgreSQL via Docker..."

docker compose -f "$(dirname "$0")/docker-compose.yml" up -d

echo ""
echo "=== Setup complete ==="
echo ""
echo "Activate the venv:  source .venv/bin/activate"
echo "Usage:              python cli.py --help"
