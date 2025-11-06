#!/usr/bin/env bash
set -euo pipefail

# ====== cấu hình nhanh (có thể đổi khi chạy) ======
API_URL="${API_URL:-http://127.0.0.1:8100}"     # FastAPI Inference
IMAGES_DIR="${IMAGES_DIR:-data/samples}"         # thư mục chứa ảnh
PRODUCT="${PRODUCT:-PCB_A}"                      # mã sản phẩm
STATION="${STATION:-ST01}"                       # station_id map tới runner
BOARD_SERIAL_PREFIX="${BOARD_SERIAL_PREFIX:-DEMO}" # prefix serial
SLEEP_BETWEEN="${SLEEP_BETWEEN:-0.2}"            # giãn cách gửi (giây)
LOG_JSONL="${LOG_JSONL:-data/processed/demo_infer_responses.jsonl}"

mkdir -p "$(dirname "$LOG_JSONL")"

# Kiểm tra health
echo ">>> Checking API health: $API_URL/healthz"
curl -sS "$API_URL/healthz" | python -m json.tool || true
echo

# Gửi lần lượt từng ảnh
echo ">>> Sending images from: $IMAGES_DIR"
i=0
# lọc các phần mở rộng phổ biến
shopt -s nullglob
for f in "$IMAGES_DIR"/*.{jpg,JPG,jpeg,JPEG,png,PNG,bmp,BMP}; do
  i=$((i+1))
  serial="${BOARD_SERIAL_PREFIX}_$(printf "%04d" "$i")"
  echo "[$i] -> $f  (product=$PRODUCT, station=$STATION, serial=$serial)"
  # POST multipart
  resp=$(curl -sS -X POST "${API_URL}/v1/infer" \
      -F "image=@${f}" \
      -F "product_code=${PRODUCT}" \
      -F "station_id=${STATION}" \
      -F "board_serial=${serial}")
  # in đẹp + ghi log jsonl
  echo "$resp" | python -m json.tool || true
  echo "$resp" >> "$LOG_JSONL"
  sleep "$SLEEP_BETWEEN"
done

echo ">>> Done. Responses saved to $LOG_JSONL"
