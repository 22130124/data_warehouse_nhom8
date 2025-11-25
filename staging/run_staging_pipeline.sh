#!/bin/bash
# =============================================================================
# STAGING LAYER PIPELINE AUTOMATION
# Tác giả: Team Data Warehouse
# Mô tả: Tự động hóa quy trình Nạp (Load) và Biến đổi (Transform) dữ liệu
#        từ File CSV -> Bảng Tạm -> Bảng Chính (Staging DB).
# =============================================================================

# 1. CẤU HÌNH ĐƯỜNG DẪN
# -----------------------------------------------------------------------------
# Đường dẫn file config hệ thống
CONFIG="/opt/dw/staging/config.xml"

# Thư mục chứa các script Python xử lý
BASE_DIR="/opt/dw/staging/load/scripts"

# Đường dẫn kích hoạt môi trường ảo Python (dùng chung với Extract Layer)
VENV="/opt/dw/staging/extract/venv/bin/activate"

# << 2. Xác định ngày hiện tại và source >>
SOURCE_ID="topcv_jobs"    # ID nguồn dữ liệu
TODAY=$(date +%Y-%m-%d)   # Lấy ngày hiện tại (YYYY-MM-DD)

# 2. KÍCH HOẠT MÔI TRƯỜNG
# -----------------------------------------------------------------------------
source $VENV

echo "=================================================="
echo "   STAGING PIPELINE STARTED: $(date)"
echo "   Target Date: $TODAY"
echo "=================================================="

# 3. BƯỚC 1: LOADING (CSV -> TEMP TABLE)
# -----------------------------------------------------------------------------
echo ""
echo "[STEP 1] Running Staging Loader..."
echo "Command: python3 staging_loader.py --source_id $SOURCE_ID --date $TODAY"

python3 $BASE_DIR/staging_loader.py \
    --config $CONFIG \
    --source_id $SOURCE_ID \
    --date $TODAY

# Kiểm tra trạng thái kết thúc của Bước 1 (0 = Thành công)
LOADER_EXIT_CODE=$?

if [ $LOADER_EXIT_CODE -eq 0 ]; then
    echo ">> [SUCCESS] Loader completed successfully."
else
    echo ">> [FAILED] Loader encountered an error (Exit Code: $LOADER_EXIT_CODE)."
    echo ">> Pipeline Aborted."
    deactivate
    exit 1
fi

# 4. BƯỚC 2: TRANSFORMING (TEMP TABLE -> MAIN TABLE)
# -----------------------------------------------------------------------------
echo ""
echo "[STEP 2] Running Staging Transformer..."
echo "Command: python3 staging_transformer.py"

python3 $BASE_DIR/staging_transformer.py \
    --config $CONFIG

# Kiểm tra trạng thái kết thúc của Bước 2
TRANSFORMER_EXIT_CODE=$?

if [ $TRANSFORMER_EXIT_CODE -eq 0 ]; then
    echo ">> [SUCCESS] Transformer completed successfully."
    echo ""
    echo "=================================================="
    echo "   PIPELINE FINISHED SUCCESSFULLY"
    echo "=================================================="
else
    echo ">> [FAILED] Transformer encountered an error (Exit Code: $TRANSFORMER_EXIT_CODE)."
    echo ">> Pipeline Finished with Errors."
    deactivate
    exit 1
fi

# 5. DỌN DẸP & THOÁT
# -----------------------------------------------------------------------------
deactivate
exit 0