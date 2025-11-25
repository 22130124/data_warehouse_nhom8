#!/bin/bash
set -euo pipefail

JAR="/opt/dw/staging/loadtowh/scripts/loadtowh.jar"
CONFIG="/opt/dw/staging/config.xml"

# Nếu có tham số thì dùng, nếu không thì lấy ngày hôm nay
RUN_DATE="${1:-$(date +%F)}"

java -jar "$JAR" "$CONFIG" "$RUN_DATE"

