#!/usr/bin/env bash
set -e

# =========================
# Config
# =========================

LOG_DIR="./logs"
PID_DIR="./pids"

mkdir -p "${LOG_DIR}" "${PID_DIR}"

# Spark jobs (order matters)
JOB_NAMES=(
  "bronze_listener"
  "silver_listener"
  "silver_embedding"
  "gold_listener"
)

JOB_SCRIPTS=(
  "./spark_jobs/bronze_streaming.py"
  "./spark_jobs/silver_streaming.py"
  "./spark_jobs/silver_embedding_async.py"
  "./spark_jobs/gold_streaming.py"
)


# =========================
# Helpers
# =========================

is_pid_running () {
  local pid=$1
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

# stop_pid_if_running () {
#   local pid=$1
#   local name=$2

#   if is_pid_running "${pid}"; then
#     echo "🛑 Stopping ${name} (PID ${pid})"
#     kill "${pid}"
#     sleep 2

#     if is_pid_running "${pid}"; then
#       echo "⚠️  Force killing ${name} (PID ${pid})"
#       kill -9 "${pid}"
#     fi
#   fi
# }

start_job () {
  local name=$1
  local script=$2
  local pid_file="${PID_DIR}/${name}.pid"
  local log_file="${LOG_DIR}/${name}.log"

  # -------------------------
  # PID check
  # -------------------------
  if [[ -f "${pid_file}" ]]; then
    old_pid=$(cat "${pid_file}")

    if is_pid_running "${old_pid}"; then
      echo "⚠️  ${name} already running (PID ${old_pid}) — skipping"
      return
    else
      echo "🧹 Removing stale PID for ${name}"
      rm -f "${pid_file}"
    fi
  fi

  # -------------------------
  # Start job
  # -------------------------
  echo "🚀 Starting ${name}..."
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3 \
    "${script}" \
    > "${log_file}" 2>&1 &

  new_pid=$!
  echo "${new_pid}" > "${pid_file}"

  echo "✅ ${name} started (PID ${new_pid})"
}

# =========================
# Start pipeline
# =========================

echo "==============================="
echo "🚀 Starting Spark listeners"
echo "==============================="

i=0
while [ $i -lt ${#JOB_NAMES[@]} ]; do
  start_job "${JOB_NAMES[$i]}" "${JOB_SCRIPTS[$i]}"
  sleep 5
  i=$((i + 1))
done

echo ""
echo "📊 Active jobs:"
for pidfile in $(ls "$PID_DIR"/*.pid 2>/dev/null); do
  name=$(basename "$pidfile" .pid)
  pid=$(cat "$pidfile")

  if is_pid_running "$pid"; then
    echo "  ✅ $name (PID $pid)"
  else
    echo "  ❌ $name (stale PID $pid)"
  fi
done

echo ""
echo "🎉 All listeners processed"
