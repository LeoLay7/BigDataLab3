#!/usr/bin/env bash
set -euo pipefail

JOBMANAGER_TARGET="${FLINK_JOBMANAGER_TARGET:-jobmanager:8081}"
JOB_NAME="${FLINK_JOB_NAME:-lab3-kafka-to-postgres-snowflake}"
JOB_JAR="${FLINK_JOB_JAR:-/opt/flink/usrlib/flink-module.jar}"

echo "Waiting for Flink JobManager at ${JOBMANAGER_TARGET}..."
for _ in $(seq 1 60); do
  if flink list -m "${JOBMANAGER_TARGET}" -r >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Cancelling stale jobs named ${JOB_NAME}..."
flink list -m "${JOBMANAGER_TARGET}" -a 2>/dev/null \
  | awk -F ' : ' -v job_name="${JOB_NAME}" 'index($0, job_name) > 0 {print $2}' \
  | while read -r job_id; do
      if [ -n "${job_id}" ]; then
        flink cancel -m "${JOBMANAGER_TARGET}" "${job_id}" >/dev/null 2>&1 || true
      fi
    done

echo "Submitting job ${JOB_NAME}..."
flink run -d -m "${JOBMANAGER_TARGET}" "${JOB_JAR}"

# Keep the container alive for operational visibility in docker compose.
tail -f /dev/null
