#!/bin/bash
set -e

# Default values (can be overridden via environment variables)
AIRFLOW_USERNAME=${AIRFLOW_USERNAME:-admin}
AIRFLOW_PASSWORD=${AIRFLOW_PASSWORD:-admin}
AIRFLOW_EMAIL=${AIRFLOW_EMAIL:-admin@example.com}
AIRFLOW_FIRSTNAME=${AIRFLOW_FIRSTNAME:-Admin}
AIRFLOW_LASTNAME=${AIRFLOW_LASTNAME:-User}

echo "Setting up Airflow with custom credentials..."
echo "Username: ${AIRFLOW_USERNAME}"
echo "Email: ${AIRFLOW_EMAIL}"

# Initialize database if needed (ignore error if already initialized)
airflow db init || true

# Wait a moment for database to be ready
sleep 2

# Check if user already exists and create/update accordingly
if airflow users list 2>/dev/null | grep -q "${AIRFLOW_USERNAME}"; then
    echo "User ${AIRFLOW_USERNAME} already exists. Updating password..."
    airflow users update \
        --username "${AIRFLOW_USERNAME}" \
        --password "${AIRFLOW_PASSWORD}" \
        --email "${AIRFLOW_EMAIL}" \
        --firstname "${AIRFLOW_FIRSTNAME}" \
        --lastname "${AIRFLOW_LASTNAME}" \
        --role Admin || true
else
    echo "Creating new user ${AIRFLOW_USERNAME}..."
    airflow users create \
        --username "${AIRFLOW_USERNAME}" \
        --password "${AIRFLOW_PASSWORD}" \
        --email "${AIRFLOW_EMAIL}" \
        --firstname "${AIRFLOW_FIRSTNAME}" \
        --lastname "${AIRFLOW_LASTNAME}" \
        --role Admin || true
fi

echo "Starting Airflow standalone..."
exec airflow standalone
