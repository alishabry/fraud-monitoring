#!/bin/bash

set -e

# Функция ожидания сервиса
wait_for_service() {
    local host="$1"
    local port="$2"
    local service="$3"

    echo "Waiting for ${service} at ${host}:${port}..."
    while ! nc -z "${host}" "${port}"; do
        sleep 1
    done
    echo "${service} is ready!"
}

# Ждем PostgreSQL если используется
if [[ "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-}" == *"postgres"* ]]; then
    DB_HOST="postgres"
    DB_PORT="5432"
    wait_for_service "${DB_HOST}" "${DB_PORT}" "PostgreSQL"
fi

# Ждем Redis если используется Celery
if [[ "${AIRFLOW__CELERY__BROKER_URL:-}" == *"redis"* ]]; then
    REDIS_HOST="redis"
    REDIS_PORT="6379"
    wait_for_service "${REDIS_HOST}" "${REDIS_PORT}" "Redis"
fi

# Инициализируем и обновляем базу данных
echo "Setting up Airflow database..."
airflow db init 2>/dev/null || true  # Игнорируем если уже инициализировано
airflow db upgrade

# Создаем администратора если его нет
echo "Setting up admin user..."
airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname "${ADMIN_FIRSTNAME:-Admin}" \
    --lastname "${ADMIN_LASTNAME:-User}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
    2>/dev/null || true  # Игнорируем если пользователь уже существует

# Проверяем, является ли команда специальной командой Airflow
# Список команд, которые нужно выполнять через "airflow"
AIRFLOW_COMMANDS="webserver scheduler triggerer standalone version info dag-processor"

# Проверяем первый аргумент
if [[ -n "$1" ]] && [[ "${AIRFLOW_COMMANDS}" =~ (^|[[:space:]])"$1"($|[[:space:]]) ]]; then
    # Выполняем команду через airflow
    exec airflow "$@"
elif [[ "$1" == "celery" ]]; then
    # Обработка celery команд
    if [[ "$2" == "worker" ]]; then
        exec airflow celery worker "${@:3}"
    elif [[ "$2" == "flower" ]]; then
        exec airflow celery flower "${@:3}"
    else
        exec airflow "$@"
    fi
else
    # Любая другая команда выполняется как есть
    exec "$@"
fi
