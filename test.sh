set -eu
cd "$(dirname "$0")"
project="cinema-test-$(date +%s)-$$"

if docker compose version >/dev/null 2>&1; then
    compose() { docker compose -p "$project" --profile test "$@"; }
elif command -v docker-compose >/dev/null 2>&1; then
    compose() { docker-compose -p "$project" --profile test "$@"; }
else
    echo "Для интеграционных тестов нужен Docker с Compose." >&2
    exit 1
fi

export MOVIE_PORT=0 REGISTRY_PORT=0 CLICKHOUSE_PORT=0 AGG_PORT=0 GRAFANA_PORT=0 MINIO_PORT=0 MINIO_CONSOLE_PORT=0
export GENERATOR_ENABLED=false SCHEDULER_ENABLED=true AGG_INTERVAL_SECONDS=3

cleanup() {
    status=$?
    trap - EXIT INT TERM
    if [ "$status" -ne 0 ]; then
        compose logs --no-color --tail=25 >&2 || true
    fi
    compose down --volumes --remove-orphans || true
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

compose build
compose run --rm tests
compose stop kafka-3
compose run --rm --no-deps tests python -m unittest -v tests.outages.BrokerDown
compose start kafka-3
compose stop postgres
compose run --rm --no-deps tests python -m unittest -v tests.outages.PostgresDown
compose start postgres
compose run --rm --no-deps tests python -m unittest -v tests.outages.PostgresRecovered
compose stop minio
compose run --rm --no-deps tests python -m unittest -v tests.outages.MinioDown
compose start minio
compose run --rm --no-deps tests python -m unittest -v tests.outages.MinioRecovered
