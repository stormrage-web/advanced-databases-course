## Результаты: тестирование производительности (MV vs RAW) и нагрузка

### Контекст стенда
- **ClickHouse версия**: см. `clickhouse_version.txt`
- **Состояние Docker**: см. `docker_compose_ps.txt`
- **Объём данных**:
  - `ecom_offers`: см. `rowcount_ecom_offers.txt`
  - `raw_events`: см. `rowcount_raw_events.txt`

### 1) Сравнение времени выполнения: MV vs сырые данные
Файл с полным выводом: **`benchmark_mv_vs_raw.txt`**

Ключевые цифры (p50, чем меньше — тем быстрее):
- **Offers by category**: MV медленнее (на этой выборке агрегат уже “дешёвый”)
- **Events by offer**: MV быстрее примерно **в 2.16x**
- **Events by category (RAW join vs MV)**: MV быстрее примерно **в 1.85x**

> Примечание: ускорение сильно зависит от размера датасета и сложности запроса.

### 2) Нагрузочное тестирование (Python)
Файл с полным выводом: **`load_test_c8_d60.txt`**

Параметры прогона:
- concurrency=8
- duration=60s

Результаты:
- Requests / QPS / latency (avg/p50/p95) — см. файл `load_test_c8_d60.txt`

### 3) Подтверждение мониторинга
- Prometheus targets (health=up): `prometheus_targets.json`
- Первые строки метрик:
  - ClickHouse: `clickhouse_metrics_head.txt`
  - Prometheus: `prometheus_metrics_head.txt`


