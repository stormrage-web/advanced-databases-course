# Практическая работа №3: ClickHouse для анализа e-commerce данных (Ozon)

## Быстрый старт (Windows / PowerShell)

### 0) Требования
- Docker Desktop + Docker Compose v2
- Python 3.10+ (для генерации/подготовки CSV и бенчмарков)

### 1) Запуск инфраструктуры

```powershell
cd infrastructure\clickhouse
docker compose up -d
docker compose ps
```

Если видите ошибку вида `open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified`, значит **Docker Desktop не запущен**.

Минимальная проверка:

```powershell
docker version
```

## Troubleshooting (частые проблемы)

### Docker Desktop не запущен / нет доступа к сервису
- Откройте **Docker Desktop** (через меню Пуск) и дождитесь статуса **Running**.
- Если `sc start com.docker.service` пишет `Access is denied`, запустите PowerShell **от имени администратора** и выполните:

```powershell
Start-Service com.docker.service
```

После этого снова проверьте:

```powershell
docker version
```

### 2) Подготовка данных (offers + events)

> В репозитории есть только `data/10ozon.csv` (каталог). Таблица `raw_events` заполняется синтетическими событиями, но со связью `ContentUnitID = offer_id` и контролируемым покрытием.

```powershell
python .\scripts\prepare_offers.py
python .\scripts\generate_raw_events.py --coverage 0.70 --avg_events_per_offer 3.0
```

> По умолчанию скрипты используют лимит 2 000 000 offers, чтобы не создавать многогигабайтные файлы.  
> Если вы уже сгенерировали огромные `offers_clean.csv/raw_events.csv`, можно удалить и пересоздать:

```powershell
Remove-Item data\offers_clean.csv, data\raw_events.csv -ErrorAction SilentlyContinue
python .\scripts\prepare_offers.py --limit 2000000
python .\scripts\generate_raw_events.py --limit_offers 2000000 --coverage 0.70 --avg_events_per_offer 3.0
```

### 3) Создание схемы и загрузка данных в ClickHouse

```powershell
.\scripts\ch_apply_schema.ps1
.\scripts\ch_load_data.ps1
```

Проверка, что данные загружены:

```powershell
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM ozon_analytics.ecom_offers"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM ozon_analytics.raw_events"
```

### 4) Запуск аналитических запросов

```powershell
.\scripts\ch_run_queries.ps1
```

### 5) Тестирование производительности (MV vs RAW)

```powershell
python .\scripts\benchmark_mv_vs_raw.py --runs 30 --warmup 10
```

### 6) Нагрузочное тестирование

```powershell
python .\scripts\load_test.py --concurrency 8 --duration 30
```

## Мониторинг

### Prometheus
- UI: `http://localhost:9090`
- Проверка: `Status -> Targets` (должен быть `clickhouse` в состоянии UP)

### Grafana
- UI: `http://localhost:3000` (логин/пароль: `admin/admin`)
- Дашборды: папка `Ozon`
  - `Ozon: Бизнес-метрики`
  - `Ozon: Технические метрики ClickHouse`

## Аутентификация ClickHouse (важно для HTTP)

В образе ClickHouse пароль для `default` может быть включён, из-за чего запросы на `http://localhost:8123/` дадут `403`.
В этой практике добавлен пользователь **`lab` без пароля** (только для локального стенда).

Проверка:

```powershell
curl "http://127.0.0.1:8123/?user=lab" --data "SELECT 1"
```

## Дополнительно: события по категориям (ускорение аналитики событий)

В схеме есть дополнительная agg-таблица `events_by_category_agg` и MV `events_by_category_mv`.  
MV не заполняет историю автоматически, поэтому после первой загрузки данных выполните backfill:

```powershell
cd infrastructure\clickhouse
.\scripts\ch_backfill_events_by_category.ps1
```

## Что скриншотить (подтверждение выполнения)

1) **Инфраструктура**
- вывод `docker compose ps` (видно 3 сервиса UP и порты)
- `http://localhost:8123/ping` в браузере (должно вернуть `Ok.`)

2) **Схема/данные**
- вывод команд `SELECT count() FROM ...` для обеих таблиц
- (опционально) `SHOW TABLES FROM ozon_analytics`

3) **Материализованные представления**
- `SHOW TABLES FROM ozon_analytics` (видно `*_mv` и `*_agg`)

4) **Аналитика**
- вывод `.\scripts\ch_run_queries.ps1` (результаты топов и покрытия)

5) **Производительность**
- вывод `benchmark_mv_vs_raw.py` (avg/p50/p95 и speedup)
- вывод `load_test.py` (QPS и латентности)

6) **Мониторинг**
- Prometheus Targets (UP)
- Grafana: оба дашборда с заполненными панелями


