# Televote

Бэкенд для минутных опросов в телеэфире. Администратор задаёт вопрос, варианты и время голосования. Зритель может выбрать один или несколько ответов и поменять выбор до конца эфира. Сервис предоставляет HTTP API для страницы голосования по ссылке или QR-коду.

| Кто | Запрос | Результат |
|---|---|---|
| Администратор | `POST /admin/polls` | ID созданного опроса |
| Зритель | `GET /api/polls/{id}` | Вопрос и варианты в JSON, cookie для голосования |
| Зритель | `POST /api/polls/{id}/votes` | `202 Accepted` после записи в Kafka |
| Администратор | `GET /admin/polls/{id}/results` | Статус опроса, а после подсчёта — обезличенные результаты |

У каждого браузера своя cookie на опрос. Если зритель проголосует дважды, в итог войдёт его последний допустимый выбор по порядку записи в Kafka. Регистрации нет, поэтому после очистки cookie или в другом браузере можно проголосовать снова. [Контракт OpenAPI](api/openapi.yaml)

## Локальный запуск

Понадобятся Git, Go 1.26+, Docker с Compose v2 и свободные порты 55432, 59092 и 8080. Compose запускает PostgreSQL и Kafka. На новом томе PostgreSQL сам применит миграции.

```sh
git clone https://github.com/rococo4/televote.git
cd televote
docker compose -f test-env/compose.yaml up -d --wait
ADMIN_TOKEN=local-editor go run ./cmd/televote
```

Оставьте сервис запущенным. Для ручной проверки откройте второй терминал с curl и Python 3. Административные запросы требуют заголовок `Authorization: Bearer <токен>`.

### Проверить голосование через curl

Создайте A/B-опрос на ближайшую минуту. У варианта A будет ID 1, у B — 2.

```sh
export ADMIN_TOKEN=local-editor
BASE=http://127.0.0.1:8080
POLL_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
BODY=$(python3 - "$POLL_ID" <<'PYTHON'
import json, sys
from datetime import datetime, timedelta, timezone
now = datetime.now(timezone.utc)
print(json.dumps(dict(id=sys.argv[1], question="Какой вариант выбираете?", choices=["A","B"],
                      min_choices=1, max_choices=1, opens_at=now.isoformat(),
                      closes_at=(now + timedelta(minutes=1)).isoformat())))
PYTHON
)
curl -i "$BASE/admin/polls" -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' -d "$BODY"
```

Сервер вернёт `201` и ID опроса. Если повторить запрос с тем же ID и параметрами, вернётся тот же опрос. При других параметрах занятый ID даст `400`. Поле `id` можно опустить, тогда сервер назначит UUID сам. Для нескольких ответов увеличьте `max_choices`.

Откройте вопрос, сохраните cookie и проголосуйте сначала за A, затем за B. Оба POST должны вернуть `202`.

```sh
VOTER_COOKIES=$(mktemp)
curl -sS -c "$VOTER_COOKIES" "$BASE/api/polls/$POLL_ID"
curl -i -b "$VOTER_COOKIES" "$BASE/api/polls/$POLL_ID/votes" -H 'Content-Type: application/json' \
  -d '{"choices":[1]}'
curl -i -b "$VOTER_COOKIES" "$BASE/api/polls/$POLL_ID/votes" -H 'Content-Type: application/json' \
  -d '{"choices":[2]}'
```

Через минуту проверьте отчёт. Если статус ещё `finalizing`, повторите запрос.

```sh
curl -fsS "$BASE/admin/polls/$POLL_ID/results" -H "Authorization: Bearer $ADMIN_TOKEN"
```

Ожидаемый итог — `final`, один бюллетень, за A — 0 голосов, за B — 1. При множественном выборе процент каждого варианта считается от числа бюллетеней, поэтому сумма процентов может превысить 100. Без токена отчёт вернёт `401`.

Второй экземпляр можно запустить командой `HTTP_ADDR=127.0.0.1:8081 ADMIN_TOKEN=local-editor go run ./cmd/televote`. Созданный на порту 8080 опрос будет доступен и на 8081. Для ручных экспериментов команда `POLL_ID=$(sh test-env/seed-demo.sh)` из корня репозитория создаёт новый A/B-опрос на час.

## Подсчёт и просмотр результата

![Путь голоса от HTTP API через Kafka до отчёта в PostgreSQL](architecture.png)

У каждого опроса свой топик Kafka. По ID опроса и cookie сервис выбирает партицию. Если зритель меняет ответ через другую реплику, обе записи всё равно попадут в одну партицию. В конце эфира каждая реплика прекращает приём и отмечает в PostgreSQL, что закончила отправку. Когда отметились все реплики или истёк общий срок ожидания, сервис записывает в каждую партицию маркер `CLOSE`.

Обработчик считает голоса до первого `CLOSE`. Если реплика задержалась и Kafka записала её голос после маркера, зритель может получить `202`, но в итог этот голос уже не войдёт. При сбое Kafka передаст партицию другому обработчику. Он перечитает её с начала, если итог партиции ещё не сохранён. Когда готовы все партиции, в PostgreSQL появится общий результат. [Закрытие опроса и восстановление после сбоя](DESIGN.md#4-где-заканчивается-минута)

<details>
<summary>Проверить решение о закрытии и суммы в PostgreSQL</summary>

```sh
docker compose -f test-env/compose.yaml exec -T postgres psql -U televote -d televote -v poll_id="$POLL_ID" <<'SQL'
SELECT closes_at, grace_us, decision_reason, decided_at
FROM poll_closures WHERE poll_id = :'poll_id';

SELECT partition_id, end_offset, ballots, counts
FROM poll_partition_results WHERE poll_id = :'poll_id' ORDER BY partition_id;

SELECT ballots, counts FROM poll_results WHERE poll_id = :'poll_id';
SQL
```

Для примера с curl последняя выборка вернёт `ballots = 1` и `counts = {"1": 0, "2": 1}`.

</details>

## Настройки

| Переменная | По умолчанию |
|---|---|
| `DATABASE_URL` | `postgres://televote:televote@127.0.0.1:55432/televote?sslmode=disable` |
| `KAFKA_BROKERS` | `127.0.0.1:59092` |
| `HTTP_ADDR` | `127.0.0.1:8080` |
| `ADMIN_TOKEN` | Пустое значение закрывает административные маршруты |
| `POLL_PARTITIONS` | `4` партиции для нового опроса |
| `POLL_CLOSE_GRACE_PERIOD` | `5s` на завершение отправок после конца эфира |

Срок ожидания хранится в PostgreSQL и одинаков для всех реплик, включая перезапущенные. Изменение настройки действует на опросы, для которых срок ещё не сохранён. Допустимы неотрицательные длительности с точностью до микросекунды, например `1500ms`. При `0` можно закрывать опрос сразу в `closes_at`.

## Тесты

Для быстрой проверки запустите зависимости через Compose. Эта команда создаст опрос, отправит голоса через две реплики и сверит итог.

```sh
go test -race -tags=e2e -run '^TestPollLifecycle$' -count=1 -v ./tests/e2e
```

Запускайте тесты из корня репозитория. Для интеграционных, сквозных и нагрузочных тестов оставьте PostgreSQL и Kafka включёнными. Опросы и HTTP-серверы тесты создадут сами. Для замера времени запускайте нагрузочные прогоны по одному.

```sh
go test -race -count=1 ./...
go test -race -tags=integration -count=1 ./internal/adapter/kafka ./internal/repository/... ./tests/integration
go test -race -tags=e2e -count=1 ./tests/e2e
go test -tags=integration,load -run '^TestSubmitVoteLoad$' -count=1 -v -timeout=3m ./tests/integration
go test -tags=e2e,load -run '^TestTwoProcessVotingLoad$' -count=1 -v -timeout=2m ./tests/e2e
```

- **Одна реплика, 30 000 POST.** В первом контрольном прогоне все запросы получили `202`. Во втором генератор пропустил 160 отправок на пике. Все 29 840 отправленных получили `202` и нашлись в Kafka. [Условия теста, исходный сбой и результаты](reports/load-test.md)
- **Две реплики за балансировщиком.** В обоих прогонах 2 000 зрителей получили cookie, проголосовали, а 200 из них изменили выбор. Итог в каждом случае — 1 800 за A и 200 за B. [Распределение запросов и задержки](reports/replica-load.md)

Для 100 млн зрителей за минуту расчётный пик с учётом повторов — **3,67 млн POST/с**. Такую нагрузку на кластере ещё предстоит проверить. [Расчёт нагрузки и выбор архитектуры](DESIGN.md)

## Остановка стенда

Остановите сервис через Ctrl+C и выполните `docker compose -f test-env/compose.yaml down`. Данные сохранятся в именованных томах. Команда `docker compose -f test-env/compose.yaml down -v` удалит локальные опросы, результаты и топики с голосами. При следующем запуске PostgreSQL применит миграции заново.

## Материалы проекта

[Структура Go-проекта и путь запроса](docs/project-layout.md) · [Журнал работы с ИИ](AI.md)
