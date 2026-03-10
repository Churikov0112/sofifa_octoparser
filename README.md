# sofifa_octoparser

Набор скриптов для обработки данных о чемпионатах, клубах и футболистах. Источник данных по SOFIFA — результат веб‑скраппинга через Octoparse 8. Дополнительно подтягиваются данные Transfermarkt через локально запущенный API.

## Что получается на выходе

- `sofifa_football_data.json` — единый JSON с лигами, командами и игроками.
- Папки с изображениями:
  - `sofifa_competitions/` — логотипы лиг
  - `sofifa_teams/` — логотипы команд
  - `sofifa_players/` — изображения игроков

## Требования

- Python 3.10+ (подойдет любой современный Python 3)
- Установленные зависимости Python: `requests`
- Octoparse 8 (для запуска task)
- Локально запущенный `transfermarkt-api`

Установка зависимости:

```bash
pip install requests
```

## Данные из Octoparse

1. В Octoparse 8 импортировать task из папки `sofifa_octoparse_task`.
2. Запустить task и дождаться выгрузки результата в Excel.
3. Конвертировать Excel → JSON через любой онлайн‑конвертер.
4. Положить полученный JSON в корень проекта с именем `sofifa_raw_data.json`.

## Запуск transfermarkt-api

Склонировать и запустить API:

```bash
git clone https://github.com/Churikov0112/transfermarkt-api
cd transfermarkt-api
# запуск по инструкции репозитория
```

API должен быть доступен локально по адресу `http://localhost:8000`.

## Пайплайн скриптов

Все скрипты запускаются по очереди, из корня проекта:

```bash
python 1_sofifa_extract_ids.py
python 2_sofifa_split_leagues_teams_players.py
python 3_sofifa_load_images.py
python 4_sofifa_tm_market_values_v3.py
python 5_tm_competitions.py
python 6_tm_teams_and_players.py
python 7_sofifa_tm_players_merge.py
python 8_sofifa_merge_to_one_file.py
```

Что делает каждый шаг:

- `1_sofifa_extract_ids.py` — добавляет ID из URL‑ов (SOFIFA и Transfermarkt).
- `2_sofifa_split_leagues_teams_players.py` — разбивает сырой JSON на лиги, команды, игроков и рейтинги.
- `3_sofifa_load_images.py` — скачивает изображения лиг, команд и игроков.
- `4_sofifa_tm_market_values_v3.py` — получает историю рыночной стоимости игроков из Transfermarkt API.
- `5_tm_competitions.py` — получает список клубов в лигах.
- `6_tm_teams_and_players.py` — получает составы команд и данные игроков.
- `7_sofifa_tm_players_merge.py` — матчинг игроков SOFIFA и Transfermarkt.
- `8_sofifa_merge_to_one_file.py` — сборка единого `sofifa_football_data.json`.

## Примечания

- Скрипты `5_tm_competitions.py` и `6_tm_teams_and_players.py` используют параметры сезона и лиг, заданные внутри файлов (см. блок `main`).
- Скрипт `4_sofifa_tm_market_values_v3.py` устойчив к перезапуску: прогресс сохраняется в `sofifa_tm_market_values.json`.
- Если какие‑то изображения отсутствуют, скрипт `3_sofifa_load_images.py` пропускает заглушки.
