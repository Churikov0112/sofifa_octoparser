import json
import os
import requests
from urllib.parse import urlparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def download_image(url, filepath, max_retries=5, retry_delay=1):
    """Скачивает изображение и сохраняет по указанному пути с повторными попытками"""
    # Проверяем, не является ли URL заглушкой
    if url == "https://cdn.sofifa.net/player_0.svg":
        return False, "Пропуск: изображение-заглушка"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            # Создаем папку если не существует
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            with open(filepath, 'wb') as f:
                f.write(response.content)

            return True, filepath

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:  # Последняя попытка
                return False, f"Ошибка при скачивании {url} после {max_retries} попыток: {e}"

            # Ждем перед следующей попыткой
            time.sleep(retry_delay * (attempt + 1))

        except Exception as e:
            return False, f"Неожиданная ошибка при скачивании {url}: {e}"

    return False, f"Не удалось скачать {url} после {max_retries} попыток"


def get_file_extension(url):
    """Извлекает расширение файла из URL"""
    parsed_url = urlparse(url)
    path = parsed_url.path
    if '.' in path:
        return os.path.splitext(path)[1]
    return '.png'  # расширение по умолчанию


def download_leagues_images(leagues_file, output_dir='leagues'):
    """Скачивает логотипы лиг"""
    print("📥 Скачивание логотипов лиг...")

    with open(leagues_file, 'r', encoding='utf-8') as f:
        leagues = json.load(f)

    success_count = 0
    error_count = 0
    skip_count = 0

    for league in leagues:
        logo_url = league.get('league_logo_url')
        league_id = league.get('league_id')

        if not logo_url or not league_id:
            print(f"⚠️  Пропуск лиги {league.get('league_name')}: отсутствует URL или ID")
            error_count += 1
            continue

        # Проверяем заглушку (хотя для лиг вряд ли будет, но на всякий случай)
        if logo_url == "https://cdn.sofifa.net/player_0.svg":
            print(f"⏭️  Пропуск лиги {league['league_name']}: изображение-заглушка")
            skip_count += 1
            continue

        extension = get_file_extension(logo_url)
        filename = f"{league_id}{extension}"
        filepath = os.path.join(output_dir, filename)

        success, result = download_image(logo_url, filepath)
        if success:
            print(f"✅ Лига {league['league_name']}: {filename}")
            success_count += 1
        else:
            if "Пропуск: изображение-заглушка" in result:
                print(f"⏭️  Лига {league['league_name']}: изображение-заглушка")
                skip_count += 1
            else:
                print(f"❌ Ошибка лиги {league['league_name']}: {result}")
                error_count += 1

        time.sleep(0.1)  # небольшая задержка между запросами

    print(f"📊 Лиги: Успешно {success_count}, Пропущено {skip_count}, Ошибок {error_count}")
    return success_count, skip_count, error_count


def download_teams_images(teams_file, output_dir='teams'):
    """Скачивает логотипы команд"""
    print("\n📥 Скачивание логотипов команд...")

    with open(teams_file, 'r', encoding='utf-8') as f:
        teams = json.load(f)

    success_count = 0
    error_count = 0
    skip_count = 0

    for team in teams:
        logo_url = team.get('team_logo_url')
        team_id = team.get('team_id')

        if not logo_url or not team_id:
            print(f"⚠️  Пропуск команды {team.get('team_name')}: отсутствует URL или ID")
            error_count += 1
            continue

        # Проверяем заглушку
        if logo_url == "https://cdn.sofifa.net/player_0.svg":
            print(f"⏭️  Пропуск команды {team['team_name']}: изображение-заглушка")
            skip_count += 1
            continue

        extension = get_file_extension(logo_url)
        filename = f"{team_id}{extension}"
        filepath = os.path.join(output_dir, filename)

        success, result = download_image(logo_url, filepath)
        if success:
            print(f"✅ Команда {team['team_name']}: {filename}")
            success_count += 1
        else:
            if "Пропуск: изображение-заглушка" in result:
                print(f"⏭️  Команда {team['team_name']}: изображение-заглушка")
                skip_count += 1
            else:
                print(f"❌ Ошибка команды {team['team_name']}: {result}")
                error_count += 1

        time.sleep(0.1)  # небольшая задержка между запросами

    print(f"📊 Команды: Успешно {success_count}, Пропущено {skip_count}, Ошибок {error_count}")
    return success_count, skip_count, error_count


def download_players_images_parallel(players_file, output_dir='players', max_workers=10):
    """Скачивает изображения игроков с использованием многопоточности"""
    print("\n📥 Скачивание изображений игроков (многопоточное)...")

    with open(players_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    # Подготовка задач для многопоточного выполнения
    tasks = []
    skip_count = 0

    for player in players:
        image_url = player.get('image_url')
        player_id = player.get('id')
        player_name = player.get('name', 'Unknown')

        if not image_url or not player_id:
            continue

        # Проверяем заглушку
        if image_url == "https://cdn.sofifa.net/player_0.svg":
            skip_count += 1
            continue

        extension = get_file_extension(image_url)
        filename = f"{player_id}{extension}"
        filepath = os.path.join(output_dir, filename)

        tasks.append((image_url, filepath, player_name, player_id))

    print(f"⏭️  Пропущено игроков с заглушками: {skip_count}")

    # Многопоточное выполнение
    success_count = 0
    error_count = 0
    completed_count = 0
    total_count = len(tasks)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Создаем future для каждой задачи
        future_to_task = {
            executor.submit(download_image, url, path): (name, pid, url)
            for url, path, name, pid in tasks
        }

        # Обрабатываем завершенные задачи
        for future in as_completed(future_to_task):
            player_name, player_id, url = future_to_task[future]
            completed_count += 1

            try:
                success, result = future.result()
                if success:
                    success_count += 1
                    if completed_count % 50 == 0:  # Периодический вывод прогресса
                        print(f"📊 Прогресс: {completed_count}/{total_count} игроков")
                else:
                    if "Пропуск: изображение-заглушка" in result:
                        skip_count += 1
                    else:
                        error_count += 1
                        print(f"❌ Ошибка игрока {player_name}: {result}")
            except Exception as e:
                error_count += 1
                print(f"❌ Исключение у игрока {player_name}: {e}")

    print(f"📊 Игроки: Успешно {success_count}, Пропущено {skip_count}, Ошибок {error_count}")
    return success_count, skip_count, error_count


def download_players_images_sequential(players_file, output_dir='players'):
    """Скачивает изображения игроков последовательно (альтернатива)"""
    print("\n📥 Скачивание изображений игроков (последовательно)...")

    with open(players_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    success_count = 0
    error_count = 0
    skip_count = 0
    total_count = len(players)

    for i, player in enumerate(players, 1):
        image_url = player.get('image_url')
        player_id = player.get('id')
        player_name = player.get('name', 'Unknown')

        if not image_url or not player_id:
            print(f"⚠️  Пропуск игрока {player_name}: отсутствует URL или ID")
            error_count += 1
            continue

        # Проверяем заглушку
        if image_url == "https://cdn.sofifa.net/player_0.svg":
            if i % 100 == 0:  # Периодический вывод информации о пропусках
                print(f"⏭️  Пропуск игрока {player_name}: изображение-заглушка")
            skip_count += 1
            continue

        extension = get_file_extension(image_url)
        filename = f"{player_id}{extension}"
        filepath = os.path.join(output_dir, filename)

        success, result = download_image(image_url, filepath)
        if success:
            success_count += 1
            if i % 50 == 0:  # Периодический вывод прогресса
                print(f"📊 Прогресс: {i}/{total_count} игроков")
        else:
            if "Пропуск: изображение-заглушка" in result:
                skip_count += 1
            else:
                error_count += 1
                print(f"❌ Ошибка игрока {player_name}: {result}")

        time.sleep(0.05)  # небольшая задержка между запросами

    print(f"📊 Игроки: Успешно {success_count}, Пропущено {skip_count}, Ошибок {error_count}")
    return success_count, skip_count, error_count


def count_placeholder_images(players_file):
    """Подсчитывает количество игроков с изображениями-заглушками"""
    print("🔍 Подсчет игроков с изображениями-заглушками...")

    with open(players_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    placeholder_count = 0
    players_with_placeholders = []

    for player in players:
        image_url = player.get('image_url')
        if image_url == "https://cdn.sofifa.net/player_0.svg":
            placeholder_count += 1
            players_with_placeholders.append({
                'name': player.get('name'),
                'id': player.get('id'),
                'team': player.get('team_id')
            })

    print(f"📊 Найдено игроков с заглушками: {placeholder_count} из {len(players)}")

    # Сохраняем список игроков с заглушками
    if players_with_placeholders:
        with open("players_with_placeholder_images.json", 'w', encoding='utf-8') as f:
            json.dump(players_with_placeholders, f, indent=2, ensure_ascii=False)
        print(f"📋 Список сохранен в players_with_placeholder_images.json")

    return placeholder_count


def main():
    """Основная функция"""
    # Файлы с данными
    leagues_file = "sofifa_leagues.json"
    teams_file = "sofifa_teams.json"
    players_file = "sofifa_players.json"

    # Проверяем существование файлов
    for file in [leagues_file, teams_file, players_file]:
        if not os.path.exists(file):
            print(f"❌ Файл {file} не найден!")
            return

    print("🚀 Начинаем скачивание изображений...")
    start_time = time.time()

    # Считаем заглушки перед началом
    placeholder_count = count_placeholder_images(players_file)

    # Скачиваем логотипы лиг
    leagues_success, leagues_skip, leagues_errors = download_leagues_images(leagues_file)

    # Скачиваем логотипы команд
    teams_success, teams_skip, teams_errors = download_teams_images(teams_file)

    # Скачиваем изображения игроков (выберите один из методов)
    # players_success, players_skip, players_errors = download_players_images_sequential(players_file)
    players_success, players_skip, players_errors = download_players_images_parallel(players_file, max_workers=8)

    # Выводим общую статистику
    total_time = time.time() - start_time
    total_success = leagues_success + teams_success + players_success
    total_skip = leagues_skip + teams_skip + players_skip
    total_errors = leagues_errors + teams_errors + players_errors

    print(f"\n🎉 Скачивание завершено!")
    print(f"⏱️  Общее время: {total_time:.2f} секунд")
    print(f"📊 Итоговая статистика:")
    print(f"   ✅ Успешно скачано: {total_success}")
    print(f"   ⏭️  Пропущено (заглушки): {total_skip}")
    print(f"   ❌ Ошибок: {total_errors}")
    print(f"   📁 Лиги: {leagues_success} успешно, {leagues_skip} пропущено")
    print(f"   📁 Команды: {teams_success} успешно, {teams_skip} пропущено")
    print(f"   📁 Игроки: {players_success} успешно, {players_skip} пропущено")

    # Создаем файл с отчетом
    report = {
        "download_summary": {
            "total_time_seconds": round(total_time, 2),
            "total_success": total_success,
            "total_skipped_placeholders": total_skip,
            "total_errors": total_errors,
            "leagues_success": leagues_success,
            "leagues_skipped": leagues_skip,
            "leagues_errors": leagues_errors,
            "teams_success": teams_success,
            "teams_skipped": teams_skip,
            "teams_errors": teams_errors,
            "players_success": players_success,
            "players_skipped": players_skip,
            "players_errors": players_errors,
            "players_placeholder_count": placeholder_count
        },
        "folders_created": [
            "leagues/",
            "teams/",
            "players/"
        ],
        "placeholder_url": "https://cdn.sofifa.net/player_0.svg",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    with open("download_report.json", 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"📋 Отчет сохранен в download_report.json")


if __name__ == "__main__":
    main()