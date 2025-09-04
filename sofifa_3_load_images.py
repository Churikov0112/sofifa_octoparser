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


def download_competitions_images(competitions_file, output_dir='sofifa_competitions'):
    """Скачивает логотипы лиг"""
    print("📥 Скачивание логотипов лиг...")

    with open(competitions_file, 'r', encoding='utf-8') as f:
        competitions = json.load(f)

    success_count = 0
    error_count = 0
    skip_count = 0

    for competition in competitions:
        logo_url = competition.get('logo_url')
        competition_id = competition.get('id')
        competition_name = competition.get('name', 'Unknown')

        if not logo_url or not competition_id:
            print(f"⚠️  Пропуск лиги {competition_name}: отсутствует URL или ID")
            error_count += 1
            continue

        # Проверяем заглушку
        if logo_url == "https://cdn.sofifa.net/player_0.svg":
            print(f"⏭️  Пропуск лиги {competition_name}: изображение-заглушка")
            skip_count += 1
            continue

        extension = get_file_extension(logo_url)
        filename = f"{competition_id}{extension}"
        filepath = os.path.join(output_dir, filename)

        success, result = download_image(logo_url, filepath)
        if success:
            print(f"✅ Лига {competition_name}: {filename}")
            success_count += 1
        else:
            if "Пропуск: изображение-заглушка" in result:
                print(f"⏭️  Лига {competition_name}: изображение-заглушка")
                skip_count += 1
            else:
                print(f"❌ Ошибка лиги {competition_name}: {result}")
                error_count += 1

        time.sleep(0.1)  # небольшая задержка между запросами

    print(f"📊 Лиги: Успешно {success_count}, Пропущено {skip_count}, Ошибок {error_count}")
    return success_count, skip_count, error_count


def download_teams_images(teams_file, output_dir='sofifa_teams'):
    """Скачивает логотипы команд"""
    print("\n📥 Скачивание логотипов команд...")

    with open(teams_file, 'r', encoding='utf-8') as f:
        teams = json.load(f)

    success_count = 0
    error_count = 0
    skip_count = 0

    for team in teams:
        logo_url = team.get('logo_url')
        team_id = team.get('id')
        team_name = team.get('name', 'Unknown')

        if not logo_url or not team_id:
            print(f"⚠️  Пропуск команды {team_name}: отсутствует URL или ID")
            error_count += 1
            continue

        # Проверяем заглушку
        if logo_url == "https://cdn.sofifa.net/player_0.svg":
            print(f"⏭️  Пропуск команды {team_name}: изображение-заглушка")
            skip_count += 1
            continue

        extension = get_file_extension(logo_url)
        filename = f"{team_id}{extension}"
        filepath = os.path.join(output_dir, filename)

        success, result = download_image(logo_url, filepath)
        if success:
            print(f"✅ Команда {team_name}: {filename}")
            success_count += 1
        else:
            if "Пропуск: изображение-заглушка" in result:
                print(f"⏭️  Команда {team_name}: изображение-заглушка")
                skip_count += 1
            else:
                print(f"❌ Ошибка команды {team_name}: {result}")
                error_count += 1

        time.sleep(0.1)  # небольшая задержка между запросами

    print(f"📊 Команды: Успешно {success_count}, Пропущено {skip_count}, Ошибок {error_count}")
    return success_count, skip_count, error_count


def download_players_images_parallel(players_file, output_dir='sofifa_players', max_workers=10):
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


def download_players_images_sequential(players_file, output_dir='sofifa_players'):
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
                'team_id': player.get('team_id')
            })

    print(f"📊 Найдено игроков с заглушками: {placeholder_count} из {len(players)}")

    # Сохраняем список игроков с заглушками
    if players_with_placeholders:
        with open("sofifa_players_with_placeholder_images.json", 'w', encoding='utf-8') as f:
            json.dump(players_with_placeholders, f, indent=2, ensure_ascii=False)
        print(f"📋 Список сохранен в sofifa_players_with_placeholder_images.json")

    return placeholder_count


def main():
    """Основная функция"""
    # Файлы с данными (новая структура)
    competitions_file = "sofifa_competitions.json"
    teams_file = "sofifa_teams.json"
    players_file = "sofifa_players.json"

    # Проверяем существование файлов
    for file in [competitions_file, teams_file, players_file]:
        if not os.path.exists(file):
            print(f"❌ Файл {file} не найден!")
            print("Сначала запустите скрипт разделения данных")
            return

    print("🚀 Начинаем скачивание изображений...")
    start_time = time.time()

    # Считаем заглушки перед началом
    placeholder_count = count_placeholder_images(players_file)

    # Скачиваем логотипы лиг
    competitions_success, competitions_skip, competitions_errors = download_competitions_images(competitions_file)

    # Скачиваем логотипы команд
    teams_success, teams_skip, teams_errors = download_teams_images(teams_file)

    # Скачиваем изображения игроков (выберите один из методов)
    # players_success, players_skip, players_errors = download_players_images_sequential(players_file)
    players_success, players_skip, players_errors = download_players_images_parallel(players_file, max_workers=8)

    # Выводим общую статистику
    total_time = time.time() - start_time
    total_success = competitions_success + teams_success + players_success
    total_skip = competitions_skip + teams_skip + players_skip
    total_errors = competitions_errors + teams_errors + players_errors

    print(f"\n🎉 Скачивание завершено!")
    print(f"⏱️  Общее время: {total_time:.2f} секунд")
    print(f"📊 Итоговая статистика:")
    print(f"   ✅ Успешно скачано: {total_success}")
    print(f"   ⏭️  Пропущено (заглушки): {total_skip}")
    print(f"   ❌ Ошибок: {total_errors}")
    print(f"   📁 Лиги: {competitions_success} успешно, {competitions_skip} пропущено")
    print(f"   📁 Команды: {teams_success} успешно, {teams_skip} пропущено")
    print(f"   📁 Игроки: {players_success} успешно, {players_skip} пропущено")


if __name__ == "__main__":
    main()