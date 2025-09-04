import json
import requests
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import random


def get_market_value_data(player_id, max_retries=5, retry_delay=3):
    """Получает данные о рыночной стоимости игрока из API с повторными попытками"""
    if not player_id or player_id == "unknown":
        return None, "Invalid player ID"

    url = f"http://localhost:8000/players/{player_id}/market_value"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=20)

            # Проверяем статус 503 специально
            if response.status_code == 503:
                raise requests.exceptions.RequestException(f"Service Unavailable (503) for player {player_id}")

            response.raise_for_status()
            return response.json(), None

        except requests.exceptions.RequestException as e:
            if "503" in str(e):
                print(f"🔴 Сервер перегружен (503) для игрока {player_id}, попытка {attempt + 1}/{max_retries}")
                # Увеличиваем задержку для 503 ошибок
                sleep_time = retry_delay * (3 ** attempt) + random.uniform(2, 5)
            else:
                sleep_time = retry_delay * (2 ** attempt) + random.uniform(1, 3)

            if attempt == max_retries - 1:  # Последняя попытка
                return None, f"API error after {max_retries} attempts: {e}"

            print(f"⚠️  Попытка {attempt + 1}/{max_retries} не удалась для игрока {player_id}: {e}")
            print(f"⏳ Ждем {sleep_time:.1f} секунд перед следующей попыткой...")
            time.sleep(sleep_time)

        except json.JSONDecodeError as e:
            return None, f"JSON decode error: {e}"
        except Exception as e:
            return None, f"Unexpected error: {e}"

    return None, f"Failed after {max_retries} attempts"


def load_existing_data(output_file):
    """Загружает существующие данные, если файл уже существует"""
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            print("⚠️  Существующий файл поврежден или пустой, начнем заново")
            return []
    return []


def save_progress(output_file, market_values_data):
    """Сохраняет прогресс в файл"""
    try:
        # Создаем временный файл для безопасного сохранения
        temp_file = output_file + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(market_values_data, f, indent=2, ensure_ascii=False)

        # Заменяем оригинальный файл
        if os.path.exists(output_file):
            os.remove(output_file)
        os.rename(temp_file, output_file)

        return True
    except Exception as e:
        print(f"❌ Ошибка при сохранении прогресса: {e}")
        # Пытаемся удалить временный файл, если он существует
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return False


def extract_player_ids_from_players(players_data):
    """Извлекает все ID игроков из данных об игроках (используем transfermarkt_id)"""
    player_ids = set()

    for player in players_data:
        if isinstance(player, dict):
            player_id = player.get('transfermarkt_id')
            if player_id and player_id != "unknown":
                player_ids.add(player_id)

    return list(player_ids)


def process_players_market_values(input_file, output_file, delay=1.0):
    """Обрабатывает игроков и получает данные о рыночной стоимости"""
    print("📡 Начинаем получение данных о рыночной стоимости игроков...")

    # Чтение файла с игроками
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            players_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Файл {input_file} не найден!")
        return
    except json.JSONDecodeError:
        print(f"❌ Ошибка чтения JSON из файла {input_file}")
        return

    # Извлекаем все ID игроков (transfermarkt_id)
    all_player_ids = extract_player_ids_from_players(players_data)
    total_all_players = len(all_player_ids)

    if total_all_players == 0:
        print("❌ Не найдено игроков для обработки!")
        return

    # Загружаем существующие данные
    existing_market_values = load_existing_data(output_file)

    # Создаем словарь для быстрого поиска по player_id в существующих данных
    existing_market_values_map = {mv['id']: mv for mv in existing_market_values if 'id' in mv}

    # Определяем, каких игроков нужно обработать
    player_ids_to_process = []
    for player_id in all_player_ids:
        if player_id not in existing_market_values_map:
            player_ids_to_process.append(player_id)

    total_players = len(player_ids_to_process)
    success_count = 0
    error_count = 0
    rate_limit_count = 0

    print(f"📊 Всего игроков: {total_all_players}, нужно обработать: {total_players}")
    print(f"📊 Уже обработано: {total_all_players - total_players}")

    if total_players == 0:
        print("🎉 Все игроки уже обработаны!")
        return

    # Начинаем с уже существующих данных
    processed_market_values = existing_market_values.copy()

    start_time = time.time()
    last_save_time = time.time()
    save_interval = 45

    # Обрабатываем игроков
    for i, player_id in enumerate(player_ids_to_process, 1):
        print(f"🔍 Обрабатываем игрока {i}/{total_players}: ID {player_id}")

        # Минимальная задержка между запросами к self-hosted API
        if delay > 0:
            time.sleep(delay)

        market_value_data, error = get_market_value_data(player_id)

        if error:
            if "503" in error:
                rate_limit_count += 1
            error_count += 1
            print(f"❌ Ошибка для игрока {player_id}: {error}")

            # Не добавляем записи об ошибках, просто пропускаем
        else:
            success_count += 1

            # Добавляем только данные о рыночной стоимости
            processed_market_values.append(market_value_data)

            print(f"✅ Успешно: ID {player_id}")

        # Периодическое сохранение
        current_time = time.time()
        if current_time - last_save_time >= save_interval or i == total_players:
            if save_progress(output_file, processed_market_values):
                print(f"💾 Прогресс сохранен ({i}/{total_players} игроков)")
                last_save_time = current_time

        if i % 5 == 0 or i == total_players:
            print(
                f"📊 Прогресс: {i}/{total_players}, Успешно: {success_count}, Ошибок: {error_count}, 503: {rate_limit_count}")

    # Финальное сохранение
    if save_progress(output_file, processed_market_values):
        print("💾 Финальный результат сохранен")

    # Создаем отчет
    total_time = time.time() - start_time

    print(f"\n🎯 Итоговая статистика:")
    print(f"   ✅ Успешно обработано: {success_count}")
    print(f"   ❌ Ошибок: {error_count}")
    print(f"   🔴 503 ошибок: {rate_limit_count}")
    print(f"   📊 Уже было обработано: {total_all_players - total_players}")
    print(f"   ⏱️  Время обработки: {total_time / 60:.1f} минут")


def main():
    """Основная функция"""
    # Конфигурация - изменены имена файлов
    input_filename = "sofifa_players.json"  # Файл с данными об игроках
    output_filename = "sofifa_tm_market_values.json"  # Выходной файл

    # Проверяем существование входного файла
    if not os.path.exists(input_filename):
        print(f"❌ Файл {input_filename} не найден!")
        print("Сначала запустите скрипт для получения данных об игроках")
        return

    print(f"⚽ Начинаем обработку рыночных стоимостей игроков из файла {input_filename}")
    print("⚠️  Используем минимальные задержки для self-hosted API")

    # Обрабатываем данные о рыночной стоимости с минимальными задержками
    process_players_market_values(input_filename, output_filename, delay=0.5)

    print("\n🎉 Все задачи завершены!")
    print(f"Данные сохранены в {output_filename}")


if __name__ == "__main__":
    main()