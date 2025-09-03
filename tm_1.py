import json
import requests
import time
import os
from datetime import datetime


def get_competition_clubs(competition_id, season_id, max_retries=3, retry_delay=2):
    """Получает данные о клубах в лиге из API с повторными попытками"""
    # url = f"https://transfermarkt-api.fly.dev/competitions/{competition_id}/clubs"
    url = f"http://localhost:8000/competitions/{competition_id}/clubs"
    params = {"season_id": season_id}

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json(), None

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:  # Последняя попытка
                return None, f"API error after {max_retries} attempts: {e}"

            # Логируем попытку
            print(f"⚠️  Попытка {attempt + 1}/{max_retries} не удалась для лиги {competition_id}: {e}")

            # Ждем перед следующей попыткой (экспоненциальная задержка)
            sleep_time = retry_delay * (2 ** attempt)
            print(f"⏳ Ждем {sleep_time} секунд перед следующей попыткой...")
            time.sleep(sleep_time)

        except json.JSONDecodeError as e:
            return None, f"JSON decode error: {e}"
        except Exception as e:
            return None, f"Unexpected error: {e}"

    return None, f"Failed after {max_retries} attempts"


def transform_competition_data(competition_data):
    """Преобразует данные лиги в требуемый формат"""
    if not competition_data:
        return None

    # Создаем новый объект с нужными полями
    transformed = {
        "id": competition_data.get("id"),
        "name": competition_data.get("name"),
        "seasonId": competition_data.get("seasonId"),
        "teams_ids": []
    }

    # Извлекаем ID команд из массива clubs
    clubs = competition_data.get("clubs", [])
    for club in clubs:
        club_id = club.get("id")
        if club_id:
            transformed["teams_ids"].append(club_id)

    return transformed


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


def save_progress(output_file, competitions_data):
    """Сохраняет прогресс в файл"""
    try:
        # Создаем временный файл для безопасного сохранения
        temp_file = output_file + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(competitions_data, f, indent=2, ensure_ascii=False)

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


def process_competitions(competition_ids, season_id, output_file, delay=1.0):
    """Обрабатывает лиги и получает данные о клубах с сохранением прогресса"""
    print("📡 Начинаем получение данных о клубах в лигах...")

    # Загружаем существующие данные
    existing_competitions = load_existing_data(output_file)

    # Создаем словарь уже обработанных лиг по id
    processed_competition_ids = {}
    for competition in existing_competitions:
        comp_id = competition.get('id')
        if comp_id:
            processed_competition_ids[comp_id] = competition

    # Фильтруем лиги: оставляем только те, которые еще не обработаны
    competitions_to_process = []
    for comp_id in competition_ids:
        if comp_id not in processed_competition_ids:
            competitions_to_process.append(comp_id)
        else:
            # Проверяем, соответствует ли сезон
            existing_comp = processed_competition_ids[comp_id]
            if existing_comp.get('seasonId') == season_id:
                print(f"ℹ️  Лига {comp_id} для сезона {season_id} уже обработана")
            else:
                # Сезон отличается - обрабатываем заново
                competitions_to_process.append(comp_id)

    total_competitions = len(competitions_to_process)
    total_all_competitions = len(competition_ids)
    success_count = 0
    error_count = 0
    skip_count = 0

    print(f"📊 Всего лиг: {total_all_competitions}, нужно обработать: {total_competitions}")
    print(f"📊 Уже обработано: {total_all_competitions - total_competitions}")

    if total_competitions == 0:
        print("🎉 Все лиги уже обработаны!")
        return

    # Начинаем с уже существующих данных
    processed_competitions = existing_competitions.copy()

    start_time = time.time()
    last_save_time = time.time()
    save_interval = 30  # Сохранять каждые 30 секунд

    for i, comp_id in enumerate(competitions_to_process, 1):
        print(f"🔍 Обрабатываем лигу {comp_id} ({i}/{total_competitions})...")

        # Получаем данные из API
        competition_data, error = get_competition_clubs(comp_id, season_id)

        if error:
            error_count += 1
            print(f"❌ Ошибка при получении данных для лиги {comp_id}: {error}")

            # Пропускаем эту лигу и продолжаем
            skip_count += 1
            continue

        # Преобразуем данные в нужный формат
        transformed_data = transform_competition_data(competition_data)

        if not transformed_data:
            error_count += 1
            print(f"❌ Ошибка при преобразовании данных для лиги {comp_id}")
            skip_count += 1
            continue

        # Добавляем данные о лиге
        success_count += 1

        # Проверяем, есть ли уже эта лига в обработанных данных
        comp_found = False
        for idx, existing_comp in enumerate(processed_competitions):
            if existing_comp.get('id') == comp_id:
                processed_competitions[idx] = transformed_data
                comp_found = True
                break

        if not comp_found:
            processed_competitions.append(transformed_data)

        print(f"✅ Успешно получены данные для лиги {comp_id}: {transformed_data.get('name', 'Unknown')}")
        print(f"   📊 Количество команд: {len(transformed_data.get('teams_ids', []))}")

        # Периодическое сохранение прогресса
        current_time = time.time()
        if current_time - last_save_time >= save_interval or i == total_competitions:
            if save_progress(output_file, processed_competitions):
                print(f"💾 Прогресс сохранен ({i}/{total_competitions} лиг)")
                last_save_time = current_time

        # Задержка между запросами для избежания rate limiting
        if i < total_competitions:  # Не ждем после последнего запроса
            print(f"⏳ Ждем {delay} секунд перед следующим запросом...")
            time.sleep(delay)

    # Финальное сохранение
    if save_progress(output_file, processed_competitions):
        print("💾 Финальный результат сохранен")

    # Создаем отчет
    total_time = time.time() - start_time

    print(f"\n🎯 Итоговая статистика:")
    print(f"   ✅ Успешно обработано: {success_count}")
    print(f"   ❌ Ошибок: {error_count}")
    print(f"   📊 Уже было обработано: {total_all_competitions - total_competitions}")
    print(f"   ⏱️  Время обработки: {total_time:.2f} секунд")


def main():
    """Основная функция"""
    # Конфигурация
    competition_ids = ["GB1", "GB2"]  # ID лиг для обработки
    season_id = "2024"  # Сезон
    output_filename = "tm_competitions.json"  # Выходной файл

    print(f"🏆 Начинаем обработку лиг: {competition_ids}")
    print(f"📅 Сезон: {season_id}")

    # Обрабатываем данные о лигах и клубах
    process_competitions(competition_ids, season_id, output_filename, delay=1.0)

    print("\n🎉 Все задачи завершены!")
    print(f"Данные сохранены в {output_filename}")


if __name__ == "__main__":
    main()