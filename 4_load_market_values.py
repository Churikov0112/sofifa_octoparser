# import json
# import requests
# import time
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import os
# from datetime import datetime
#
#
# def get_market_value_data(player_id, max_retries=3, retry_delay=2):
#     """Получает данные о рыночной стоимости игрока из API с повторными попытками"""
#     if not player_id or player_id == "unknown":
#         return None, "Invalid player ID"
#
#     url = f"https://transfermarkt-api.fly.dev/players/{player_id}/market_value"
#
#     for attempt in range(max_retries):
#         try:
#             response = requests.get(url, timeout=15)
#             response.raise_for_status()
#             return response.json(), None
#
#         except requests.exceptions.RequestException as e:
#             if attempt == max_retries - 1:  # Последняя попытка
#                 return None, f"API error after {max_retries} attempts: {e}"
#
#             # Логируем попытку
#             print(f"⚠️  Попытка {attempt + 1}/{max_retries} не удалась для player_id {player_id}: {e}")
#
#             # Ждем перед следующей попыткой (экспоненциальная задержка)
#             sleep_time = retry_delay * (2 ** attempt)
#             print(f"⏳ Ждем {sleep_time} секунд перед следующей попыткой...")
#             time.sleep(sleep_time)
#
#         except json.JSONDecodeError as e:
#             return None, f"JSON decode error: {e}"
#         except Exception as e:
#             return None, f"Unexpected error: {e}"
#
#     return None, f"Failed after {max_retries} attempts"
#
#
# def process_players_market_value(input_file, output_file, max_workers=5, delay=0.5):
#     """Обрабатывает игроков и добавляет данные о рыночной стоимости"""
#     print("📡 Начинаем получение данных о рыночной стоимости игроков...")
#
#     # Чтение исходного файла
#     try:
#         with open(input_file, 'r', encoding='utf-8') as f:
#             players = json.load(f)
#     except FileNotFoundError:
#         print(f"❌ Файл {input_file} не найден!")
#         return
#     except json.JSONDecodeError:
#         print(f"❌ Ошибка чтения JSON из файла {input_file}")
#         return
#
#     total_players = len(players)
#     success_count = 0
#     error_count = 0
#     skip_count = 0
#
#     print(f"📊 Всего игроков для обработки: {total_players}")
#
#     # Функция для обработки одного игрока
#     def process_single_player(player):
#         player_name = player.get('name', 'Unknown')
#         transfermarkt_id = player.get('transfermarkt_id')
#
#         # Проверяем наличие ID
#         if not transfermarkt_id or transfermarkt_id == "unknown":
#             return player, False, f"Пропуск {player_name}: отсутствует transfermarkt_id"
#
#         # Получаем данные из API
#         market_value_data, error = get_market_value_data(transfermarkt_id)
#
#         if error:
#             return player, False, f"Ошибка {player_name}: {error}"
#
#         # Добавляем данные к игроку
#         player['transfermarkt_market_value'] = market_value_data
#         return player, True, f"Успех {player_name}"
#
#     # Многопоточная обработка
#     processed_players = []
#
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         # Создаем future для каждого игрока
#         future_to_player = {
#             executor.submit(process_single_player, player): player
#             for player in players
#         }
#
#         # Обрабатываем результаты
#         for i, future in enumerate(as_completed(future_to_player), 1):
#             original_player = future_to_player[future]
#
#             try:
#                 player, success, message = future.result()
#
#                 if success:
#                     success_count += 1
#                     processed_players.append(player)
#                     if success_count % 10 == 0:
#                         print(f"📊 Прогресс: {i}/{total_players}, Успешно: {success_count}")
#                 else:
#                     if "Пропуск" in message:
#                         skip_count += 1
#                         processed_players.append(original_player)  # сохраняем оригинального игрока
#                     else:
#                         error_count += 1
#                         processed_players.append(original_player)  # сохраняем оригинального игрока
#
#                     if error_count % 10 == 0 or skip_count % 10 == 0:
#                         print(f"📊 Прогресс: {i}/{total_players}, Ошибок: {error_count}, Пропусков: {skip_count}")
#
#                 # Выводим сообщение для ошибок и пропусков
#                 if not success:
#                     print(f"ℹ️  {message}")
#
#             except Exception as e:
#                 error_count += 1
#                 processed_players.append(original_player)
#                 print(f"❌ Неожиданная ошибка при обработке игрока: {e}")
#
#             # Задержка между запросами для избежания rate limiting
#             time.sleep(delay)
#
#     # Сохраняем результат
#     try:
#         with open(output_file, 'w', encoding='utf-8') as f:
#             json.dump(processed_players, f, indent=2, ensure_ascii=False)
#
#         print(f"\n✅ Данные успешно сохранены в {output_file}")
#
#     except Exception as e:
#         print(f"❌ Ошибка при сохранении файла: {e}")
#         return
#
#     # Создаем отчет
#     report = {
#         "processing_summary": {
#             "total_players": total_players,
#             "successful_requests": success_count,
#             "skipped_players": skip_count,
#             "failed_requests": error_count,
#             "success_rate": f"{(success_count / total_players * 100):.1f}%",
#             "processing_date": datetime.now().isoformat(),
#             "api_endpoint": "https://transfermarkt-api.fly.dev/players/{id}/market_value"
#         },
#         "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     }
#
#     # Сохраняем отчет
#     report_filename = "market_value_report.json"
#     with open(report_filename, 'w', encoding='utf-8') as f:
#         json.dump(report, f, indent=2, ensure_ascii=False)
#
#     print(f"📋 Отчет сохранен в {report_filename}")
#     print(f"\n🎯 Итоговая статистика:")
#     print(f"   ✅ Успешно обработано: {success_count}")
#     print(f"   ⏭️  Пропущено: {skip_count} (отсутствует transfermarkt_id)")
#     print(f"   ❌ Ошибок: {error_count}")
#     print(f"   📊 Общий успех: {(success_count / total_players * 100):.1f}%")
#
#
# def validate_market_value_data():
#     """Валидирует полученные данные о рыночной стоимости"""
#     print("\n🔍 Валидация полученных данных...")
#
#     try:
#         with open("sofifa_players_with_market_value.json", 'r', encoding='utf-8') as f:
#             players = json.load(f)
#     except FileNotFoundError:
#         print("❌ Файл с данными о рыночной стоимости не найден")
#         return
#
#     players_with_data = 0
#     players_without_data = 0
#     total_market_value = 0
#     market_values = []
#
#     for player in players:
#         market_value_data = player.get('transfermarkt_market_value')
#         if market_value_data and isinstance(market_value_data, dict):
#             players_with_data += 1
#             market_value = market_value_data.get('marketValue', 0)
#             total_market_value += market_value
#             market_values.append(market_value)
#         else:
#             players_without_data += 1
#
#     # Статистика
#     avg_market_value = total_market_value / players_with_data if players_with_data > 0 else 0
#     max_market_value = max(market_values) if market_values else 0
#     min_market_value = min(market_values) if market_values else 0
#
#     validation_report = {
#         "validation_summary": {
#             "total_players": len(players),
#             "players_with_market_data": players_with_data,
#             "players_without_market_data": players_without_data,
#             "data_coverage": f"{(players_with_data / len(players) * 100):.1f}%",
#             "average_market_value": avg_market_value,
#             "max_market_value": max_market_value,
#             "min_market_value": min_market_value,
#             "total_market_value_sum": total_market_value
#         },
#         "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     }
#
#     with open("market_value_validation.json", 'w', encoding='utf-8') as f:
#         json.dump(validation_report, f, indent=2, ensure_ascii=False)
#
#     print(f"✅ Валидация завершена. Отчет сохранен в market_value_validation.json")
#     print(f"📊 Данные получены для {players_with_data} из {len(players)} игроков")
#     print(f"💰 Средняя рыночная стоимость: €{avg_market_value:,.0f}")
#
#
# def main():
#     """Основная функция"""
#     input_filename = "sofifa_players.json"  # Файл с игроками
#     output_filename = "sofifa_players_with_market_value.json"
#
#     # Проверяем существование файла
#     if not os.path.exists(input_filename):
#         print(f"❌ Файл {input_filename} не найден!")
#         print("Убедитесь, что файл с игроками существует")
#         return
#
#     # Обрабатываем данные о рыночной стоимости
#     process_players_market_value(input_filename, output_filename, max_workers=3, delay=1.0)
#
#     # Валидируем полученные данные
#     validate_market_value_data()
#
#     print("\n🎉 Все задачи завершены!")
#     print("Следующие шаги:")
#     print("1. Проверьте файл sofifa_players_with_market_value.json")
#     print("2. Посмотрите отчеты в market_value_report.json и market_value_validation.json")
#     print("3. Объедините данные с другими файлами при необходимости")
#
#     merge_market_value_to_hierarchical()
#
#
# if __name__ == "__main__":
#     main()

import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from datetime import datetime


def get_market_value_data(player_id, max_retries=3, retry_delay=2):
    """Получает данные о рыночной стоимости игрока из API с повторными попытками"""
    if not player_id or player_id == "unknown":
        return None, "Invalid player ID"

    url = f"https://transfermarkt-api.fly.dev/players/{player_id}/market_value"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.json(), None

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:  # Последняя попытка
                return None, f"API error after {max_retries} attempts: {e}"

            # Логируем попытку
            print(f"⚠️  Попытка {attempt + 1}/{max_retries} не удалась для player_id {player_id}: {e}")

            # Ждем перед следующей попыткой (экспоненциальная задержка)
            sleep_time = retry_delay * (2 ** attempt)
            print(f"⏳ Ждем {sleep_time} секунд перед следующей попыткой...")
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


def save_progress(output_file, players_data):
    """Сохраняет прогресс в файл"""
    try:
        # Создаем временный файл для безопасного сохранения
        temp_file = output_file + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(players_data, f, indent=2, ensure_ascii=False)

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


def process_players_market_value(input_file, output_file, max_workers=5, delay=0.5):
    """Обрабатывает игроков и добавляет данные о рыночной стоимости с сохранением прогресса"""
    print("📡 Начинаем получение данных о рыночной стоимости игроков...")

    # Чтение исходного файла
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            all_players = json.load(f)
    except FileNotFoundError:
        print(f"❌ Файл {input_file} не найден!")
        return
    except json.JSONDecodeError:
        print(f"❌ Ошибка чтения JSON из файла {input_file}")
        return

    # Загружаем существующие данные
    existing_players = load_existing_data(output_file)

    # Создаем словарь уже обработанных игроков по id
    processed_player_ids = {}
    for player in existing_players:
        player_id = player.get('id')
        if player_id:
            processed_player_ids[player_id] = player

    # Фильтруем игроков: оставляем только тех, кто еще не обработан
    players_to_process = []
    for player in all_players:
        player_id = player.get('id')
        if player_id and player_id not in processed_player_ids:
            players_to_process.append(player)
        elif player_id in processed_player_ids:
            # Проверяем, есть ли уже данные о рыночной стоимости
            existing_player = processed_player_ids[player_id]
            if 'transfermarkt_market_value' in existing_player:
                # Игрок уже полностью обработан
                continue
            else:
                # Игрок в файле, но без данных - обрабатываем заново
                players_to_process.append(player)

    total_players = len(players_to_process)
    total_all_players = len(all_players)
    success_count = 0
    error_count = 0
    skip_count = 0

    print(f"📊 Всего игроков: {total_all_players}, нужно обработать: {total_players}")
    print(f"📊 Уже обработано: {total_all_players - total_players}")

    if total_players == 0:
        print("🎉 Все игроки уже обработаны!")
        return

    # Начинаем с уже существующих данных
    processed_players = existing_players.copy()

    # Функция для обработки одного игрока
    def process_single_player(player):
        player_name = player.get('name', 'Unknown')
        player_id = player.get('id')
        transfermarkt_id = player.get('transfermarkt_id')

        # Проверяем наличие ID
        if not transfermarkt_id or transfermarkt_id == "unknown":
            return player, False, f"Пропуск {player_name}: отсутствует transfermarkt_id", player_id

        # Получаем данные из API
        market_value_data, error = get_market_value_data(transfermarkt_id)

        if error:
            return player, False, f"Ошибка {player_name}: {error}", player_id

        # Добавляем данные к игроку
        player['transfermarkt_market_value'] = market_value_data
        return player, True, f"Успех {player_name}", player_id

    # Многопоточная обработка
    start_time = time.time()
    last_save_time = time.time()
    save_interval = 30  # Сохранять каждые 30 секунд

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Создаем future для каждого игрока
        future_to_player = {
            executor.submit(process_single_player, player): player
            for player in players_to_process
        }

        # Обрабатываем результаты
        for i, future in enumerate(as_completed(future_to_player), 1):
            original_player = future_to_player[future]

            try:
                player, success, message, player_id = future.result()

                if success:
                    success_count += 1
                    # Заменяем или добавляем игрока
                    player_found = False
                    for idx, existing_player in enumerate(processed_players):
                        if existing_player.get('id') == player_id:
                            processed_players[idx] = player
                            player_found = True
                            break
                    if not player_found:
                        processed_players.append(player)

                    if success_count % 5 == 0:  # Чаще выводим прогресс
                        print(f"📊 Прогресс: {i}/{total_players}, Успешно: {success_count}")
                else:
                    if "Пропуск" in message:
                        skip_count += 1
                        # Добавляем оригинального игрока (без данных)
                        player_found = False
                        for idx, existing_player in enumerate(processed_players):
                            if existing_player.get('id') == player_id:
                                player_found = True
                                break
                        if not player_found:
                            processed_players.append(original_player)
                    else:
                        error_count += 1
                        # Добавляем оригинального игрока (без данных)
                        player_found = False
                        for idx, existing_player in enumerate(processed_players):
                            if existing_player.get('id') == player_id:
                                player_found = True
                                break
                        if not player_found:
                            processed_players.append(original_player)

                    if error_count % 10 == 0 or skip_count % 10 == 0:
                        print(f"📊 Прогресс: {i}/{total_players}, Ошибок: {error_count}, Пропусков: {skip_count}")

                # Выводим сообщение для ошибок и пропусков
                if not success and (error_count + skip_count) % 5 == 0:
                    print(f"ℹ️  {message}")

            except Exception as e:
                error_count += 1
                print(f"❌ Неожиданная ошибка при обработке игрока: {e}")
                # Добавляем оригинального игрока в случае ошибки
                player_id = original_player.get('id')
                player_found = False
                for idx, existing_player in enumerate(processed_players):
                    if existing_player.get('id') == player_id:
                        player_found = True
                        break
                if not player_found:
                    processed_players.append(original_player)

            # Периодическое сохранение прогресса
            current_time = time.time()
            if current_time - last_save_time >= save_interval or i == total_players:
                if save_progress(output_file, processed_players):
                    print(f"💾 Прогресс сохранен ({i}/{total_players} игроков)")
                    last_save_time = current_time

            # Задержка между запросами для избежания rate limiting
            time.sleep(delay)

    # Финальное сохранение
    if save_progress(output_file, processed_players):
        print("💾 Финальный результат сохранен")

    # Создаем отчет
    total_time = time.time() - start_time
    report = {
        "processing_summary": {
            "total_players": total_all_players,
            "processed_this_run": total_players,
            "successful_requests": success_count,
            "skipped_players": skip_count,
            "failed_requests": error_count,
            "already_processed": total_all_players - total_players,
            "success_rate": f"{(success_count / total_players * 100):.1f}%" if total_players > 0 else "100%",
            "processing_time_seconds": round(total_time, 2),
            "processing_date": datetime.now().isoformat(),
            "api_endpoint": "https://transfermarkt-api.fly.dev/players/{id}/market_value"
        },
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Сохраняем отчет
    report_filename = "market_value_report.json"
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"📋 Отчет сохранен в {report_filename}")
    print(f"\n🎯 Итоговая статистика:")
    print(f"   ✅ Успешно обработано: {success_count}")
    print(f"   ⏭️  Пропущено: {skip_count}")
    print(f"   ❌ Ошибок: {error_count}")
    print(f"   📊 Уже было обработано: {total_all_players - total_players}")
    print(f"   ⏱️  Время обработки: {total_time:.2f} секунд")


def main():
    """Основная функция"""
    input_filename = "sofifa_players.json"  # Файл с игроками
    output_filename = "sofifa_players_with_market_value.json"

    # Проверяем существование файла
    if not os.path.exists(input_filename):
        print(f"❌ Файл {input_filename} не найден!")
        print("Убедитесь, что файл с игроками существует")
        return

    # Обрабатываем данные о рыночной стоимости
    process_players_market_value(input_filename, output_filename, max_workers=3, delay=1.0)

    print("\n🎉 Все задачи завершены!")
    print("Следующие шаги:")
    print("1. Проверьте файл sofifa_players_with_market_value.json")
    print("2. Посмотрите отчеты в market_value_report.json и market_value_validation.json")

if __name__ == "__main__":
    main()