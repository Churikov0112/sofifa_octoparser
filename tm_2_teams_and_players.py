import json
import requests
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import random


def get_club_players(club_id, season_id, max_retries=5, retry_delay=3):
    """Получает данные о игроках клуба из API с повторными попытками и обработкой 503"""
    # url = f"https://transfermarkt-api.fly.dev/clubs/{club_id}/players"
    url = f"http://localhost:8000/clubs/{club_id}/players"
    params = {"season_id": season_id}

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=20)

            # Проверяем статус 503 специально
            if response.status_code == 503:
                raise requests.exceptions.RequestException(f"Service Unavailable (503) for club {club_id}")

            response.raise_for_status()
            return response.json(), None

        except requests.exceptions.RequestException as e:
            if "503" in str(e):
                print(f"🔴 Сервер перегружен (503) для клуба {club_id}, попытка {attempt + 1}/{max_retries}")
                # Увеличиваем задержку для 503 ошибок
                sleep_time = retry_delay * (3 ** attempt) + random.uniform(2, 5)
            else:
                sleep_time = retry_delay * (2 ** attempt) + random.uniform(1, 3)

            if attempt == max_retries - 1:  # Последняя попытка
                return None, f"API error after {max_retries} attempts: {e}"

            print(f"⚠️  Попытка {attempt + 1}/{max_retries} не удалась для клуба {club_id}: {e}")
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


def save_progress(output_file, data):
    """Сохраняет прогресс в файл"""
    try:
        # Создаем временный файл для безопасного сохранения
        temp_file = output_file + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

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


def load_competitions_data(input_file):
    """Загружает данные о лигах и клубах"""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Файл {input_file} не найден!")
        return None
    except json.JSONDecodeError:
        print(f"❌ Ошибка чтения JSON из файла {input_file}")
        return None


def extract_clubs_from_competitions(competitions_data, season_id):
    """Извлекает все клубы из данных о лигах для указанного сезона"""
    all_clubs = []

    for competition in competitions_data:
        comp_season = competition.get('seasonId')
        comp_id = competition.get('id')

        # Проверяем, что сезон совпадает
        if comp_season == season_id:
            team_ids = competition.get('teams_ids', [])
            for team_id in team_ids:
                # Создаем базовую информацию о клубе
                club_info = {
                    'id': team_id,
                    'name': f"Team {team_id}",  # Имя будет получено позже из API
                    'competition_id': comp_id,
                    'season_id': season_id
                }
                all_clubs.append(club_info)

    return all_clubs


class RateLimiter:
    """Класс для управления rate limiting"""

    def __init__(self, max_requests=10, time_window=60, initial_delay=2.0):
        self.max_requests = max_requests
        self.time_window = time_window
        self.request_times = []
        self.initial_delay = initial_delay
        self.consecutive_503 = 0

    def wait_if_needed(self):
        """Ждет если превышен лимит запросов"""
        current_time = time.time()

        # Удаляем старые запросы
        self.request_times = [t for t in self.request_times if current_time - t < self.time_window]

        if len(self.request_times) >= self.max_requests:
            wait_time = self.time_window - (current_time - min(self.request_times)) + 0.1
            print(f"⏰ Превышен лимит запросов. Ждем {wait_time:.1f} секунд...")
            time.sleep(wait_time)
            current_time = time.time()

        self.request_times.append(current_time)

        # Добавляем базовую задержку
        delay = self.initial_delay

        # Увеличиваем задержку после 503 ошибок
        if self.consecutive_503 > 0:
            delay += random.uniform(3, 8) * self.consecutive_503

        if delay > 0:
            time.sleep(delay)

    def record_503(self):
        """Записывает 503 ошибку для увеличения задержек"""
        self.consecutive_503 += 1
        print(f"🔴 Зарегистрировано 503 ошибок подряд: {self.consecutive_503}")

    def reset_503(self):
        """Сбрасывает счетчик 503 ошибок"""
        if self.consecutive_503 > 0:
            print(f"🟢 Сбрасываем счетчик 503 ошибок")
            self.consecutive_503 = 0


def process_clubs_players(input_file, teams_output_file, players_output_file, season_id, max_workers=2, delay=3.0):
    """Обрабатывает клубы и получает данные о игроках с улучшенной обработкой 503"""
    print("📡 Начинаем получение данных о игроках клубов...")

    competitions_data = load_competitions_data(input_file)
    if competitions_data is None:
        return

    # Извлекаем все клубы
    all_clubs = extract_clubs_from_competitions(competitions_data, season_id)
    total_all_clubs = len(all_clubs)

    if total_all_clubs == 0:
        print("❌ Не найдено клубов для обработки!")
        return

    # Загружаем существующие данные
    existing_teams_data = load_existing_data(teams_output_file)
    existing_players_data = load_existing_data(players_output_file)

    # Создаем словари для быстрого поиска
    existing_teams_map = {team['id']: team for team in existing_teams_data if 'id' in team}
    existing_players_map = {player['id']: player for player in existing_players_data if 'id' in player}

    # Определяем, какие клубы нужно обработать
    clubs_to_process = []
    for club in all_clubs:
        club_id = club.get('id')
        if club_id not in existing_teams_map:
            clubs_to_process.append(club)
        else:
            # Проверяем, соответствует ли сезон и есть ли данные об игроках
            existing_team = existing_teams_map[club_id]
            if existing_team.get('season_id') != season_id or 'players_ids' not in existing_team:
                clubs_to_process.append(club)

    total_clubs = len(clubs_to_process)
    success_count = 0
    error_count = 0
    rate_limit_count = 0

    print(f"📊 Всего клубов: {total_all_clubs}, нужно обработать: {total_clubs}")
    print(f"📊 Уже обработано: {total_all_clubs - total_clubs}")

    if total_clubs == 0:
        print("🎉 Все клубы уже обработаны!")
        return

    # Начинаем с уже существующих данных
    processed_teams = existing_teams_data.copy()
    processed_players = existing_players_data.copy()

    rate_limiter = RateLimiter(max_requests=8, time_window=60, initial_delay=delay)
    start_time = time.time()
    last_save_time = time.time()
    save_interval = 45

    # Обрабатываем клубы
    for i, club_info in enumerate(clubs_to_process, 1):
        club_id = club_info.get('id')
        competition_id = club_info.get('competition_id', 'Unknown')

        print(f"🔍 Обрабатываем клуб {i}/{total_clubs}: ID {club_id} (Лига: {competition_id})")

        rate_limiter.wait_if_needed()
        players_data, error = get_club_players(club_id, season_id)

        if error:
            if "503" in error:
                rate_limit_count += 1
                rate_limiter.record_503()
                delay += random.uniform(2, 5)
                print(f"🔴 Увеличиваем задержку до {delay:.1f} секунд")
            error_count += 1
            print(f"❌ Ошибка для клуба {club_id}: {error}")

            # Добавляем клуб без данных об игроках
            if not any(team.get('id') == club_id for team in processed_teams):
                team_data = {
                    'id': club_id,
                    'name': f"Team {club_id}",
                    'competition_id': competition_id,
                    'season_id': season_id,
                    'players_ids': []
                }
                processed_teams.append(team_data)
        else:
            success_count += 1
            rate_limiter.reset_503()

            # Извлекаем информацию о команде из ответа
            team_name = players_data.get('name', f"Team {club_id}")

            # Создаем данные команды
            player_ids = []
            team_data = {
                'id': club_id,
                'name': team_name,
                'competition_id': competition_id,
                'season_id': season_id,
                'players_ids': player_ids
            }

            # Обрабатываем игроков
            players_list = players_data.get('players', [])
            for player in players_list:
                player_id = player.get('id')
                if player_id:
                    player_ids.append(player_id)

                    # Создаем данные игрока с team_id
                    player_data = player.copy()
                    player_data['team_id'] = club_id

                    # Удаляем старую запись если существует и добавляем новую
                    processed_players = [p for p in processed_players if p.get('id') != player_id]
                    processed_players.append(player_data)

            # Удаляем старую запись команды если существует и добавляем новую
            processed_teams = [team for team in processed_teams if team.get('id') != club_id]
            processed_teams.append(team_data)

            print(f"✅ Успешно: {team_name} ({len(player_ids)} игроков)")

        # Периодическое сохранение
        current_time = time.time()
        if current_time - last_save_time >= save_interval or i == total_clubs:
            if save_progress(teams_output_file, processed_teams) and save_progress(players_output_file,
                                                                                   processed_players):
                print(f"💾 Прогресс сохранен ({i}/{total_clubs} клубов)")
                last_save_time = current_time

        if i % 5 == 0 or i == total_clubs:
            print(
                f"📊 Прогресс: {i}/{total_clubs}, Успешно: {success_count}, Ошибок: {error_count}, 503: {rate_limit_count}")

    # Финальное сохранение
    if save_progress(teams_output_file, processed_teams) and save_progress(players_output_file, processed_players):
        print("💾 Финальный результат сохранен")

    total_time = time.time() - start_time
    print(f"\n🎯 Итоговая статистика:")
    print(f"   ✅ Успешно обработано: {success_count}")
    print(f"   ❌ Ошибок: {error_count}")
    print(f"   🔴 503 ошибок: {rate_limit_count}")
    print(f"   📊 Уже было обработано: {total_all_clubs - total_clubs}")
    print(f"   ⏱️  Время обработки: {total_time / 60:.1f} минут")


def main():
    """Основная функция"""
    # Конфигурация
    input_filename = "tm_competitions.json"  # Файл с данными о лигах
    teams_output_filename = "tm_teams.json"  # Выходной файл для команд
    players_output_filename = "tm_players.json"  # Выходной файл для игроков
    season_id = "2024"  # Сезон

    # Проверяем существование входного файла
    if not os.path.exists(input_filename):
        print(f"❌ Файл {input_filename} не найден!")
        print("Сначала запустите скрипт для получения данных о лигах")
        return

    print(f"⚽ Начинаем обработку клубов из файла {input_filename}")
    print(f"📅 Сезон: {season_id}")
    print(f"📁 Выходные файлы: {teams_output_filename}, {players_output_filename}")
    print("⚠️  Используем консервативные настройки для избежания 503 ошибок")

    # Обрабатываем данные о игроках клубов с увеличенными задержками
    process_clubs_players(input_filename, teams_output_filename, players_output_filename, season_id, max_workers=1,
                          delay=5.0)

    print("\n🎉 Все задачи завершены!")
    print(f"Данные о командах сохранены в {teams_output_filename}")
    print(f"Данные об игроках сохранены в {players_output_filename}")


if __name__ == "__main__":
    main()