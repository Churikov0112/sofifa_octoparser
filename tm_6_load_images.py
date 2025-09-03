import json
import requests
import os
import time
from urllib.parse import urlparse
from pathlib import Path
import random
import concurrent.futures
from threading import Lock
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ImageDownloader:
    def __init__(self, max_workers=5, max_retries=3, base_delay=0.5):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.lock = Lock()
        self.success_count = 0
        self.error_count = 0

    def _download_with_retry(self, url, output_path, item_id, item_type="player"):
        """Загружает изображение с механизмом повторных попыток"""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=15, stream=True)
                if response.status_code == 200:
                    # Сохраняем изображение
                    with open(output_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    logger.info(f"✅ Успешно {item_type}: {item_id}, попытка {attempt + 1}")
                    return True

                elif response.status_code == 404:
                    logger.warning(f"⚠️  Изображение не найдено: {item_type} {item_id}, URL: {url}")
                    return False  # Не повторяем для 404 ошибок

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    # Экспоненциальная backoff задержка
                    delay = self.base_delay * (2 ** attempt) + random.uniform(0.1, 0.5)
                    logger.warning(f"⚠️  Попытка {attempt + 1}/{self.max_retries} для {item_type} {item_id}: {e}")
                    logger.info(f"⏳ Ждем {delay:.1f} секунд перед повторной попыткой...")
                    time.sleep(delay)
                else:
                    logger.error(f"❌ Ошибка после {self.max_retries} попыток для {item_type} {item_id}: {e}")

        return False

    def download_player_image(self, player_info, output_dir, downloaded_images):
        """Загружает изображение для одного игрока"""
        player_id = player_info['id']
        image_url = player_info['imageUrl']

        # Пропускаем если уже скачано
        if player_id in downloaded_images:
            return True, player_id, "already_downloaded"

        # Извлекаем расширение файла из URL
        parsed_url = urlparse(image_url)
        filename = os.path.basename(parsed_url.path)
        name, ext = os.path.splitext(filename)
        output_filename = f"{player_id}{ext}"
        output_path = os.path.join(output_dir, output_filename)

        # Пробуем сначала big версию
        big_url = image_url.replace('/header/', '/big/')
        urls_to_try = [big_url, image_url]

        success = False
        for url in urls_to_try:
            if self._download_with_retry(url, output_path, player_id, "player"):
                success = True
                break

        with self.lock:
            if success:
                self.success_count += 1
                downloaded_images.add(player_id)
                return True, player_id, "success"
            else:
                self.error_count += 1
                return False, player_id, "error"

    def download_team_logo(self, team_info, output_dir, downloaded_teams):
        """Загружает логотип для одной команды"""
        team_id = team_info['id']

        # Пропускаем если уже скачано
        if team_id in downloaded_teams:
            return True, team_id, "already_downloaded"

        # Формируем URL для логотипа команды
        logo_url = f"https://tmssl.akamaized.net/images/wappen/big/{team_id}.png"
        output_filename = f"{team_id}.png"
        output_path = os.path.join(output_dir, output_filename)

        # Загружаем логотип
        success = self._download_with_retry(logo_url, output_path, team_id, "team")

        with self.lock:
            if success:
                self.success_count += 1
                downloaded_teams.add(team_id)
                return True, team_id, "success"
            else:
                self.error_count += 1
                return False, team_id, "error"

    def process_items(self, items, process_func, output_dir, downloaded_items, total_items, item_type="players"):
        """Обрабатывает элементы в многопоточном режиме"""
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Создаем future для каждого элемента
            future_to_item = {
                executor.submit(process_func, item, output_dir, downloaded_items): item
                for item in items
            }

            # Обрабатываем результаты по мере их завершения
            for i, future in enumerate(concurrent.futures.as_completed(future_to_item), 1):
                item_info = future_to_item[future]
                item_id = item_info['id']

                try:
                    success, pid, status = future.result()
                    results.append((success, pid, status))

                    # Периодически выводим прогресс
                    if i % 10 == 0 or i == total_items:
                        logger.info(
                            f"📊 Прогресс {item_type}: {i}/{total_items}, Успешно: {self.success_count}, Ошибок: {self.error_count}")

                except Exception as e:
                    with self.lock:
                        self.error_count += 1
                    logger.error(f"❌ Неожиданная ошибка для {item_type[:-1]} {item_id}: {e}")
                    results.append((False, item_id, "exception"))

        return results


def load_json_data(input_file):
    """Загружает данные из JSON файла"""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"❌ Файл {input_file} не найден!")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка чтения JSON из файла {input_file}: {e}")
        return None


def get_downloaded_items(output_dir, valid_extensions=('.jpg', '.jpeg', '.png', '.webp')):
    """Получает список уже скачанных элементов"""
    downloaded = set()
    if os.path.exists(output_dir):
        for filename in os.listdir(output_dir):
            if filename.endswith(valid_extensions):
                # Извлекаем ID из имени файла (без расширения)
                item_id = os.path.splitext(filename)[0]
                downloaded.add(item_id)
    return downloaded


def create_output_dir(output_dir):
    """Создает папку для сохранения"""
    try:
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"📁 Папка создана/проверена: {output_dir}")
        return True
    except OSError as e:
        logger.error(f"❌ Не удалось создать папку {output_dir}: {e}")
        return False


def download_players(downloader, players_data, players_output_dir):
    """Загружает изображения игроков"""
    logger.info("👥 Начинаем загрузку изображений игроков...")

    # Фильтруем игроков, у которых есть imageUrl
    players_to_process = []
    for player in players_data:
        if isinstance(player, dict):
            player_id = player.get('id')
            image_url = player.get('imageUrl')

            if player_id and image_url:
                players_to_process.append({
                    'id': player_id,
                    'imageUrl': image_url
                })

    total_players = len(players_to_process)
    downloaded_players = get_downloaded_items(players_output_dir)

    logger.info(f"📊 Игроков с картинками: {total_players}")
    logger.info(f"📊 Уже скачано игроков: {len(downloaded_players)}")

    if total_players == 0:
        logger.info("🎉 Все изображения игроков уже скачаны!")
        return [], 0, 0

    # Сбрасываем счетчики для игроков
    downloader.success_count = 0
    downloader.error_count = 0

    # Запускаем загрузку игроков
    results = downloader.process_items(
        players_to_process,
        downloader.download_player_image,
        players_output_dir,
        downloaded_players,
        total_players,
        "players"
    )

    return results, downloader.success_count, downloader.error_count


def download_teams(downloader, teams_data, teams_output_dir):
    """Загружает логотипы команд"""
    logger.info("🏟️  Начинаем загрузку логотипов команд...")

    # Фильтруем команды с ID
    teams_to_process = []
    for team in teams_data:
        if isinstance(team, dict):
            team_id = team.get('id')
            if team_id:
                teams_to_process.append({
                    'id': team_id,
                    'name': team.get('name', 'Unknown')
                })

    total_teams = len(teams_to_process)
    downloaded_teams = get_downloaded_items(teams_output_dir, ('.png',))

    logger.info(f"📊 Команд для загрузки: {total_teams}")
    logger.info(f"📊 Уже скачано команд: {len(downloaded_teams)}")

    if total_teams == 0:
        logger.info("🎉 Все логотипы команд уже скачаны!")
        return [], 0, 0

    # Сбрасываем счетчики для команд
    downloader.success_count = 0
    downloader.error_count = 0

    # Запускаем загрузку команд
    results = downloader.process_items(
        teams_to_process,
        downloader.download_team_logo,
        teams_output_dir,
        downloaded_teams,
        total_teams,
        "teams"
    )

    return results, downloader.success_count, downloader.error_count


def main():
    """Основная функция"""
    # Конфигурация
    players_input_file = "tm_players_profiles.json"
    teams_input_file = "tm_teams.json"
    players_output_dir = "tm_players"
    teams_output_dir = "tm_teams"

    # Параметры многопоточности
    max_workers = 8
    max_retries = 3
    base_delay = 0.5

    # Создаем папки для сохранения
    if not create_output_dir(players_output_dir) or not create_output_dir(teams_output_dir):
        return

    # Создаем загрузчик
    downloader = ImageDownloader(
        max_workers=max_workers,
        max_retries=max_retries,
        base_delay=base_delay
    )

    start_time = time.time()
    report_data = {
        "players": {},
        "teams": {},
        "summary": {}
    }

    # Загрузка игроков
    players_data = load_json_data(players_input_file)
    if players_data:
        player_results, player_success, player_errors = download_players(downloader, players_data, players_output_dir)
        report_data["players"] = {
            "processed": len(player_results),
            "successful": player_success,
            "failed": player_errors,
            "already_downloaded": len(get_downloaded_items(players_output_dir)) - player_success
        }

    # Загрузка команд
    teams_data = load_json_data(teams_input_file)
    if teams_data:
        team_results, team_success, team_errors = download_teams(downloader, teams_data, teams_output_dir)
        report_data["teams"] = {
            "processed": len(team_results),
            "successful": team_success,
            "failed": team_errors,
            "already_downloaded": len(get_downloaded_items(teams_output_dir, ('.png',))) - team_success
        }

    total_time = time.time() - start_time

    # Создаем итоговый отчет
    total_processed = report_data["players"].get("processed", 0) + report_data["teams"].get("processed", 0)
    total_success = report_data["players"].get("successful", 0) + report_data["teams"].get("successful", 0)
    total_errors = report_data["players"].get("failed", 0) + report_data["teams"].get("failed", 0)

    print(f"\n🎯 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"👥 ИГРОКИ:")
    print(f"   ✅ Успешно: {report_data['players'].get('successful', 0)}")
    print(f"   ❌ Ошибок: {report_data['players'].get('failed', 0)}")
    print(f"   ⏭️  Пропущено: {report_data['players'].get('already_downloaded', 0)}")
    print(f"🏟️  КОМАНДЫ:")
    print(f"   ✅ Успешно: {report_data['teams'].get('successful', 0)}")
    print(f"   ❌ Ошибок: {report_data['teams'].get('failed', 0)}")
    print(f"   ⏭️  Пропущено: {report_data['teams'].get('already_downloaded', 0)}")
    print(f"📊 ОБЩЕЕ:")
    print(f"   ⚡ Потоков: {max_workers}")
    print(f"   ⏱️  Время: {total_time:.1f} сек ({total_time / 60:.1f} мин)")
    print(f"   🚀 Скорость: {total_processed / total_time:.2f} элементов/сек")
    print(f"📁 Папки:")
    print(f"   👥 Игроки: {players_output_dir}")
    print(f"   🏟️  Команды: {teams_output_dir}")


if __name__ == "__main__":
    main()