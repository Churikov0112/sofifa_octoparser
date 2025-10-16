import json
import os
import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PlayerImageDownloader:
    def __init__(self, output_folder, max_workers=5, timeout=30, max_retries=3):
        self.output_folder = output_folder
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = self._create_session()

        # Создаем папку для сохранения изображений
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

    def _create_session(self):
        """Создает сессию с настройками для избежания блокировок"""
        session = requests.Session()

        # Настройка повторных попыток
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Заголовки для имитации браузера
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        return session

    def _download_image(self, player_id, player_name, img_url):
        """Скачивает изображение игрока"""
        try:
            img_response = self.session.get(img_url, timeout=self.timeout)
            img_response.raise_for_status()

            # Получаем расширение файла из URL
            parsed_url = urlparse(img_url)
            extension = os.path.splitext(parsed_url.path)[1]

            # Формируем имя файла
            filename = f"{player_id}{extension}"
            filepath = os.path.join(self.output_folder, filename)

            # Сохраняем изображение
            with open(filepath, 'wb') as img_file:
                img_file.write(img_response.content)

            return True, filename
        except Exception as e:
            return False, str(e)

    def _process_player(self, player):
        """Обрабатывает одного игрока"""
        player_id = player['id']
        player_name = player['name']

        logger.info(f"Обрабатываем игрока: {player_name} (ID: {player_id})")

        # URL страницы игрока
        url = f"https://www.transfermarkt.com/somebody/profil/spieler/{player_id}"

        try:
            # Загружаем страницу
            response = self.session.get(url, timeout=self.timeout)

            # Проверяем на блокировку
            if response.status_code == 503:
                logger.warning(f"Обнаружена блокировка 503 для игрока {player_name}")
                return False, player_id, "Blocked by server (503)"

            response.raise_for_status()

            # Парсим HTML
            soup = BeautifulSoup(response.content, 'html.parser')

            # Ищем элемент с изображением
            img_tag = soup.find('img', class_='data-header__profile-image')

            if img_tag and img_tag.get('src'):
                # Получаем URL изображения
                img_url = img_tag['src']

                # Заменяем 'header' на 'big' в URL
                big_img_url = img_url.replace('header', 'big')

                # Скачиваем изображение
                success, result = self._download_image(player_id, player_name, big_img_url)

                if success:
                    logger.info(f"✓ Скачано: {result}")
                    return True, player_id, result
                else:
                    logger.error(f"✗ Ошибка скачивания для игрока {player_name}: {result}")
                    return False, player_id, result
            else:
                logger.warning(f"✗ Изображение не найдено для игрока {player_name}")
                return False, player_id, "Image not found"

        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Ошибка сети для игрока {player_name}: {e}")
            return False, player_id, str(e)
        except Exception as e:
            logger.error(f"✗ Неожиданная ошибка для игрока {player_name}: {e}")
            return False, player_id, str(e)

        finally:
            # Случайная пауза между запросами
            time.sleep(random.uniform(1, 3))

    def download_images(self, json_file):
        """Основной метод для скачивания изображений"""
        # Читаем JSON файл
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                players = json.load(f)
        except Exception as e:
            logger.error(f"Ошибка чтения JSON файла: {e}")
            return

        logger.info(f"Начинаем обработку {len(players)} игроков с {self.max_workers} потоками")

        # Используем ThreadPoolExecutor для многопоточности
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Создаем futures для каждого игрока
            future_to_player = {
                executor.submit(self._process_player, player): player
                for player in players
            }

            # Обрабатываем результаты по мере завершения
            success_count = 0
            total_count = len(players)

            for future in as_completed(future_to_player):
                player = future_to_player[future]
                try:
                    success, player_id, result = future.result()
                    if success:
                        success_count += 1
                except Exception as e:
                    logger.error(f"Ошибка в потоке для игрока {player['name']}: {e}")

        logger.info(f"Обработка завершена! Успешно: {success_count}/{total_count}")


def download_player_images(json_file, output_folder, max_workers=5):
    """Функция для запуска скачивания"""
    downloader = PlayerImageDownloader(output_folder, max_workers)
    downloader.download_images(json_file)


if __name__ == "__main__":
    # Использование
    download_player_images(
        json_file='tm_players.json',
        output_folder='tm_players',
        max_workers=5  # Можно регулировать количество потоков
    )