import json
import re


def extract_id_from_url(url, pattern):
    """Извлекает ID из URL с помощью регулярного выражения"""
    match = re.search(pattern, url)
    return match.group(1) if match else None


def process_players(input_file, output_file):
    # Чтение исходного JSON файла
    with open(input_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    # Регулярные выражения для извлечения ID
    transfermarkt_pattern = r'/spieler/(\d+)'
    sofifa_id_pattern = r'/player/(\d+)/'
    league_id_pattern = r'/league/(\d+)'
    team_id_pattern = r'/team/(\d+)/'

    # Обработка каждого игрока
    for player in players:
        # 1. Добавляем transfermarkt_id
        if 'transfermarkt_url' in player and player['transfermarkt_url']:
            player['transfermarkt_id'] = extract_id_from_url(player['transfermarkt_url'], transfermarkt_pattern)

        # 2. Добавляем id (из sofifa url)
        if 'url' in player and player['url']:
            player['id'] = extract_id_from_url(player['url'], sofifa_id_pattern)

        # 3. Добавляем league_id
        if 'league_url' in player and player['league_url']:
            player['league_id'] = extract_id_from_url(player['league_url'], league_id_pattern)

        # 4. Добавляем team_id
        if 'team_url' in player and player['team_url']:
            player['team_id'] = extract_id_from_url(player['team_url'], team_id_pattern)

    # Сохранение результата в новый файл
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(players, f, indent=2, ensure_ascii=False)

    print(f"Обработка завершена. Результат сохранен в {output_file}")
    print(f"Обработано игроков: {len(players)}")


# Запуск обработки
if __name__ == "__main__":
    input_filename = "sofifa_english_clubs_players_25.json"
    output_filename = "sofifa_english_clubs_players_25_with_ids.json"

    process_players(input_filename, output_filename)