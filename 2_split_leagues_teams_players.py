import json
from collections import defaultdict


def split_into_separate_files(input_file, output_prefix):
    """
    Разделяет данные на три отдельных файла: лиги, команды, игроки
    """
    # Чтение исходного JSON файла
    with open(input_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    # Создаем отдельные коллекции
    leagues = {}
    teams = {}
    players_list = []

    # Собираем уникальные лиги и команды
    for player in players:
        # Обрабатываем лигу
        league_id = player.get('league_id')
        if league_id and league_id not in leagues:
            leagues[league_id] = {
                'league_id': league_id,
                'league_name': player.get('league_name'),
                'league_url': player.get('league_url'),
                'league_logo_url': player.get('league_logo_url')
            }

        # Обрабатываем команду
        team_id = player.get('team_id')
        if team_id and team_id not in teams:
            teams[team_id] = {
                'team_id': team_id,
                'team_name': player.get('team_name'),
                'team_url': player.get('team_url'),
                'team_logo_url': player.get('team_logo_url'),
                'league_id': league_id  # ссылка на лигу
            }

        # Создаем запись игрока (убираем дублирующиеся данные)
        player_data = {key: value for key, value in player.items()
                       if not key.startswith(('league_', 'team_'))}
        player_data['team_id'] = team_id  # ссылка на команду

        players_list.append(player_data)

    # Сохраняем лиги в отдельный файл
    leagues_filename = f"{output_prefix}_leagues.json"
    with open(leagues_filename, 'w', encoding='utf-8') as f:
        json.dump(list(leagues.values()), f, indent=2, ensure_ascii=False)

    # Сохраняем команды в отдельный файл
    teams_filename = f"{output_prefix}_teams.json"
    with open(teams_filename, 'w', encoding='utf-8') as f:
        json.dump(list(teams.values()), f, indent=2, ensure_ascii=False)

    # Сохраняем игроков в отдельный файл
    players_filename = f"{output_prefix}_players.json"
    with open(players_filename, 'w', encoding='utf-8') as f:
        json.dump(players_list, f, indent=2, ensure_ascii=False)

    # Также создаем файл с мета-информацией о связях
    meta_info = {
        "structure": {
            "leagues": "Уникальные лиги с основными данными",
            "teams": "Уникальные команды с ссылкой на league_id",
            "players": "Игроки с ссылкой на team_id"
        },
        "relationships": {
            "league -> teams": "Одна лига может содержать много команд",
            "team -> players": "Одна команда может содержать много игроков",
            "team -> league": "Каждая команда принадлежит одной лиге (через league_id)"
        },
        "statistics": {
            "total_leagues": len(leagues),
            "total_teams": len(teams),
            "total_players": len(players_list),
            "files_created": [
                leagues_filename,
                teams_filename,
                players_filename
            ]
        }
    }

    meta_filename = f"{output_prefix}_meta.json"
    with open(meta_filename, 'w', encoding='utf-8') as f:
        json.dump(meta_info, f, indent=2, ensure_ascii=False)

    print("✅ Разделение данных завершено!")
    print("📊 Статистика:")
    print(f"   Лиг: {len(leagues)}")
    print(f"   Команд: {len(teams)}")
    print(f"   Игроков: {len(players_list)}")
    print("📁 Созданные файлы:")
    print(f"   • {leagues_filename}")
    print(f"   • {teams_filename}")
    print(f"   • {players_filename}")
    print(f"   • {meta_filename} (мета-информация)")

    return leagues_filename, teams_filename, players_filename, meta_filename


# Дополнительная функция для чтения и объединения данных
def read_combined_data(leagues_file, teams_file, players_file):
    """
    Читает данные из отдельных файлов и возвращает объединенную структуру
    """
    with open(leagues_file, 'r', encoding='utf-8') as f:
        leagues = {league['league_id']: league for league in json.load(f)}

    with open(teams_file, 'r', encoding='utf-8') as f:
        teams = {team['team_id']: team for team in json.load(f)}

    with open(players_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    # Обогащаем данные игроков информацией о командах и лигах
    enriched_players = []
    for player in players:
        team_id = player.get('team_id')
        team = teams.get(team_id, {})
        league_id = team.get('league_id')
        league = leagues.get(league_id, {})

        enriched_player = player.copy()
        enriched_player['team_info'] = {
            'team_name': team.get('team_name'),
            'team_logo_url': team.get('team_logo_url')
        }
        enriched_player['league_info'] = {
            'league_name': league.get('league_name'),
            'league_logo_url': league.get('league_logo_url')
        }

        enriched_players.append(enriched_player)

    return {
        'leagues': list(leagues.values()),
        'teams': list(teams.values()),
        'players': enriched_players
    }


# Функция для быстрого поиска данных
def create_lookup_indexes(leagues_file, teams_file, players_file):
    """
    Создает индексы для быстрого поиска данных
    """
    with open(leagues_file, 'r', encoding='utf-8') as f:
        leagues_data = json.load(f)

    with open(teams_file, 'r', encoding='utf-8') as f:
        teams_data = json.load(f)

    with open(players_file, 'r', encoding='utf-8') as f:
        players_data = json.load(f)

    # Создаем индексы
    leagues_by_id = {league['league_id']: league for league in leagues_data}
    leagues_by_name = {league['league_name'].lower(): league for league in leagues_data}

    teams_by_id = {team['team_id']: team for team in teams_data}
    teams_by_name = {team['team_name'].lower(): team for team in teams_data}
    teams_by_league = defaultdict(list)
    for team in teams_data:
        teams_by_league[team['league_id']].append(team)

    players_by_id = {player['id']: player for player in players_data}
    players_by_team = defaultdict(list)
    players_by_name = {}
    for player in players_data:
        players_by_team[player['team_id']].append(player)
        # Добавляем поиск по имени и полному имени
        players_by_name[player['name'].lower()] = player
        if 'full_name' in player:
            players_by_name[player['full_name'].lower()] = player

    indexes = {
        'leagues': {
            'by_id': leagues_by_id,
            'by_name': leagues_by_name
        },
        'teams': {
            'by_id': teams_by_id,
            'by_name': teams_by_name,
            'by_league': dict(teams_by_league)
        },
        'players': {
            'by_id': players_by_id,
            'by_team': dict(players_by_team),
            'by_name': players_by_name
        }
    }

    # Сохраняем индексы
    indexes_filename = "sofifa_indexes.json"
    with open(indexes_filename, 'w', encoding='utf-8') as f:
        json.dump(indexes, f, indent=2, ensure_ascii=False)

    print(f"✅ Индексы созданы и сохранены в {indexes_filename}")
    return indexes


# Запуск обработки
if __name__ == "__main__":
    input_filename = "sofifa_english_clubs_players_25_with_ids.json"
    output_prefix = "sofifa"

    # Разделяем данные на отдельные файлы
    leagues_file, teams_file, players_file, meta_file = split_into_separate_files(input_filename, output_prefix)

    # Создаем индексы для быстрого поиска
    indexes = create_lookup_indexes(leagues_file, teams_file, players_file)

    print("\n🎯 Теперь вы можете:")
    print("   • Быстро искать игроков по ID, имени или команде")
    print("   • Получать все команды определенной лиги")
    print("   • Анализировать данные независимо по каждому типу сущностей")
    print("   • Эффективно использовать память при работе с данными")