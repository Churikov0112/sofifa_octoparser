import json
from collections import defaultdict
import re


def extract_kit_number(kit_number_str):
    """Извлекает номер из строки 'Kit number X' с помощью регулярного выражения"""
    if not kit_number_str:
        return None

    # Ищем число в строке
    match = re.search(r'\d+', kit_number_str)
    if match:
        return match.group()
    return None


def split_into_separate_files(input_file, output_prefix):
    """
    Разделяет данные на четыре отдельных файла: лиги, команды, игроки, статистика игроков
    """
    # Чтение исходного JSON файла
    with open(input_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    # Создаем отдельные коллекции
    competitions = {}
    teams = {}
    players_list = []
    players_ratings_list = []

    # Собираем уникальные лиги и команды, а также создаем списки ID
    for player in players:
        # Обрабатываем лигу (competition)
        league_id = player.get('league_id')
        if league_id and league_id not in competitions:
            competitions[league_id] = {
                'id': league_id,
                'name': player.get('league_name'),
                'logo_url': player.get('league_logo_url'),
                'teams_ids': []  # Будет заполнен позже
            }

        # Обрабатываем команду
        team_id = player.get('team_id')
        if team_id and team_id not in teams:
            teams[team_id] = {
                'id': team_id,
                'name': player.get('team_name'),
                'transfermarkt_id': player.get('team_transfermarkt_id'),
                'logo_url': player.get('team_logo_url'),
                'competition_id': league_id,
                'players_ids': []  # Будет заполнен позже
            }

        # Создаем запись игрока (без статистики)
        player_id = player.get('id')
        player_data = {
            'id': player_id,
            'team_id': team_id,
            'transfermarkt_id': player.get('transfermarkt_id'),
            'name': player.get('name'),
            'image_url': player.get('image_url'),
            'kit_number': extract_kit_number(player.get('kit_number')),
            'best_position_short': player.get('best_position')
        }

        # Создаем запись статистики игрока
        ratings_data = {
            'player_id': player_id,
            'overall': player.get('overall'),
            'potential': player.get('potential'),
            'crossing': player.get('crossing'),
            'finishing': player.get('finishing'),
            'heading_accuracy': player.get('heading_accuracy'),
            'shot_passing': player.get('shot_passing'),
            'volleys': player.get('volleys'),
            'aggression': player.get('aggression'),
            'interceptions': player.get('interceptions'),
            'att_position': player.get('att_position'),
            'vision': player.get('vision'),
            'penalties': player.get('penalties'),
            'composure': player.get('composure'),
            'dribbling': player.get('dribbling'),
            'curve': player.get('curve'),
            'fk_accuracy': player.get('fk_accuracy'),
            'long_passing': player.get('long_passing'),
            'ball_control': player.get('ball_control'),
            'defensive_awareness': player.get('defensive_awareness'),
            'standing_tackle': player.get('standing_tackle'),
            'sliding_tackle': player.get('sliding_tackle'),
            'acceleration': player.get('acceleration'),
            'sprint_speed': player.get('sprint_speed'),
            'agility': player.get('agility'),
            'reactions': player.get('reactions'),
            'balance': player.get('balance'),
            'gk_diving': player.get('gk_diving'),
            'gk_handling': player.get('gk_handling'),
            'gk_kicking': player.get('gk_kicking'),
            'gk_positioning': player.get('gk_positioning'),
            'gk_reflexes': player.get('gk_reflexes'),
            'shot_power': player.get('shot_power'),
            'jumping': player.get('jumping'),
            'stamina': player.get('stamina'),
            'strength': player.get('strength'),
            'long_shots': player.get('long_shots'),
            'best_overall': player.get('best_overall')
        }

        # Удаляем None значения из статистики
        ratings_data = {k: v for k, v in ratings_data.items() if v is not None}

        players_list.append(player_data)
        players_ratings_list.append(ratings_data)

    # Теперь заполняем списки ID для competitions и teams
    for player in players_list:
        team_id = player.get('team_id')
        if team_id and team_id in teams:
            # Добавляем player_id в список команды
            if player['id'] not in teams[team_id]['players_ids']:
                teams[team_id]['players_ids'].append(player['id'])

            # Добавляем team_id в список лиги
            competition_id = teams[team_id].get('competition_id')
            if competition_id and competition_id in competitions:
                if team_id not in competitions[competition_id]['teams_ids']:
                    competitions[competition_id]['teams_ids'].append(team_id)

    # Сохраняем competitions в отдельный файл
    competitions_filename = f"{output_prefix}_competitions.json"
    with open(competitions_filename, 'w', encoding='utf-8') as f:
        json.dump(list(competitions.values()), f, indent=2, ensure_ascii=False)

    # Сохраняем команды в отдельный файл
    teams_filename = f"{output_prefix}_teams.json"
    with open(teams_filename, 'w', encoding='utf-8') as f:
        json.dump(list(teams.values()), f, indent=2, ensure_ascii=False)

    # Сохраняем игроков в отдельный файл
    players_filename = f"{output_prefix}_players.json"
    with open(players_filename, 'w', encoding='utf-8') as f:
        json.dump(players_list, f, indent=2, ensure_ascii=False)

    # Сохраняем статистику игроков в отдельный файл
    players_ratings_filename = f"{output_prefix}_players_ratings.json"
    with open(players_ratings_filename, 'w', encoding='utf-8') as f:
        json.dump(players_ratings_list, f, indent=2, ensure_ascii=False)

    print("✅ Разделение данных завершено!")
    print("📊 Статистика:")
    print(f"   Лиг: {len(competitions)}")
    print(f"   Команд: {len(teams)}")
    print(f"   Игроков: {len(players_list)}")
    print(f"   Статистик игроков: {len(players_ratings_list)}")
    print("📁 Созданные файлы:")
    print(f"   • {competitions_filename}")
    print(f"   • {teams_filename}")
    print(f"   • {players_filename}")
    print(f"   • {players_ratings_filename}")

    return competitions_filename, teams_filename, players_filename, players_ratings_filename


# Дополнительная функция для чтения и объединения данных
def read_combined_data(competitions_file, teams_file, players_file, players_ratings_file):
    """
    Читает данные из отдельных файлов и возвращает объединенную структуру
    """
    with open(competitions_file, 'r', encoding='utf-8') as f:
        competitions = {comp['id']: comp for comp in json.load(f)}

    with open(teams_file, 'r', encoding='utf-8') as f:
        teams = {team['id']: team for team in json.load(f)}

    with open(players_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    with open(players_ratings_file, 'r', encoding='utf-8') as f:
        players_ratings = {rating['player_id']: rating for rating in json.load(f)}

    # Обогащаем данные игроков информацией о командах, лигах и статистике
    enriched_players = []
    for player in players:
        player_id = player.get('id')
        team_id = player.get('team_id')
        team = teams.get(team_id, {})
        competition_id = team.get('competition_id')
        competition = competitions.get(competition_id, {})
        ratings = players_ratings.get(player_id, {})

        enriched_player = player.copy()
        enriched_player['team_info'] = {
            'name': team.get('name'),
            'logo_url': team.get('logo_url')
        }
        enriched_player['competition_info'] = {
            'name': competition.get('name'),
            'logo_url': competition.get('logo_url')
        }
        # Добавляем статистику, исключая player_id чтобы избежать дублирования
        ratings_without_id = {k: v for k, v in ratings.items() if k != 'player_id'}
        enriched_player['ratings'] = ratings_without_id

        enriched_players.append(enriched_player)

    return {
        'competitions': list(competitions.values()),
        'teams': list(teams.values()),
        'players': enriched_players,
        'players_ratings': list(players_ratings.values())
    }


# Запуск обработки
if __name__ == "__main__":
    input_filename = "sofifa_raw_data_with_ids.json"
    output_prefix = "sofifa"

    # Разделяем данные на отдельные файлы
    competitions_file, teams_file, players_file, players_ratings_file = split_into_separate_files(input_filename, output_prefix)
