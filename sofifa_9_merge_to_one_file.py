import json
from pathlib import Path
from collections import defaultdict


def merge_football_data():
    # Загрузка всех JSON файлов
    with open('sofifa_competitions.json', 'r', encoding='utf-8') as f:
        sofifa_competitions = json.load(f)

    with open('sofifa_teams.json', 'r', encoding='utf-8') as f:
        sofifa_teams = json.load(f)

    with open('sofifa_players.json', 'r', encoding='utf-8') as f:
        sofifa_players = json.load(f)

    with open('sofifa_players_ratings.json', 'r', encoding='utf-8') as f:
        sofifa_ratings = json.load(f)

    with open('sofifa_tm_players.json', 'r', encoding='utf-8') as f:
        tm_players = json.load(f)

    with open('sofifa_tm_market_values.json', 'r', encoding='utf-8') as f:
        tm_market_values = json.load(f)

    # Создаем словари для быстрого поиска
    competitions_dict = {comp['id']: comp for comp in sofifa_competitions}
    teams_dict = {team['id']: team for team in sofifa_teams}
    sofifa_players_dict = {player['id']: player for player in sofifa_players}
    sofifa_ratings_dict = {rating['player_id']: rating for rating in sofifa_ratings}
    tm_players_dict = {player['id']: player for player in tm_players}
    tm_market_values_dict = {mv['id']: mv for mv in tm_market_values}

    # Обрабатываем команды - добавляем transfermarkt_id из игроков
    team_tm_ids = defaultdict(set)
    for tm_player in tm_players:
        if 'team_id' in tm_player and tm_player['team_id']:
            team_tm_ids[tm_player['team_id']].add(tm_player.get('id', ''))

    enhanced_teams = []
    for team in sofifa_teams:
        team_data = team.copy()
        # Добавляем transfermarkt_id из связанных игроков (если есть)
        if team['id'] in team_tm_ids:
            team_data['transfermarkt_id'] = next(iter(team_tm_ids[team['id']]), None)
        enhanced_teams.append(team_data)

    # Обрабатываем лиги
    enhanced_competitions = []
    for comp in sofifa_competitions:
        comp_data = comp.copy()
        enhanced_competitions.append(comp_data)

    # Обрабатываем игроков - ТОЛЬКО из SOFIFA (1292 игрока)
    merged_players = []
    players_with_sofifa = 0
    players_with_transfermarkt = 0

    for sofifa_player in sofifa_players:
        player_id = sofifa_player['id']
        tm_player_id = sofifa_player.get('transfermarkt_id')

        # Основная структура игрока
        player_data = {}

        # SOFIFA данные
        sofifa_data = {
            'id': sofifa_player.get('id'),
            'team_id': sofifa_player.get('team_id'),
            'name': sofifa_player.get('name'),
            'image_url': sofifa_player.get('image_url'),
            'kit_number': sofifa_player.get('kit_number'),
            'best_position_short': sofifa_player.get('best_position_short')
        }

        # Добавляем рейтинги если есть
        ratings = sofifa_ratings_dict.get(player_id)
        if ratings:
            players_with_sofifa += 1
            ratings_converted = {}
            for key, value in ratings.items():
                if key != 'player_id' and value is not None:
                    try:
                        ratings_converted[key] = int(value)
                    except (ValueError, TypeError):
                        ratings_converted[key] = value
            sofifa_data['ratings'] = ratings_converted

        player_data['sofifa'] = sofifa_data

        # TRANSFERMARKT данные (если есть)
        transfermarkt_data = {}
        tm_player = None

        # Ищем игрока в Transfermarkt по transfermarkt_id из SOFIFA
        if tm_player_id and tm_player_id in tm_players_dict:
            tm_player = tm_players_dict[tm_player_id]
        # Если не нашли по ID, пробуем найти по имени (резервный вариант)
        else:
            # Простой поиск по имени (можно улучшить при необходимости)
            sofifa_name = sofifa_player.get('name', '').lower()
            for tm_player_candidate in tm_players:
                if tm_player_candidate.get('name', '').lower() == sofifa_name:
                    tm_player = tm_player_candidate
                    break

        if tm_player:
            players_with_transfermarkt += 1
            transfermarkt_data = {
                'id': tm_player.get('id'),
                'name': tm_player.get('name'),
                'position': tm_player.get('position'),
                'dateOfBirth': tm_player.get('dateOfBirth'),
                'age': tm_player.get('age'),
                'nationality': tm_player.get('nationality'),
                'height': tm_player.get('height'),
                'foot': tm_player.get('foot'),
                'marketValue': tm_player.get('marketValue')
            }

            # Добавляем историю рыночной стоимости если есть
            market_value_data = tm_market_values_dict.get(tm_player.get('id'))
            if market_value_data:
                transfermarkt_data['marketValue'] = market_value_data.get('marketValue')
                transfermarkt_data['marketValueHistory'] = market_value_data.get('marketValueHistory', [])
                transfermarkt_data['updatedAt'] = market_value_data.get('updatedAt')

        if transfermarkt_data:
            player_data['transfermarkt'] = transfermarkt_data

        merged_players.append(player_data)

    # Создаем финальную структуру
    output_data = {
        'competitions': enhanced_competitions,
        'teams': enhanced_teams,
        'players': merged_players,
        'metadata': {
            'players_with_sofifa': players_with_sofifa,
            'players_with_transfermarkt': players_with_transfermarkt
        }
    }

    # Сохраняем результат
    with open('sofifa_football_data.json', 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"Успешно объединено:")
    print(f"- Лиг: {len(enhanced_competitions)}")
    print(f"- Команд: {len(enhanced_teams)}")
    print(f"- Игроков: {len(merged_players)}")
    print(f"- Игроков с SOFIFA данными: {players_with_sofifa}")
    print(f"- Игроков с Transfermarkt данными: {players_with_transfermarkt}")


if __name__ == '__main__':
    merge_football_data()