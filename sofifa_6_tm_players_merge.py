import json
from pathlib import Path


def match_players():
    # Загрузка данных из файлов
    try:
        with open('sofifa_players.json', 'r', encoding='utf-8') as f:
            sofifa_players = json.load(f)
    except FileNotFoundError:
        print("❌ Файл sofifa_players.json не найден")
        return
    except json.JSONDecodeError:
        print("❌ Ошибка при чтении sofifa_players.json")
        return

    try:
        with open('tm_players.json', 'r', encoding='utf-8') as f:
            tm_players = json.load(f)
    except FileNotFoundError:
        print("❌ Файл tm_players.json не найден")
        return
    except json.JSONDecodeError:
        print("❌ Ошибка при чтении tm_players.json")
        return

    # Создаем словарь для быстрого поиска игроков по transfermarkt_id
    tm_players_dict = {player['id']: player for player in tm_players}

    # Ищем совпадения
    matched_players = []
    matched_count = 0
    total_sofifa_players = len(sofifa_players)

    for sofifa_player in sofifa_players:
        transfermarkt_id = sofifa_player.get('transfermarkt_id')

        if transfermarkt_id and transfermarkt_id in tm_players_dict:
            matched_players.append(tm_players_dict[transfermarkt_id])
            matched_count += 1

    # Сохраняем результат в новый файл
    try:
        with open('sofifa_tm_players.json', 'w', encoding='utf-8') as f:
            json.dump(matched_players, f, ensure_ascii=False, indent=2)
        print("✅ Файл sofifa_tm_players.json успешно создан")
    except Exception as e:
        print(f"❌ Ошибка при сохранении файла: {e}")
        return

    # Выводим статистику
    print("\n📊 Статистика совпадений:")
    print(f"Всего игроков в sofifa_players.json: {total_sofifa_players}")
    print(f"Всего игроков в tm_players.json: {len(tm_players)}")
    print(f"Найдено совпадений: {matched_count}")
    print(f"Процент совпадений: {(matched_count / total_sofifa_players * 100):.1f}%")


if __name__ == "__main__":
    match_players()