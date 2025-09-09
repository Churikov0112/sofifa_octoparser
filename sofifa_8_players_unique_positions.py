import json


def get_unique_positions(filename):
    # Чтение JSON файла
    with open(filename, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Извлечение уникальных позиций
    unique_positions = set()

    for player in data:
        if 'best_position_short' in player:
            unique_positions.add(player['best_position_short'])

    # Преобразование в отсортированный список для красивого вывода
    sorted_positions = sorted(list(unique_positions))

    return sorted_positions


# Использование функции
if __name__ == "__main__":
    filename = "sofifa_players.json"

    try:
        unique_positions = get_unique_positions(filename)

        print("Уникальные позиции (best_position_short):")
        print("-" * 40)

        for i, position in enumerate(unique_positions, 1):
            print(f"{i}. {position}")

        print(f"\nВсего уникальных позиций: {len(unique_positions)}")

    except FileNotFoundError:
        print(f"Ошибка: Файл '{filename}' не найден.")
    except json.JSONDecodeError:
        print(f"Ошибка: Файл '{filename}' имеет неверный JSON формат.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")