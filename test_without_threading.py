import csv
import time
import json
from rosreestr2coord import Area

main_dict = {}

# Открытие CSV файла для чтения
csv_file_path = 'test_100.csv'

# Обработка CSV файла
with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)

    for row in reader:
        cad_num = row[0]
        try:
            start_time = time.time()

            area = Area(cad_num, with_proxy=True, use_cache=False)

            data = {
                "address": area.attrs.get('address'),
                "id": cad_num,
                "kvartal_cn": area.attrs.get('kvartal_cn'),
                "area_value": area.attrs.get('area_value'),
                "center": area.attrs.get('center')
            }

            elapsed_time = time.time() - start_time
            main_dict[cad_num] = data
            print(f"Обработка кадастрового номера {cad_num} заняла {elapsed_time} секунд.")

        except Exception as e:
            print(f"Ошибка при обработке кадастрового номера {cad_num}: {e}")

# Запись словаря в JSON файл
with open('output_without.json', 'w') as json_file:
    json.dump(main_dict, json_file, indent=4, ensure_ascii=False)
