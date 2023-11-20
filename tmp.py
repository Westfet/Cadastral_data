import threading
import time
import os
from config import db_username, db_host, db_name, db_password
from sqlalchemy import create_engine, Column, String, Float, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
from rosreestr2coord import Area

# Подключение к базе данных PostgreSQL
try:
    engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}/{db_name}')
    connection = engine.connect()
    print("Подключение к БД прошло успешно.")
except Exception as ex:
    print(f"Ошибка подключения к БД: {ex}")

# Создание сессии SQLAlchemy
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


# Определение класса Property
class Property(Base):
    __tablename__ = 'properties'

    id = Column(String, primary_key=True)
    address = Column(String)
    kvartal_cn = Column(String)
    area_value = Column(Float)
    center = Column(JSON)


# Создание таблицы
Base.metadata.create_all(engine)

start_time = time.time()

empty_cad_list = []


# asyncio
# создаем асинхронные функции
#
def get_data(cad_data):
    try:
        if cad_data:
            area = Area(cad_data, with_proxy=True, use_cache=False)

            # Извлекаем нужные атрибуты
            data = {
                "address": area.attrs.get('address') or None,
                "kvartal_cn": area.attrs.get('kvartal_cn') or None,
                "area_value": area.attrs.get('area_value') or None,
                "center": area.attrs.get('center') or None
            }

            # Сохраняем данные в БД
            property_obj = Property(id=cad_data[0], **data)
            session.add(property_obj)
        else:
            print("Пустая строка!")
    except Exception as e:
        print(f"Ошибка при обработке кадастрового номера {cad_data}: {e}")


def process_txt_file(txt_file):
    with open(txt_file, 'r', encoding='utf-8') as txtfile:
        lines = txtfile.readlines()

        # Чистка списка потоков ???
        threads = []
        for line in lines:
            cadastre_numbers = line.strip().split(',')  # Разделение строк по запятой
            for cadastre_number in cadastre_numbers:
                thread = threading.Thread(target=get_data, args=(cadastre_number,))
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()
        print("Файл обработан. Спим")
        time.sleep(1)





# split_txt_to_multiple_files('list_example.txt', 'txt_folder')

count = 0
# Проходим по всем файлам TXT в папке
txt_files_folder = 'txt_folder'
for filename in os.listdir(txt_files_folder):
    count += 1
    if filename == f'{count}.txt':
        file_path = os.path.join(txt_files_folder, filename)
        process_txt_file(file_path)

# Код фиксации изменений в БД
try:
    # Фиксация изменений
    session.commit()
    print("Данные успешно сохранены в БД.")
except Exception as commit_error:
    print(f"Ошибка при фиксации изменений: {commit_error}")

print(time.time() - start_time)
