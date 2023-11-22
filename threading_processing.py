import csv
import time
import threading
from config import db_username, db_host, db_name, db_password
from sqlalchemy import create_engine, Column, String, Float, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
from rosreestr2coord import Area

start_time = time.time()

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


def data_generator(csv_file_path):
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        batch = []
        for row in reader:
            batch.append(row[0])  # Добавляем участок в порцию
            if len(batch) == 200:  # Как только порция достигает 200 участков
                yield batch  # Возвращаем ее


def process_data(data):
    processed = 0
    for cad_data in data:
        try:
            area = Area(cad_data, with_proxy=True, use_cache=False)

            # Получаем тип объекта
            parcel_type = area.attrs.get('parcel_type')

            # Извлекаем нужные атрибуты
            data = {
                "address": area.attrs.get('address'),
                "kvartal_cn": area.attrs.get('kvartal_cn'),
                "area_value": area.attrs.get('area_value'),
                "center": area.attrs.get('center')
            }

            # Сохраняем данные в БД
            property_obj = Property(id=cad_data, **data)
            session.add(property_obj)
            processed += 1

        except Exception as e:
            print(f"Ошибка при обработке кадастрового номера {cad_data}: {e}")

    return processed


def process_batch(batch):
    processed = process_data(batch)  # Обработка данных
    session.commit()  # Фиксируем изменения в БД
    print(f"{processed} файлов обработано. Сейчас мы спим.")
    time.sleep(1)  # Засыпаем на 1 секунду перед обработкой следующей порции


csv_file_path = 'test_10000.csv'
generator = data_generator(csv_file_path)

threads = []
try:
    while True:
        try:
            data = next(generator)  # Получаем порцию данных

            thread = threading.Thread(target=process_batch, args=(data,))  # Создаем поток для обработки данных
            threads.append(thread)  # Добавляем поток в список
            thread.start()  # Запускаем поток

        except StopIteration:
            break  # Если данные кончились, выходим из цикла

    for thread in threads:
        thread.join()  # Ожидаем завершения всех потоков

except Exception as commit_error:
    print(f"Ошибка при фиксации изменений: {commit_error}")

print(time.time() - start_time)
