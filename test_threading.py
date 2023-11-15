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


def get_data(cad_data):
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

    except Exception as e:
        print(f"Ошибка при обработке кадастрового номера {cad_data}: {e}")


# Открытие CSV файла для чтения
csv_file_path = 'test_1000.csv'

# Чтение данных из CSV файла
with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    # Запуск потока для каждой строки в CSV файле
    for row in reader:
        threading.Thread(target=get_data, args=(row[0],)).start()

# Ожидание завершения всех потоков перед завершением программы
for thread in threading.enumerate():
    if thread != threading.current_thread():
        thread.join()

try:
    # Фиксация изменений
    session.commit()
    print("Данные успешно сохранены в БД.")
except Exception as commit_error:
    print(f"Ошибка при фиксации изменений: {commit_error}")

print(time.time() - start_time)
