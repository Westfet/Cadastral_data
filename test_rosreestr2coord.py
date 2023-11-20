import csv
import time
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

# Открытие CSV файла для чтения
csv_file_path = 'test_100.csv'

# Обработка CSV файла
with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)

    for row in reader:

        cad_num = row[0]
        try:
            start_time = time.time()

            area = Area(cad_num, with_proxy=True, use_cache=False,)

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
            property_obj = Property(id=cad_num, **data)
            session.add(property_obj)

            elapsed_time = time.time() - start_time
            print(f"Обработка кадастрового номера {cad_num} заняла {elapsed_time} секунд.")

        except Exception as e:
            print(f"Ошибка при обработке кадастрового номера {cad_num}: {e}")
            continue

    try:
        start_commit_time = time.time()
        # Фиксация изменений
        session.commit()
        commit_elapsed_time = time.time() - start_commit_time
        print(f"Фиксация изменений заняла {commit_elapsed_time} секунд.")
        print("Данные успешно сохранены в БД.")
    except Exception as commit_error:
        print(f"Ошибка при фиксации изменений: {commit_error}")
