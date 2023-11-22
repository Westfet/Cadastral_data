import asyncio
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
empty_data = []  # Глобальный список для хранения пустых данных


async def get_data(cad_data):
    global empty_data
    try:
        if cad_data:
            area = Area(cad_data, with_proxy=True, use_cache=False)
            if not area:  # Проверка на пустоту экземпляра класса Area
                empty_data.append(cad_data)

                return

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


async def process_and_save_txt_file(txt_file):
    try:
        with open(txt_file, 'r', encoding='utf-8') as txtfile:
            lines = [line.strip() for line in txtfile.readlines()]

        coros = [get_data(cad_num) for cad_num in lines]
        results = await asyncio.gather(*coros)
        return results
    except Exception as e:
        print(f"Ошибка при обработке файла {txt_file}: {e}")
        return []


async def process_all_files():
    results = []
    txt_files_folder = 'txt_folder'
    for filename in os.listdir(txt_files_folder):
        file_path = os.path.join(txt_files_folder, filename)
        if filename.endswith('.txt'):
            file_results = await process_and_save_txt_file(file_path)
            results.extend(file_results)
            print('Очередной файл из папки обработан, спим')
            await asyncio.sleep(1)  # Ждем 1 секунду между обработкой файлов

    return results


async def main():
    await process_all_files()
    session.commit()
    process_time = start_time - time.time()
    print("Данные успешно сохранены в БД.")
    print(empty_data)
    print(f'Обработка файла заняла {process_time}')


# Запускаем основной цикл асинхронной программы
asyncio.run(main())