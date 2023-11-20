import os
import time
import csv
from rosreestr2coord import Area
import threading
from split_txtpy import split_csv_into_microfiles
from config import db_username, db_host, db_name, db_password
from sqlalchemy import create_engine, Column, String, Float, JSON
from sqlalchemy.orm import sessionmaker, declarative_base


def process_microfile(csv_file_path):
    try:
        engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}/{db_name}')
        connection = engine.connect()
        Base = declarative_base()
        Session = sessionmaker(bind=engine)
        session = Session()

        class Property(Base):
            __tablename__ = 'properties'
            id = Column(String, primary_key=True)
            address = Column(String)
            kvartal_cn = Column(String)
            area_value = Column(Float)
            center = Column(JSON)
            # ...

        Base.metadata.create_all(engine)

        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                parcel_type = area.attrs.get('parcel_type')
                data = {
                    "address": area.attrs.get('address'),
                    "kvartal_cn": area.attrs.get('kvartal_cn'),
                    "area_value": area.attrs.get('area_value'),
                    "center": area.attrs.get('center')
                }
                property_obj = Property(id=cad_data, **data)
                session.add(property_obj)

        session.commit()
        print(f"Обработан файл: {csv_file_path}")
        time.sleep(1)  # Пауза на 1 секунду

    except Exception as e:
        print(f"Ошибка при обработке файла {csv_file_path}: {e}")


def process_microfiles():
    split_csv_into_microfiles('original.csv', 'microfiles')  # Разделение на микрофайлы по 300 участков
    folder_name = 'microfiles'
    files = os.listdir(folder_name)

    for file in files:
        file_path = f'{folder_name}/{file}'
        threads = []

        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                thread = threading.Thread(target=process_microfile, args=(file_path,))
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()

        # Загрузка изменений в БД после обработки микрофайла
        # ...

        time.sleep(1)  # Пауза на 1 секунду


if __name__ == "__main__":
    process_microfiles()
