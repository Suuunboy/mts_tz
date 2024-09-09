import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, text
import logging
from tabulate import tabulate


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


#Функция для извлечения данных из xlsx файла
def extract_data(file_path):
    logging.info(f'Извлечение данных из {file_path}')
    try:
        data = pd.read_excel(file_path)
        data.columns = data.columns.str.strip()
        logging.info('Данные успешно извлечены')
        return data
    except Exception as e:
        logging.error(e)
        raise


#Функция для преобразования данных  и удаления некорректных  строк
def transform_data(data: pd.DataFrame):
    logging.info('Преобразование данных')
    try:
        data['transaction_date'] = pd.to_datetime(data['transaction_date'], errors='coerce')

        data['year'] = data['transaction_date'].dt.year
        data['month'] = data['transaction_date'].dt.month

        data['is_refund'] = data['transaction_type'].apply(lambda x: True if x == 'refund' else False)

        data = data.dropna(subset=['transaction_id', 'user_id', 'transaction_amount', 'transaction_date', 'transaction_type'])

        data = data[data['transaction_amount'] > 0]

        data['user_id'] = pd.to_numeric(data['user_id'], errors='coerce')
        data = data.dropna(subset=['user_id'])

        data['user_id'] = data['user_id'].astype(int)

        data = data.drop_duplicates(subset='transaction_id')

        logging.info('Данные успешно преобразованы')
        return data

    except Exception as e:
        logging.error(e)
        raise


#Функция для создания таблицы
def create_table(engine):
    meta = MetaData()

    transactions = Table (
        'transactions', meta,
        Column('transaction_id', Integer, primary_key=True),
        Column('user_id', Integer, index=True),
        Column('transaction_amount', Float),
        Column('transaction_date', DateTime),
        Column('transaction_type', String),
        Column('year', Integer),
        Column('month', Integer),
        Column('is_refund', Boolean)
    )

    meta.create_all(engine)


#Функция для загрузки преобразованных данных в таблицу базы данных
def load_data(data: pd.DataFrame, db_path):
    engine = create_engine(f'sqlite:///{db_path}')

    create_table(engine)

    data.to_sql('transactions', con=engine, if_exists='append', index=False)
    logging.info(f'Данные загружены в {db_path}')


#Вспомогательная функция для выполнения запроса
def query(db_path, query_str):
    engine = create_engine(f'sqlite:///{db_path}')

    with engine.connect() as connection:
        result = connection.execute(text(query_str))
        rows = result.fetchall()
        headers = result.keys()
        print(tabulate(rows, headers, tablefmt="pretty"))


file_path = 'transactions.xlsx'
db_path = 'transactions.db'

# data = extract_data(file_path)
# transformed_data = transform_data(data)
# load_data(transformed_data, db_path)

query_str = """
    SELECT year, month, COUNT(DISTINCT user_id) AS unique_users
    FROM transactions
    WHERE transaction_type = 'purchase'
    GROUP BY year, month
    ORDER BY year, month;
    """

query(db_path, query_str)



