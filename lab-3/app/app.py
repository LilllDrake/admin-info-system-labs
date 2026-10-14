import os
import time

import psycopg
from flask import Flask, jsonify, render_template, request
from psycopg.rows import dict_row

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024


def connect(target, *, autocommit=False):
    # target выбирает адрес: WRITE для primary, READ для standby.
    prefix = {'write': 'WRITE', 'read': 'READ'}[target]
    return psycopg.connect(
        host=os.environ[f'{prefix}_DB_HOST'],
        port=int(os.environ.get(f'{prefix}_DB_PORT', '5432')),
        dbname=os.environ.get('DB_NAME', 'my_database'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ['DB_PASSWORD'],
        connect_timeout=3,
        options='-c statement_timeout=10000',
        application_name=f'orders_app_{target}',
        row_factory=dict_row,
        autocommit=autocommit,
    )


def init_db():
    # Таблица создаётся только на primary; на standby её перенесёт репликация.
    with connect('write') as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS public.lab_orders (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                description varchar(120) NOT NULL,
                created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')


def payload():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise ValueError('Отправь JSON-объект.')
    return data


def insert_order(conn, description):
    return conn.execute(
        '''INSERT INTO public.lab_orders (description) VALUES (%s)
           RETURNING id, description, created_at''',
        (description,),
    ).fetchone()


def serialize(row):
    return {**row, 'created_at': row['created_at'].isoformat()}


@app.get('/')
def index():
    return render_template('index.html')


@app.get('/api/orders')
def list_orders():
    # Список всегда читается через READ_DB_HOST, без подмены на primary.
    with connect('read') as conn:
        rows = conn.execute(
            'SELECT * FROM public.lab_orders ORDER BY id DESC LIMIT 100'
        ).fetchall()
        total = conn.execute('SELECT count(*) AS total FROM public.lab_orders').fetchone()['total']
    return jsonify(orders=[serialize(row) for row in rows], total=total,
                   source=os.environ['READ_DB_HOST'])


@app.post('/api/orders')
def create_order():
    description = payload().get('description', '')
    if not isinstance(description, str) or not 1 <= len(description.strip()) <= 120:
        raise ValueError('Название заказа: от 1 до 120 символов.')
    with connect('write') as conn:
        row = insert_order(conn, description.strip())
    # Выход из with фиксирует транзакцию до отправки успешного HTTP-ответа.
    return jsonify(order=serialize(row)), 201


@app.post('/api/load')
def generate_load():
    count = payload().get('count', 200)
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 100000:
        raise ValueError('Размер пачки: целое число от 1 до 100000.')
    created = 0
    last_id = None
    started = time.monotonic()
    try:
        # Каждый INSERT фиксируется отдельно: WAL передаётся по ходу нагрузки.
        with connect('write', autocommit=True) as conn:
            for number in range(count):
                row = insert_order(conn, f'Тестовый заказ {number + 1}')
                created += 1
                last_id = row['id']
    except psycopg.Error:
        app.logger.exception('Ошибка во время нагрузки')
        return jsonify(error='Пачка создана не полностью.', created=created, last_id=last_id), 503
    return jsonify(created=created, last_id=last_id,
                   seconds=round(time.monotonic() - started, 3))


@app.errorhandler(ValueError)
def invalid_data(error):
    return jsonify(error=str(error)), 400


@app.errorhandler(psycopg.Error)
def database_error(error):
    app.logger.exception('Ошибка подключения или запроса к БД')
    return jsonify(error='База недоступна или отклонила запрос. Подробности в логах app.'), 503


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=8000, debug=False, threaded=True)
