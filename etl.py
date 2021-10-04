import os
import json
import logging
from urllib3.exceptions import HTTPError
from time import sleep
from typing import Generator

import backoff
import psycopg2
from elasticsearch.exceptions import ElasticsearchException
from psycopg2.extensions import connection as _connection
from psycopg2.extras import RealDictCursor
from redis import Redis
from elasticsearch import Elasticsearch

from utils import EnhancedJSONEncoder, coroutine
from models import Movie, Genre, Person
from state import RedisStorage

logger = logging.getLogger()


class ETL:
    def __init__(self, conn: _connection, storage: RedisStorage):
        self.conn = conn
        self.storage = storage
        self.es = Elasticsearch(hosts="es")
        # Размер пакетного запроса
        self.batch_size = 10

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(ElasticsearchException, HTTPError),
        max_tries=10,
    )
    @backoff.on_exception(backoff.expo, BaseException)
    def start(self, target):
        logger.info("Started")
        while True:
            target.send(1)
            sleep(0.1)

    @coroutine
    def extract(self, target: Generator) -> Generator:
        logger.info('Extract')
        while _ := (yield):
            queries = [
                {
                    'index': 'movies',
                    'query': movies_query()
                },
                {
                    'index': 'genres',
                    'query': genres_query()
                },
                {
                    'index': 'persons',
                    'query': persons_query()
                }
            ]
            state = self.storage.retrieve_state()

            for query in queries:
                q = query['query']

                with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(q, (state,))

                    while True:
                        all_rows = [dict(row) for row in cursor.fetchmany(self.batch_size)]

                        if not all_rows:
                            break

                        target.send(
                            {
                                'index': query['index'],
                                'data': all_rows
                            }
                        )

    @coroutine
    def transform(self, target: Generator) -> Generator:
        logger.info('Transformed data')
        while result := (yield):
            index = result['index']
            rows = result['data']
            transformed = []
            for row in rows:
                if index == 'movies':
                    movie = Movie.from_dict({**row})
                    transformed.append(movie)
                if index == 'genres':
                    genre = Genre.from_dict({**row})
                    transformed.append(genre)
                if index == 'persons':
                    person = Person.from_dict({**row})
                    transformed.append(person)

            target.send(
                {
                    'index': index,
                    'data': transformed
                }
            )

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(ElasticsearchException, HTTPError),
        max_tries=10,
    )
    @coroutine
    def load(self) -> Generator:
        logger.info('Load data')

        while transformed := (yield):
            index = transformed['index']
            data = transformed['data']

            if len(transformed) == 0:
                continue

            logging.info(f"Updating {index} in ES")

            movies_to_es = []
            for row in data:
                movies_to_es.extend(
                    [
                        json.dumps(
                            {
                                "index": {
                                    "_index": index,
                                    "_id": row.id
                                }
                            }
                        ),
                        json.dumps(row, cls=EnhancedJSONEncoder),
                    ]
                )

            prepare_data = "\n".join(movies_to_es) + "\n"
            logger.info(f"records {prepare_data}")
            self.es.bulk(body=prepare_data, index=index)
            storage.save_state(data[0].updated_at)

    def __call__(self, *args, **kwargs):
        load = self.load()
        transform = self.transform(load)
        extract = self.extract(transform)
        self.start(extract)


def movies_query() -> str:
    query = f"""
    WITH movies AS (
            SELECT
            id, 
            title, 
            description, 
            rating, 
            type,
            imdb_rating,
            updated_at 
        FROM film_work
        LIMIT 100
    )
    SELECT
        fw.id, 
        fw.title, 
        fw.description, 
        fw.rating, 
        fw.type,
        fw.imdb_rating,
        fw.updated_at,
        CASE
            WHEN pfw.role = 'ACTOR' 
            THEN ARRAY_AGG(jsonb_build_object('id', p.id, 'name', p.name, 'p_updated_at', p.updated_at))
        END AS actors,
        CASE
            WHEN pfw.role = 'WRITER' 
            THEN ARRAY_AGG(jsonb_build_object('id', p.id, 'name', p.name, 'p_updated_at', p.updated_at))
        END AS writers,
        CASE
            WHEN pfw.role = 'DIRECTOR' 
            THEN ARRAY_AGG(jsonb_build_object('id', p.id, 'name', p.name, 'p_updated_at', p.updated_at))
        END AS directors,
        
        ARRAY_AGG(g.name) AS genres
        FROM movies as fw
        LEFT JOIN film_work_person pfw ON pfw.film_work_id = fw.id
        LEFT JOIN person p ON p.id = pfw.person_id
        LEFT JOIN film_work_genre gfw ON gfw.film_work_id = fw.id
        LEFT JOIN genre g ON g.id = gfw.genre_id
        GROUP BY
            fw.id, 
            fw.title, 
            fw.description, 
            fw.rating, 
            fw.type,
            fw.imdb_rating,
            fw.updated_at,
            pfw.role
        ORDER BY fw.updated_at;
    """

    return query


def genres_query() -> str:
    query = """
    SELECT
        g.name,
        g.id as id,
        g.updated_at,
        fw.id as fw_id
    FROM genre g
    LEFT JOIN film_work_genre gfw ON gfw.genre_id = g.id
    LEFT JOIN film_work fw ON fw.id = gfw.film_work_id
    ORDER BY g.updated_at DESC
    LIMIT 100;
    """

    return query


def persons_query() -> str:
    query = f"""
    SELECT
        p.name,
        p.id as id,
        p.updated_at as p_updated_at,
        fw.id as fw_id,
        pfw.role as person_role
    FROM person p
    LEFT JOIN film_work_person pfw ON pfw.person_id = p.id
    LEFT JOIN film_work fw ON fw.id = pfw.film_work_id
    ORDER BY p.updated_at DESC
    LIMIT 100;
    """

    return query


if __name__ == "__main__":
    dsl = {
        "dbname": os.getenv("POSTGRES_DB_NAME", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "@J2JqrPRYoFnVv2jvV"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "options": "-c search_path=public,postgres",
    }

    try:
        with psycopg2.connect(**dsl) as pg_conn:
            redis_adapter = Redis(host="redis")
            storage = RedisStorage(redis_adapter=redis_adapter)
            etl = ETL(conn=pg_conn, storage=storage)
            etl()
    except psycopg2.DatabaseError as error:
        logger.info(f"Database error {error}")
    finally:
        pg_conn.close()
