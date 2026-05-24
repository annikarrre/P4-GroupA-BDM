import psycopg
from src.config import POSTGRES_DSN


def get_conn():
    return psycopg.connect(POSTGRES_DSN)