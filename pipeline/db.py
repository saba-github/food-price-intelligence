from database.connection import get_connection as get_shared_connection


def get_connection():
    return get_shared_connection(application_name="pipeline")
