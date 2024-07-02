"""
DB file
"""
from typing import List, Dict, Union, Optional
import sqlite3
from sqlite3 import IntegrityError
import pandas as pd
from .constants import (
    DB_NAME
)


def create_connection() -> Optional[sqlite3.Connection]:
    """
    Create a database connection to a SQLite database
    """
    try:
        connection = sqlite3.connect(DB_NAME)
        return connection

    except sqlite3.Error as e:
        print(e)


def fetch_all_tables(connection: sqlite3.Connection) -> List[str]:
    """
    Fetch all tables from the database
    """
    cursor = connection.cursor()
    cursor.execute(
        """
            SELECT name
            FROM sqlite_master
            WHERE type='table';
        """
    )
    tables = cursor.fetchall()
    return [table[0] for table in tables]


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    """
    Check if a table exists in the database
    """
    cursor = connection.cursor()
    cursor.execute(
        f"""
            SELECT name
            FROM sqlite_master
            WHERE type='table'
                AND name='{table_name}';
        """
    )
    return cursor.fetchone() is not None


def table_operations(
    connection: sqlite3.Connection,
    operation: str,
    table_name: str
) -> Union[None, List[str]]:
    """
    Create a table in the database

    Need to add:
        - Flexible table schema (still can't find out a good solution)
    """
    cursor = connection.cursor()

    if operation == "delete":
        cursor.execute(
            f"""
                DROP TABLE {table_name}
            """
        )
        connection.commit()

    elif operation == "create":
        ## TODO: Flexible table schema
        cursor.execute(
            f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    date TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    CONSTRAINT unique_transaction UNIQUE (name, date, source)
                );
            """
        )
        connection.commit()

    all_tables = fetch_all_tables(connection)
    return all_tables


def write_trans(connection, name: str, date: str, amount: int, source: str) -> None:
    """
    Write a transaction to the database
    """
    cursor = connection.cursor()

    check_query = """
    SELECT amonut
    FROM money
    WHERE name = ?
        AND date = ?
        AND source = ?
    """
    cursor.execute(check_query, (name, date, source))

    if cursor.fetchone() is not None:
        raise IntegrityError("Transaction already exists")

    query = """
    INSERT INTO money (name, date, amount, source)
    VALUES (?, ?, ?, ?)
    """
    cursor.execute(query, (name, date, amount, source))
    connection.commit()
    print("Transaction written to database")


def delete_trans(connection, name: str, date: str, source: str) -> None:
    """
    Delete single transaction from the database
    """
    cursor = connection.cursor()
    query = """
    DELETE FROM money
    WHERE name = ? AND date = ? AND source = ?
    """
    cursor.execute(query, (name, date, source))
    connection.commit()
    print("Transaction deleted from database")


def update_trans(connection, amount: int, name: str, date: str, source: str) -> None:
    """
    Update single transaction in the database
    """
    cursor = connection.cursor()
    query = """
    UPDATE money
    SET amount = ?
    WHERE name = ? AND date = ? AND source = ?
    """
    cursor.execute(query, (amount, name, date, source))
    connection.commit()
    print("Transaction updated in database")


def easy_query(connection):
    """
    Query all data from the database
    """
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM money")
    data = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    return data


def query_database(
    connection,
    query_type: str,
    fields: List[str],
    filters: Dict[str, str]
) -> pd.DataFrame:
    """
    Qeury from database

    Query types:
        sum, average
    """
    cursor = connection.cursor()

    if not query_type:
        raise ValueError("Missing required argument: query_type")

    query_parts = ["SELECT"]
    if query_type.lower() in ("sum", "average"):
        query_parts.append(f"{query_type.upper()}({fields[0] if fields else '*'})")

    else:
        query_parts.append(f"{','.join(fields or ['*'])}")

    query_parts.append("FROM money")

    if filters:
        where_clause = " AND ".join([f"{k} = ?" for k in filters])
        query_parts.append(f"WHERE {where_clause}")

    query = " ".join(query_parts)
    args = tuple(filters.values()) if filters else None
    cursor.execute(query, args)
    data = pd.DataFrame(cursor.fetchall(), columns=fields or [col[0] for col in cursor.description])

    return data
