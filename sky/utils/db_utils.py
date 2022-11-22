"""Utils for sky databases."""
import contextlib
import threading
import sqlite3
from typing import Callable


def add_column_to_table(
    cursor: 'sqlite3.Cursor',
    conn: 'sqlite3.Connection',
    table_name: str,
    column_name: str,
    column_type: str,
):
    """Add a column to a table."""
    for row in cursor.execute(f'PRAGMA table_info({table_name})'):
        if row[1] == column_name:
            break
    else:
        cursor.execute(f'ALTER TABLE {table_name} '
                       f'ADD COLUMN {column_name} {column_type}')
    conn.commit()


def rename_column(
    cursor: 'sqlite3.Cursor',
    conn: 'sqlite3.Connection',
    table_name: str,
    old_name: str,
    new_name: str,
):
    """Rename a column in a table."""
    # NOTE: This only works for sqlite3 >= 3.25.0. Be careful to use this.

    for row in cursor.execute(f'PRAGMA table_info({table_name})'):
        if row[1] == old_name:
            cursor.execute(f'ALTER TABLE {table_name} '
                           f'RENAME COLUMN {old_name} to {new_name}')
            break
    conn.commit()


class ThreadLocalSQLite(threading.local):
    """Thread-local wrapper of a sqlite3 database."""

    def __init__(self, db_path: str, create_table: Callable):
        super().__init__()
        self._db_path = db_path

        # closing() automatically calls close() on exit.
        with contextlib.closing(sqlite3.connect(db_path)) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                create_table(cursor, connection)

    def safe_cursor(self):
        """Returns a newly created, auto-commiting, auto-closing cursor.

        This should be used as a context manager:

            # Users need not call close() or commit().

            with safe_cursor() as cursor:
                rows = cursor.execute('SELECT * from table')
                # Use rows here, before exiting.
                for row in rows:
                    # Do something with 'row'.

            with safe_cursor() as cursor:
                cursor.execute('INSERT into table ...')
        """

        class Cursor:
            """A newly created, auto-commiting, auto-closing cursor."""

            def __init__(self, db_path: str):
                self._db_path = db_path

            def __enter__(self):
                self.conn = sqlite3.connect(self._db_path)
                self.cursor = self.conn.cursor()
                return self.cursor

            def __exit__(self, type, value, traceback): # pylint: disable=redefined-builtin
                self.cursor.close()
                self.conn.commit()
                self.conn.close()

        return Cursor(self._db_path)
