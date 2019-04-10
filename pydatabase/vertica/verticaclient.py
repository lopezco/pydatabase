import vertica_python as vpy
import pandas as pd
import getpass
import logging
from copy import deepcopy

logger = logging.getLogger(__name__)

def _use_connection(func):
    def wrapper(*args, **kwargs):
        args[0]._open_connection()
        result = func(*args, **kwargs)
        args[0]._close_connection()
        return result
    return wrapper


class VerticaClient(object):
    """
    Example to create a new class:
    ```python
    from pydatabase.vertica import VerticaClient
    import logging

    logger = logging.getLogger(__name__)


    class MyVerticaClient(VerticaClient):

        _conn_params = {
            'host': 'myhost1.com',
            'port': 5433, 'database': 'mydatabase', 'connection_load_balance': True,
            'backup_server_node': ['myhost2:5433', 'myhost3:5433']}


    if __name__ == '__main__':
        vc = MyVerticaClient(store_credentials=True)
        vc.fetch("SELECT * FROM MYSCHEMA.MYTABLE LIMIT 10")
    ```
    """

    _username = None
    _password = None
    _conn = None
    _conn_params = None

    def __init__(self, password=None, username=None, store_credentials=False, conn_params=None):
        store_credentials = (password is not None or username is not None) or store_credentials
        if store_credentials:
            if username is None:
                print("Username: ")
                self._username = getpass._raw_input()
            else:
                self._username = username

            if password is None:
                self._password = getpass.getpass()
            else:
                self._password = password

        self._conn_params = conn_params or self._conn_params

    def _get_conn_parameters(self):
        tmp = deepcopy(self._conn_params)
        tmp.update({'user': self.username, 'password': self.password})
        return tmp

    def _open_connection(self):
        self._conn = vpy.connect(**self._get_conn_parameters())

    def _close_connection(self):
        self._conn.close()
        self._conn = None

    @property
    def password(self):
        if self._password is None:
            return getpass.getpass()
        else:
            return self._password

    @property
    def username(self):
        if self._username is None:
            print("Username: ")
            return getpass._raw_input()
        else:
            return self._username

    @_use_connection
    def fetch(self, query):
        """
        Executes the 'query' and returns the result as a dataframe

        Params:
        =======
        query: str
            Query to execute

        Return:
        =======
        out: pandas.DataFrame
            The result table
        """
        return pd.read_sql(query, self._conn).replace({None: pd.np.nan})

    def get_tables(self, schemas=None):
        """
        Params:
        =======
        schemas: list
            Vertica schema where the tables are defined

        Return:
        =======
        out: pd.DataFrame
            Table with tables information
        """
        base_query = """
        SELECT table_schema, table_name
        FROM v_catalog.tables
        """
        if schemas is not None:
            if isinstance(schemas, str):
                schemas = [schemas]

            base_query += "WHERE LOWER(table_schema) LIKE '{}'"

            query = """
            UNION ALL
            """.join([base_query.format(s.lower()) for s in schemas])
        else:
            query = base_query

        return self.fetch(query)

    def table_exist(self, tables, schema):
        """
        Params:
        =======
        schemas: list
            Vertica schema where the tables are defined

        Return:
        =======
        out: pd.DataFrame
            Table with tables information
        """
        if isinstance(tables, str):
            tables = [tables]

        queries = []
        for t in tables:
            queries.append("""
            SELECT table_name
            FROM v_catalog.tables
            WHERE LOWER(table_schema) LIKE '{}'  AND LOWER(table_name) LIKE '{}'
            """.format(schema.lower(), t.lower()))

        query = """
            UNION ALL
            """.join(queries)
        return len(self.fetch(query)) > 0

    def get_table_columns(self, tables, schema=None):
        """
        Params:
        =======
        tables: list[str]
            Table names
        schema: str
            Vertica schema where the tables are defined

        Return:
        =======
        out: pd.DataFrame
            Table with columns information
        """
        base_query = """
        SELECT table_schema, table_name, column_name, data_type
        FROM v_catalog.columns
        WHERE LOWER(table_name) LIKE '{}'
        """
        if schema is not None:
            base_query += "AND LOWER(table_schema) LIKE '{}'".format(schema.lower())

        query = """
        UNION ALL
        """.join([base_query.format(t.lower()) for t in tables])

        return self.fetch(query)

    def drop_tables(self, tables):
        for t in tables:
            logger.info('Dropping table: {}'.format(t))
            cur = self._conn.cursor()
            try:
                cur.execute('DROP TABLE {}'.format(t))
            except Exception as e:
                logger.error('Could not drop table {}: {}'.format(t, e))
