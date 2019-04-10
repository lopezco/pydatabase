from tqdm import tqdm
import pandas as pd
import numpy as np
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from .verticaclient import VerticaClient


class VerticaTable(object):
    """
    Example to create a new class to automatically handle connections:

    ```python
    from pydatabase.vertica.vertica_table import VerticaTable, VerticaClient
    import logging

    logger = logging.getLogger(__name__)


    class MyVerticaClient(VerticaClient):

        _conn_params = {
            'host': 'myhost1.com',
            'port': 5433, 'database': 'mydatabase', 'connection_load_balance': True,
            'backup_server_node': ['myhost2:5433', 'myhost3:5433']}


    class MyVerticaTable(VerticaTable):
        _connection_client_class = MyVerticaClient


    if __name__ == '__main__':
        vtbl = MyVerticaTable('MYSCHEMA.MYTABLE', **{'username': 'myusername'})

    ```
    """

    _connection_client_class = VerticaClient

    def set_verbose(self, verbose):
        self.verbose = verbose

    def __init__(self, name, connection_client=None, **kwargs):
        """
        Vertica Table

        Params:
        =======
        connection_client: VerticaClient
            Vertica connection client
        name: str
            Table name on Vertica including schema
        **kwargs:
            Arguments to configure a client. See `VerticaClient`

        Return:
        =======
        out: VerticaTable
            Vertica Table wrapper

        """
        self._client = connection_client or self._connection_client_class(store_credentials=True, **kwargs)
        _name = name.split('.')
        self.name = _name[1]
        self.schema = _name[0]
        self.verbose = False

        # Verify that exists
        assert self._client.table_exist(self.name, self.schema), "Table {} does't exists".format(name)

    @staticmethod
    def _get_batch(iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            tmp = iterable[ndx:min(ndx + n, l)]
            yield tmp

    def _schema_plus_table_name(self):
        return '"{}"."{}"'.format(self.schema, self.name)

    def describe(self, function_list=None, limit=None, batch_size=10):
        """
        Compute the description of a vertica table

        Params:
        =======
        funtion_list: list
            List of tuples [(<str:over>, <str:function>, <list[str]:type_list>) where <over> can be either 'ALL' or 'DISTINCT',
            <function> can be one of Vertica's aggregation functions, and <type_list> a list of types (See previous parameter)
        limit: int
            Limit the number of records to use. None for no limit
        batch_size: int
            Number of columns included in each call

        Return:
        =======
        df: pd.DataFrame
            Result with the same columns as keys inside <variable_types> parameter and the '{over}_{function}' as index
        """

        if function_list is None:
            function_list = [
                ('ALL', 'MIN', ['Num', 'Date']),
                ('ALL', 'MAX', ['Num', 'Date']),
                ('ALL', 'AVG', ['Num']),
                ('ALL', 'STDDEV', ['Num']),
                ('ALL', 'COUNT', ['Num', 'Date', 'Char']),
                ('DISTINCT', 'COUNT', ['Num', 'Date', 'Char']),
                ('ALL', 'TOP3', ['Num', 'Date', 'Char'])]

        # get variable types
        #  variable_types: dict
        #       {<str:var_name>: <str:var_type>} with <var_type> in ('Char', 'Num', 'Date')
        col_df = self.columns_summary()
        col_df["data_type"] = (
            col_df["data_type"]
                .str.replace("varchar.*", "Char")
                .str.replace("timestamp.*", "Date")
                .str.replace("int.*", "Num")
                .str.replace("float.*", "Num")
        )
        variable_types = col_df.set_index('column_name')["data_type"].to_dict()

        table_name = self._schema_plus_table_name()

        data_buffer = []
        n_func = len(function_list)
        func_count = 0

        for over, func, valid_types in function_list:
            func_count += 1
            print('Computung ({}/{}): {} {} of {}'.format(func_count, n_func, over, func, valid_types))

            col_buffer = []
            for var_list in tqdm(self._get_batch(list(variable_types.keys()), batch_size)):

                if func.startswith('TOP'):
                    top_limit = int(func.replace('TOP', ''))
                    queries_buffer = []
                    for var in var_list:
                        if variable_types[var] not in valid_types:
                            continue

                        query = "SELECT ISNULL(TO_CHAR({col}), '<NULL>') FROM {tbl} GROUP BY {col} ORDER BY COUNT(*) DESC LIMIT {limit}".format(
                            tbl=table_name, col=var, limit=top_limit)

                        if self.verbose:
                            print(query)

                        # Fetch data
                        _var_top = self._client.fetch(query)

                        if _var_top is None:
                            continue

                        queries_buffer.append(pd.Series(_var_top.iloc[:, 0].astype(str).str.cat(sep='; '),
                                                        name='TOP{}_VALUES'.format(top_limit),
                                                        index=[var]).to_frame().transpose())

                    if not len(queries_buffer):
                        continue

                    _col = pd.concat(queries_buffer, axis=1)
                    col_buffer.append(_col)

                else:
                    select_list = ['{func}({over} "{var}") AS "{var}"'.format(func=func, var=var, over=over)
                                   for var in var_list
                                   if variable_types[var] in valid_types]

                    if not len(select_list):
                        continue

                    # Build query
                    select_str = ", ".join(select_list)

                    query = 'SELECT {select_str} FROM {from_str} {limit}'.format(
                        select_str=select_str,
                        from_str=table_name,
                        limit="LIMIT {}".format(int(limit)) if limit is not None else "")

                    # Fetch data
                    if self.verbose:
                        print(query)

                    _col = self._client.fetch(query)

                    if _col is None:
                        continue

                    col_buffer.append(_col)

            if not len(col_buffer):
                continue

            # Concatenate results
            _df = pd.concat(col_buffer, axis=1)

            # Add function tag as index
            _df.index = ['{}_{}'.format(over, func)]

            # Append to data buffer
            data_buffer.append(_df)

        if not len(data_buffer):
            return

        # Concat to build dataset
        df = pd.concat(data_buffer, axis=0)

        # Compute number of records
        n_records = float(len(self))

        # Reshape output
        df = df.transpose()
        df.index.name = "Variable"
        df = df.reset_index()

        # Compute other stats
        if any(df.columns.isin(["ALL_COUNT"])):
            df['MISSING_VALUES_RATE'] = (1 - df['ALL_COUNT'].values / n_records) * 100.0

        if any(df.columns.isin(["DISTINCT_COUNT"])):
            df['DISTINCT_VALUES_RATE'] = df['DISTINCT_COUNT'] / n_records * 100.0

        return df

    def columns_summary(self):
        """
        Return:
        =======
        out: pd.DataFrame
            Table with columns information
        """
        return self._client.get_table_columns([self.name], schema=self.schema)

    @property
    def columns(self):
        """
        Return:
        =======
        out: pandas.Index
            List of column names
        """
        return pd.Index(self.columns_summary()['column_name'].values)

    def __len__(self):
        """
        Return:
        =======
        out: int
            Number of rows
        """
        table_name = self._schema_plus_table_name()
        return self._client.fetch('SELECT COUNT(*) FROM {from_str}'.format(from_str=table_name)).values.ravel()[0]

    @property
    def size(self):
        """
        Return:
        =======
        out: tuple[int]
            Tuple (Number of rows, number of columns)
        """
        return len(self), len(self.columns)

    def fetch_batch(self, batch_size=10000, subset=None):
        """
        Fetch batch of data

        Params:
        =======
        batch_size: int
            Number of rows in batch
        subset: list[str]
            Columns subset to select

        Return:
        =======
        out: generator[pandas.DataFrame]
            Dataframe of size = (batch_size, len(subset))
        """
        nrecords = len(self)
        table_name = self._schema_plus_table_name()

        for i in range(1, nrecords + 1, batch_size):
            start_i = i
            end_i = i + batch_size - 1 if i + batch_size - 1 <= nrecords else nrecords

            # Build query
            select_str = "*" if subset is None else ", ".join(subset)

            query = """
            SELECT *
            FROM (
                SELECT ROW_NUMBER() OVER() AS row_number, {select_str}
                FROM {from_str}) t1
            WHERE row_number BETWEEN {start_i} AND {end_i};
            """.format(
                select_str=select_str,
                from_str=table_name,
                start_i=start_i,
                end_i=end_i)

            # Fetch data
            yield self._client.fetch(query)

    def __getitem__(self, subset):
        """
        Fetch column(s)

        Params:
        =======
        subset: list[str]
            Columns subset to select

        Return:
        =======
        out: pandas.DataFrame
            Dataframe with selected columns

        Exemple:
        =======
        ```python
        # Get column A
        vtable['A']

        # Get column A & B
        vtable[['A', 'B']]

        # Get all columns
        vtable[:]
        ```
        """
        if isinstance(subset, tuple):
            where_clause = 'WHERE {}'.format(subset[0])
            subset = subset[1]
        else:
            where_clause = ''

        if isinstance(subset, str):
            subset = [subset]

        table_name = self._schema_plus_table_name()
        select_str = "*" if subset is None or subset == slice(None, None, None) else ", ".join(subset)
        query = """SELECT {select_str} FROM {from_str} {where_str}""".format(select_str=select_str, from_str=table_name,
                                                                             where_str=where_clause)
        return self._client.fetch(query)

    @classmethod
    def from_dataiku_dataset(cls, dku_dataset):
        try:
            import dataiku
        except ImportError:
            raise Exception("Needs Dataiku library to create a Table from Dataiku dataset")
        else:
            vertica_table = dku_dataset.get_config()["params"]['table']
            for k, v in dataiku.get_custom_variables().items():
                vertica_table = vertica_table.replace("${{{}}}".format(k), v)

            vertica_schema = dku_dataset.get_config()["params"]['schema']
            dataiku_table = dku_dataset.short_name
            dataiku_project = dku_dataset.project_key

            # Init client
            client = cls._connection_client_class(dataiku_project, dataiku_table)
            return VerticaTable(vertica_table, vertica_schema, connection_client=client)

    def variable_availability_summary(self, date_variable, num_threads=12):
        yyyymm_query = 'EXTRACT(YEAR FROM "{date}") * 100 + EXTRACT(MONTH FROM "{date}")'.format(date=date_variable)
        tmp = self[['MIN({})'.format(yyyymm_query), 'MAX({})'.format(yyyymm_query)]]
        min_date = int(tmp.transpose()[0]['MIN'])
        max_date = int(tmp.transpose()[0]['MAX'])
        date_range = pd.date_range(pd.to_datetime(min_date, format='%Y%m'),
                                   pd.to_datetime(max_date, format='%Y%m'), freq='MS').strftime('%Y%m').tolist()

        def get_variable_availability(yyyymm, date_query):
            return (
                (~self['{date_query} = {val}'.format(date_query=date_query, val=yyyymm), :]
                 .isnull())
                    .agg(np.any)
                    .to_frame(yyyymm)
                    .transpose()
            )

        pool = ThreadPool(num_threads)
        results = pool.map(partial(get_variable_availability, date_query=yyyymm_query), date_range)

        if not len(results):
            return

        df = pd.concat(results, axis=0)

        # Build indexer for first and las appearance
        tmp = df.unstack().reset_index()
        tmp.columns = ['VARIABLE', 'DATE_YYYYMM', 'VALUE']
        tmp = tmp.loc[tmp['VALUE'] == 1, ['DATE_YYYYMM', 'VARIABLE']].groupby('VARIABLE').agg(
            ['min', 'max']).reset_index()
        tmp.columns = ['VARIABLE', 'min', 'max']
        min_multi_idx = [tuple(x.values()) for x in tmp[['VARIABLE', 'min']].to_dict('records')]
        max_multi_idx = [tuple(x.values()) for x in tmp[['VARIABLE', 'max']].to_dict('records')]

        # Build output
        tbl = df.unstack().to_frame('VALUE')
        tbl['APPEARENCE'] = pd.np.nan
        tbl['VALUE'] = tbl['VALUE'].astype(int)
        tbl.loc[min_multi_idx, 'APPEARENCE'] = 'FIRST'
        tbl.loc[max_multi_idx, 'APPEARENCE'] = 'LAST'
        cols = ['VARIABLE', 'DATE_YYYYMM']
        cols.extend(tbl.columns.tolist())
        tbl = tbl.reset_index()
        tbl.columns = cols

        # Compute completeness
        num_months = self['DATEDIFF(MONTH, MIN("DATE_APPLICATION"), MAX("DATE_APPLICATION")) + 1 AS "DIFF"']['DIFF'][0]
        grouped_count = tbl.groupby('VARIABLE')['VALUE'].sum()
        incomplete_variables = grouped_count[grouped_count != num_months].index

        tbl['INCOMPLETE'] = tbl['VARIABLE'].isin(incomplete_variables)

        return tbl
