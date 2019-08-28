#  Access the sakila database data from mysql server
import mysql.connector
import pandas as pd

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 100)


def connect_database(host, user, passwd, database):
    my_db = mysql.connector.connect(
        host=host,
        user=user,
        passwd=passwd,
        database=database
    )
    return my_db


def get_data_from_query(connection, query, pd_df=True):
    """

    :param connection: the database connection
    :param query: required query to execute, should be 'SELECT' query
    :param pd_df: if true, returns pandas dataframe
    :return: if pd_df is false, returns a list of elements, ie, list of tuples each tuple representing a row
    """
    if pd_df:
        read_df = pd.read_sql(query, con=connection)
        return read_df
    else:
        db_cursor = connection.cursor()
        db_cursor.execute(query)
        result = db_cursor.fetchall()
        return result


def set_multi_index(df, col_list, inplace=True):
    if inplace:
        df.set_index(col_list, inplace=True)
    else:
        new_df = df.set_index(col_list)
        return new_df


if __name__ == '__main__':
    pass
