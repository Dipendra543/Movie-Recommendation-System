#  Access the sakila database data from mysql server
import mysql.connector
import os
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


def select_top_three_genre(df):
    """

    :param df: give the grouped dataframe
    :return: a dictionary with key as customer_id and values as [FULL_NAME,(TOP 3 genres)]
    """
    top3_dict = {}
    # print(df.index.drop_duplicates(keep='first'))
    multi_index = df.index.drop_duplicates(keep='first')
    for each_element in multi_index:
        # print(each_element)
        # print(df.loc[each_element[0]]['Category'].head(3).to_list())
        temp = tuple(df.loc[each_element[0]]['Category'].head(3).to_list())
        top3_dict[each_element[0]] = [each_element[1], temp]

    return top3_dict


if __name__ == '__main__':
    db_connection = connect_database('localhost', 'root', os.getenv("MYSQL_LOCALHOST_PASSWORD"), 'sakila')
    query = """select rental.customer_id, CONCAT(customer.first_name," ",customer.last_name) as FULL_NAME, 
        category.name as Category, count(*) as COUNT_RENTED_MOVIES
        from rental
        join inventory on rental.inventory_id = inventory.inventory_id
        join film_category on inventory.film_id = film_category.film_id
        join category on film_category.category_id = category.category_id
        join customer on customer.customer_id = rental.customer_id
        group by rental.customer_id, category.category_id
        order by FULL_NAME, COUNT_RENTED_MOVIES desc;
        
    """
    read_data = get_data_from_query(db_connection, query, pd_df=True)
    set_multi_index(read_data, ['customer_id', 'FULL_NAME'], inplace=True)
    # print(read_data.index.drop_duplicates(keep='first'))
    # read_data.to_csv("grouped_data.csv")
    # print(read_data.head(20))
    top3_genres_customers = select_top_three_genre(read_data)
    print(top3_genres_customers.keys())