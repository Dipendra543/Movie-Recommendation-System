import access_data as ad
import os
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


def select_top_genre(df):
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


def get_top_genre(genre_dict, customer_id):
    return genre_dict[customer_id]


def get_actor_list(connection):
    query = """select CONCAT(first_name, " " ,last_name) as Full_Name from actor"""
    actor_df = ad.get_data_from_query(connection, query)
    # print(type(actor_df['Full_Name']))
    actor_list = list(actor_df['Full_Name'])
    return actor_list


def actor_present(actor, col):
    if actor in col.split(","):
        return 1
    else:
        return 0


def actors_df_categorical(connection, original_df):
    """
    :param connection: The connection parameter
    :return:
    """
    actor_list = get_actor_list(connection)
    for each_actor in actor_list:
        original_df[each_actor] = original_df.apply(lambda x: actor_present(each_actor, x['actors']), axis=1)
    # print(original_df.head(2))
    original_df.drop(['actors'], axis=1, inplace=True)
    # print(original_df.head(2))
    # original_df.to_csv("test.csv", index=False)
    return original_df


def get_movie_similarity_dfs(connection):
    query = """select * from film_list;"""
    whole_df = ad.get_data_from_query(connection, query)

    movie_genre = whole_df[['FID', 'category']]
    movie_genre = pd.get_dummies(movie_genre, prefix=['category'])

    # For actors we need to separate the values and encode ourselves
    movie_actors = whole_df[['FID', 'actors']]
    encoded_actor_df = actors_df_categorical(connection, movie_actors)

    movie_price = whole_df[['FID', 'price']]
    movie_length = whole_df[['FID', 'length']]

    fid_list = list(whole_df['FID']) # The fid_list is used by the function "create_df_with_cos"
    return movie_genre, encoded_actor_df, movie_price, movie_length, fid_list


def find_movies_similarity(all_dataframes_tuples):
    cos_genre_similarity = cosine_similarity(all_dataframes_tuples[0].values)*0.55
    cos_actor_similarity = cosine_similarity(all_dataframes_tuples[1].values)*0.25
    cos_price_similarity = cosine_similarity(all_dataframes_tuples[2].values)*0.1
    cos_length_similarity = cosine_similarity(all_dataframes_tuples[3].values)*0.1
    total_cos_similarity = cos_genre_similarity+cos_actor_similarity+cos_price_similarity+cos_length_similarity

    return total_cos_similarity, all_dataframes_tuples[4]


def create_df_with_cos(cos_similarity_matrix):
    fid_list = cos_similarity_matrix[1]
    global movies_sim_df
    movies_sim_df = pd.DataFrame(cos_similarity_matrix[0], columns=fid_list, index=fid_list)
    # print(movies_sim_df.head(2))
    return movies_sim_df


def get_similar_movies(film_id):
    # print(film_id)
    df = movies_sim_df.loc[movies_sim_df.index == film_id].reset_index().\
        melt(id_vars='index', var_name='sim_moveId', value_name='relevance').\
        sort_values('relevance', axis=0, ascending=False)[1:6]
    # print(df)
    return df


def create_similarity_df():
    global movies_similarity
    movies_similarity = pd.DataFrame(columns=['index', 'sim_moveId', 'relevance'])
    for x in movies_sim_df.index.tolist():
        movies_similarity = movies_similarity.append(get_similar_movies(x))

    movies_similarity.set_index('index', inplace=True)
    print(movies_similarity.head(10))


def recommend_movie(film_id):
    print("Printing the similar movies::")
    print(movies_similarity.loc[film_id])


if __name__ == "__main__":
    db_connection = ad.connect_database('localhost', 'root', os.getenv("MYSQL_LOCALHOST_PASSWORD"), 'sakila')
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
    read_data = ad.get_data_from_query(db_connection, query, pd_df=True)
    ad.set_multi_index(read_data, ['customer_id', 'FULL_NAME'], inplace=True)
    top3_genres_customers = select_top_genre(read_data)
    # print(get_top_genre(top3_genres_customers,2))


    all_similarity_dfs = get_movie_similarity_dfs(db_connection)
    total_cosine_sim = find_movies_similarity(all_similarity_dfs)
    create_df_with_cos(total_cosine_sim)
    create_similarity_df()
    recommend_movie(8)

