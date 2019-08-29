import access_data as ad
import os
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


def select_top_genre():
    """
    :return: a dictionary with key as customer_id and values as [FULL_NAME,(TOP 3 genres)]
    """
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
    read_data = ad.get_data_from_query(ad.db_connection, query, pd_df=True)
    ad.set_multi_index(read_data, ['customer_id', 'FULL_NAME'], inplace=True)
    top3_dict = {}
    # print(df.index.drop_duplicates(keep='first'))
    multi_index = read_data.index.drop_duplicates(keep='first')
    # print(read_data.head())
    for each_element in multi_index:
        # print(each_element)
        # print(df.loc[each_element[0]]['Category'].head(3).to_list())
        temp = tuple(read_data.loc[each_element[0]]['Category'].head(3).to_list())
        top3_dict[each_element[0]] = [each_element[1], temp]

    return top3_dict


def get_top_genre(genre_dict, customer_id):
    # print(type(genre_dict), customer_id)
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


def find_recent_purchase(customer_id):
    query = f"""select rental.customer_id, film_list.FID, film_list.title, film_list.category, rental.rental_date
            from rental
            join inventory on rental.inventory_id = inventory.inventory_id
            join film_list on inventory.film_id = film_list.FID
            where rental.customer_id = {customer_id}
            order by rental.customer_id, rental.rental_date desc;"""

    recent_purchase_df = ad.get_data_from_query(ad.db_connection, query)
    return recent_purchase_df


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
    # global movies_similarity
    movies_similarity = pd.DataFrame(columns=['index', 'sim_moveId', 'relevance'])
    for x in movies_sim_df.index.tolist():
        movies_similarity = movies_similarity.append(get_similar_movies(x))

    movies_similarity.set_index('index', inplace=True)
    return movies_similarity
    # print(movies_similarity.head(10))


def recommend_movie(movie_sim_df, film_id):
    # print(movie_sim_df.head())
    # print("Printing the similar movies::")
    movie_list = list(movie_sim_df.loc[film_id]['sim_moveId'])
    # print(movie_list)
    return movie_list


def get_movie_details(connection, movie_list):
    temp_string = ""
    for each_movie in movie_list:
        temp_string += str(each_movie) + ","

    temp_string = temp_string.rstrip(',')
    final_temp_string = "("+temp_string+")"
    # print("final_temp_string", final_temp_string)

    query = f"""select FID, title, category from film_list
                where FID in {final_temp_string};"""
    required_df = ad.get_data_from_query(connection, query)
    print(required_df)


def get_recent_watched_fav(top3_genres_customers, customer_id):
    """

    :param top3_genres_customers: the dictionary with top3 genre of each customer
    :param customer_id: the id of the customer
    :return: list of film ids of the recent movie the customer has watched and which falls under the top3_genres_customers
    """
    recent_purchased_df = find_recent_purchase(customer_id)
    # print('Comedy' in get_top_genre(top3_genres_customers, customer_id)[1])
    temp = get_top_genre(top3_genres_customers, customer_id)[1]
    for i in range(recent_purchased_df.shape[0]):
        # print("Inside get_recent_watched_fav: ", each_category)
        # if each_category in get_top_genre(top3_genres_customers,customer_id)[1]:
        #     print("First Match Found:", recent_purchased_df['FID'], each_category)
        #     break
        # print(recent_purchased_df.iloc[i]['category'])
        if recent_purchased_df.iloc[i]['category'] in temp:
            # print("Match found:", recent_purchased_df.iloc[i]['category'])
            return recent_purchased_df.iloc[i]['FID']
            break


if __name__ == "__main__":
    pass

