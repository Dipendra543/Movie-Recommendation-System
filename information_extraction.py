import access_data as ad
import os
from os import path
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import warnings
import pickle

warnings.filterwarnings('ignore')


def check_if_file_exists(folder, filename):
    if path.exists(f"{folder}/{filename}"):
        # print("inside if statement")
        return True
    elif path.exists(f"{folder}"):
        # print("inside elif statement")
        return False
    else:
        # print("inside else statement")
        os.mkdir(folder)
        return False


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

    if check_if_file_exists("pickled_files", "dictionary.pkl"):
        with open("pickled_files/dictionary.pkl", "rb") as f:
            top3_dict = pickle.load(f)
    else:
        read_data = ad.get_data_from_query(ad.db_connection, query, pd_df=True)
        ad.set_multi_index(read_data, ['customer_id', 'FULL_NAME'], inplace=True)
        top3_dict = {}
        # print(df.index.drop_duplicates(keep='first'))
        multi_index = read_data.index.drop_duplicates(keep='first')
        # print(read_data.head())
        for each_element in multi_index:
            temp = tuple(read_data.loc[each_element[0]]['Category'].head(3).to_list())
            top3_dict[each_element[0]] = [each_element[1], temp]

        with open("pickled_files/dictionary.pkl", "wb") as f:
            pickle.dump(top3_dict, f)

    return top3_dict


def get_top_genre(genre_dict, customer_id):
    return genre_dict[customer_id]


def get_actor_list(connection):
    if check_if_file_exists("pickled_files", "actor_list.pkl"):
        with open("pickled_files/actor_list.pkl", "rb") as f:
            actor_list = pickle.load(f)
    else:
        query = """select CONCAT(first_name, " " ,last_name) as Full_Name from actor"""
        actor_df = ad.get_data_from_query(connection, query)
        actor_list = list(actor_df['Full_Name'])

        with open("pickled_files/actor_list.pkl", "wb") as f:
            pickle.dump(actor_list, f)

    return actor_list


def actor_present(actor, col):
    if actor in col.split(","):
        return 1
    else:
        return 0


def actors_df_categorical(connection, original_df):
    """
    :param connection: The connection parameter
    :param original_df: the original dataframe to be manipulated
    :return: dataframe "original_df" after converting to categorical
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

    if check_if_file_exists("pickled_files", "recent_purchase.pkl"):
        with open("pickled_files/recent_purchase.pkl", "rb") as f:
            recent_purchase_df = pickle.load(f)

    else:

        query = f"""select rental.customer_id, film_list.FID, film_list.title, film_list.category, rental.rental_date
                from rental
                join inventory on rental.inventory_id = inventory.inventory_id
                join film_list on inventory.film_id = film_list.FID
                where rental.customer_id = {customer_id}
                order by rental.customer_id, rental.rental_date desc;"""

        recent_purchase_df = ad.get_data_from_query(ad.db_connection, query)

        with open("pickled_files/recent_purchase.pkl", "wb") as f:
            pickle.dump(recent_purchase_df, f)

    return recent_purchase_df


def get_movie_similarity_dfs(connection):
    query = """select * from film_list;"""
    whole_df = ad.get_data_from_query(connection, query)

    if check_if_file_exists("pickled_files", "movie_genre.pkl"):
        with open("pickled_files/movie_genre.pkl", "rb") as f:
            movie_genre = pickle.load(f)

    else:
        movie_genre = whole_df[['FID', 'category']]
        movie_genre = pd.get_dummies(movie_genre, prefix=['category'])
        with open("pickled_files/movie_genre.pkl", "wb") as f:
            pickle.dump(movie_genre, f)

    if check_if_file_exists("pickled_files", "encoded_actor_df.pkl"):
        with open("pickled_files/encoded_actor_df.pkl", "rb") as f:
            encoded_actor_df = pickle.load(f)

    else:
        movie_actors = whole_df[['FID', 'actors']]
        encoded_actor_df = actors_df_categorical(connection, movie_actors)
        with open("pickled_files/encoded_actor_df.pkl", "wb") as f:
            pickle.dump(encoded_actor_df, f)

    if check_if_file_exists("pickled_files", "movie_price.pkl"):
        with open("pickled_files/movie_price.pkl", "rb") as f:
            movie_price = pickle.load(f)

    else:
        movie_price = whole_df[['FID', 'price']]
        with open("pickled_files/movie_price.pkl", "wb") as f:
            pickle.dump(movie_price, f)

    if check_if_file_exists("pickled_files", "movie_length.pkl"):
        with open("pickled_files/movie_length.pkl", "rb") as f:
            movie_length = pickle.load(f)

    else:
        movie_length = whole_df[['FID', 'length']]
        with open("pickled_files/movie_length.pkl", "wb") as f:
            pickle.dump(movie_length, f)

    if check_if_file_exists("pickled_files", "fid_list.pkl"):
        with open("pickled_files/fid_list.pkl", "rb") as f:
            fid_list = pickle.load(f)

    else:
        fid_list = list(whole_df['FID'])  # The fid_list is used by the function "create_df_with_cos"
        with open("pickled_files/fid_list.pkl", "wb") as f:
            pickle.dump(fid_list, f)

    return movie_genre, encoded_actor_df, movie_price, movie_length, fid_list


def find_movies_similarity(all_dataframes_tuples):
    if check_if_file_exists("pickled_files", "total_cos_similarity.pkl"):
        with open("pickled_files/total_cos_similarity.pkl", "rb") as f:
            total_cos_similarity = pickle.load(f)

    else:
        cos_genre_similarity = cosine_similarity(all_dataframes_tuples[0].values) * 0.55
        cos_actor_similarity = cosine_similarity(all_dataframes_tuples[1].values) * 0.25
        cos_price_similarity = cosine_similarity(all_dataframes_tuples[2].values) * 0.1
        cos_length_similarity = cosine_similarity(all_dataframes_tuples[3].values) * 0.1
        total_cos_similarity = cos_genre_similarity + cos_actor_similarity + cos_price_similarity + cos_length_similarity

        with open("pickled_files/total_cos_similarity.pkl", "wb") as f:
            pickle.dump(total_cos_similarity, f)

    return total_cos_similarity, all_dataframes_tuples[4]


def create_df_with_cos(cos_similarity_matrix):
    fid_list = cos_similarity_matrix[1]
    global movies_sim_df
    movies_sim_df = pd.DataFrame(cos_similarity_matrix[0], columns=fid_list, index=fid_list)
    # print(movies_sim_df.head(2))
    return movies_sim_df


def get_similar_movies(film_id):
    # print(film_id)
    df = movies_sim_df.loc[movies_sim_df.index == film_id].reset_index(). \
             melt(id_vars='index', var_name='sim_moveId', value_name='relevance'). \
             sort_values('relevance', axis=0, ascending=False)[1:6]
    # print(df)
    return df


def create_similarity_df():
    # global movies_similarity
    if check_if_file_exists("pickled_files", "movies_similarity.pkl"):
        with open("pickled_files/movies_similarity.pkl", "rb") as f:
            movies_similarity = pickle.load(f)

    else:
        movies_similarity = pd.DataFrame(columns=['index', 'sim_moveId', 'relevance'])
        for x in movies_sim_df.index.tolist():
            movies_similarity = movies_similarity.append(get_similar_movies(x))

        movies_similarity.set_index('index', inplace=True)

        with open("pickled_files/movies_similarity.pkl", "wb") as f:
            pickle.dump(movies_similarity, f)

    return movies_similarity


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
    final_temp_string = "(" + temp_string + ")"
    # print("final_temp_string", final_temp_string)

    query = f"""select FID, title, category from film_list
                where FID in {final_temp_string};"""
    required_df = ad.get_data_from_query(connection, query)
    # print(required_df)
    return required_df


def get_recent_watched_fav(top3_genres_customers, customer_id):
    """

    :param top3_genres_customers: the dictionary with top3 genre of each customer
    :param customer_id: the id of the customer
    :return: list of film ids of the recent movie the customer has watched and which falls under the top3_genres_customers
    """
    recent_purchased_df = find_recent_purchase(customer_id)
    temp = get_top_genre(top3_genres_customers, customer_id)[1]
    print(f"\n The top genres for the customer {customer_id} are {temp}")
    for i in range(recent_purchased_df.shape[0]):
        if recent_purchased_df.iloc[i]['category'] in temp:
            print("\n The Recent movie purchased by customer which is also his/her favorite genre is:",
                  recent_purchased_df.iloc[i]['FID'], recent_purchased_df.iloc[i]['title'],
                  recent_purchased_df.iloc[i]['category'])
            return recent_purchased_df.iloc[i]['FID']
            break


if __name__ == "__main__":
    print(check_if_file_exists("output", "dictionary.pkl"))