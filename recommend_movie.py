import information_extraction as ie
import access_data as ad


def recommend_movie_to_customer(customer_id):
    # top3_genres_customers = ie.get_top_genre(ie.select_top_genre(), customer_id)
    top3_genres_customers = ie.select_top_genre()
    all_similarity_dfs = ie.get_movie_similarity_dfs(ad.db_connection)
    total_cosine_sim = ie.find_movies_similarity(all_similarity_dfs)
    ie.create_df_with_cos(total_cosine_sim)

    # print(ie.find_recent_purchase(customer_id))
    movie_similarity_df = ie.create_similarity_df()
    film_id_recent_fav = ie.get_recent_watched_fav(top3_genres_customers, customer_id)
    movie_list = ie.recommend_movie(movie_similarity_df, film_id_recent_fav)

    ie.get_movie_details(ad.db_connection, movie_list)


def get_customer_input():
    while True:
        try:
            customer_id = int(input("Enter customer_id(int)"))
        except ValueError:
            print("Sorry, please enter an integer id")
        else:
            break

    return customer_id


if __name__ == '__main__':
    # print(recommend_movie(70))
    cid = get_customer_input()
    print(f"Following is the list of movies recommended for customer {cid}: \n")
    recommend_movie_to_customer(cid)
