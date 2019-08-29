import information_extraction as ie


def recommend_movie(film_id):
    print("Printing the similar movies::")
    print(ie.movies_similarity.loc[film_id])


if __name__ == '__main__':
    print(recommend_movie())