import recommend_movie as rm
import access_data as ad
import pandas as pd


def get_all_customer_ids():
    query = """select distinct customer_id from rental"""
    returned_data = ad.get_data_from_query(ad.db_connection, query)
    # print(list(returned_data['customer_id']))
    customer_id_list = list(returned_data['customer_id'])
    return customer_id_list


def save_all_recommendations(customer_list):

    final_huge_df = pd.DataFrame(columns=['FID', 'title', 'category', 'CID'])
    for each_customer in customer_list:
        # print("Inside the function", each_customer)
        get_df = rm.recommend_movie_to_customer(each_customer)
        get_df['CID'] = each_customer
        final_huge_df = final_huge_df.append(get_df)

    print(final_huge_df.shape)
    final_huge_df.to_csv("final_recommendations.csv", index=False)


if __name__ == '__main__':
    cust_list = get_all_customer_ids()
    save_all_recommendations(cust_list)
