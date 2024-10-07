from data_ingestion import DataLoader
from data_matching_conforming import PeopleDF, PromotionsDF, TransactionsDF, TransfersDF, UserTransactionsDF, UserTransfersDF, ItemSummaryDF, StoreSummaryDF
import data_analysis
from flask import Flask, jsonify, render_template
import pandas as pd

app = Flask(__name__)

def load_data(file_paths):
    """Call DataLoader class to load data from different file formats."""
    dataframes = {}

    for file_path in file_paths:
        file_name = file_path.split('/')[-1]
        data_loader = DataLoader(file_path)
        dataframes[file_name] = data_loader.load_data()

    df_people_json = dataframes['people.json']
    df_people_yml = dataframes['people.yml']
    df_promotions = dataframes['promotions.csv']
    df_transactions = dataframes['transactions.xml']
    df_transfers = dataframes['transfers.csv']

    return df_people_json, df_people_yml, df_promotions, df_transactions, df_transfers

def data_matching_conforming(df_people_json, df_people_yml, df_promotions, df_transactions, df_transfers):
    """Call the classes from data_matching_conforming.py to match and conform the data and output it into csv files."""
    # Main DataFrames
    c1 = PeopleDF(df_people_json, df_people_yml)
    c1.standardize_data()
    people_df = c1.merge_data()

    c2 = PromotionsDF(df_promotions, people_df)
    c2.standardize_data()
    promotions_df = c2.resulting_df()

    c3 = TransactionsDF(df_transactions, people_df)
    c3.standardize_data()
    transactions_df = c3.resulting_df()

    c4 = TransfersDF(df_transfers)
    c4.standardize_data()
    transfers_df = c4.resulting_df()

    # Derived DataFrames
    c5 = UserTransactionsDF(transactions_df, people_df)
    _ = c5.merge_data()

    c6 = UserTransfersDF(transfers_df, people_df)
    _ = c6.merge_data()

    c7 = ItemSummaryDF(transactions_df)
    _ = c7.aggregate_data()

    c8 = StoreSummaryDF(transactions_df)
    _ = c8.aggregate_data()

def load_conformed_data():
    """Load the conformed data from the newly created CSV files."""
    item_summary_df = pd.read_csv('new_data/item_summary_df.csv')
    people_df = pd.read_csv('new_data/people_df.csv')
    promotions_df = pd.read_csv('new_data/promotions_df.csv')
    store_summary_df = pd.read_csv('new_data/store_summary_df.csv')
    transactions_df = pd.read_csv('new_data/transactions_df.csv')
    transfers_df = pd.read_csv('new_data/transfers_df.csv')
    user_transactions_df = pd.read_csv('new_data/user_transactions_df.csv')
    user_transfers_df = pd.read_csv('new_data/user_transfers_df.csv')

    conformed_data = {
        'item_summary': item_summary_df,
        'people': people_df,
        'promotions': promotions_df,
        'store_summary': store_summary_df,
        'transactions': transactions_df,
        'transfers': transfers_df,
        'user_transactions': user_transactions_df,
        'user_transfers': user_transfers_df
    }

    return conformed_data

@app.route('/', methods=['GET'])
def home():
    return '<title>Venmito</title><h1>Welcome to Venmito API!</h1>', 200

@app.route('/get_user/<int:user_id>', methods=['GET'])
def get_user(user_id):
    user = data_analysis.get_user(people_df, user_id)
    if user is not None:
        return jsonify(user.iloc[0].to_dict()), 200
    else:
        return jsonify({"error": "User not found"}), 404

@app.route('/promotions_per_country', methods=['GET'])
def get_promotions_per_country():
    promotions_per_country = data_analysis.promotions_by_type_of_clients(promotions_df, people_df)
    if promotions_per_country is not None:
        res = promotions_per_country.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Promotions not found"}), 404

@app.route('/promotions_per_country_with_acceptance_rate', methods=['GET'])
def get_promotions_per_country_with_acceptance_rate():
    promotions_per_country = data_analysis.promotions_by_type_of_clients_with_acceptance_rate(promotions_df, people_df)
    if promotions_per_country is not None:
        res = promotions_per_country.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Promotions not found"}), 404

@app.route('/items_sold_per_country', methods=['GET'])
def get_items_sold_per_country():
    items_sold_per_country = data_analysis.items_sold_per_country(transactions_df, people_df)
    if items_sold_per_country is not None:
        res = items_sold_per_country.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Items not found"}), 404
    
@app.route('/promotion_responded/<string:response>', methods=['GET'])
def get_promotion_responded(response):
    promotion_responded = data_analysis.promotion_responded(promotions_df, response)
    if promotion_responded is not None and response == 'Yes' or response == 'No':
        return jsonify({"count": int(promotion_responded)}), 200
    else:
        return jsonify({"error": "Invalid Response"}), 404

@app.route('/client_response_rate', methods=['GET'])
def get_client_response_rate():
    promotion_response_rate = data_analysis.client_response_rate(promotions_df)
    if promotion_response_rate is not None:
        response_rate_dict = {
            "Yes": promotion_response_rate.tolist()[1],  # Assuming Yes is at index 1
            "No": promotion_response_rate.tolist()[0]    # Assuming No is at index 0
        }
        return jsonify({"response_rate": response_rate_dict}), 200
    else:
        return jsonify({"error": "Response rate not found"}), 404

@app.route('/promotion_response_yes_buyer', methods=['GET'])
def get_promotion_response_yes_buyer():
    promotion_response_yes_buyer = data_analysis.promotion_response_yes_buyer(promotions_df, transactions_df)
    if promotion_response_yes_buyer is not None:
        res = promotion_response_yes_buyer.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Promotion response not found"}), 404

@app.route('/top_promotions/<int:n>', methods=['GET'])
def get_top_promotions(n):
    top_promotions = data_analysis.analyze_client_promotions(promotions_df, people_df, n)
    if top_promotions is not None:
        res = top_promotions.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Top promotions not found"}), 404

@app.route('/possible_conversion_strategy', methods=['GET'])
def get_possible_conversion_strategy():
    possible_conversion_strategy = data_analysis.possible_conversion_strategy(user_transactions_df, promotions_df, people_df)
    if possible_conversion_strategy is not None:
        res = possible_conversion_strategy.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Conversion strategy not found"}), 404

@app.route('/top_selling_items/<int:n>', methods=['GET'])
def get_top_selling_items(n):
    top_selling_items = data_analysis.top_selling_items(item_summary_df, n)
    if top_selling_items is not None:
        res = top_selling_items.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Top selling items not found"}), 404

@app.route('/most_profitable_items/<int:n>', methods=['GET'])
def get_most_profitable_items(n):
    most_profitable_items = data_analysis.most_profitable_items(item_summary_df, n)
    if most_profitable_items is not None:
        res = most_profitable_items.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Most profitable items not found"}), 404

@app.route('/most_profitable_stores/<int:n>', methods=['GET'])
def get_most_profitable_stores(n):
    most_profitable_stores = data_analysis.most_profitable_stores(store_summary_df, n)
    if most_profitable_stores is not None:
        res = most_profitable_stores.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Most profitable stores not found"}), 404

@app.route('/most_store_transactions/<int:n>', methods=['GET'])
def get_most_store_transactions(n):
    most_bought_from_stores = data_analysis.most_bought_from_stores(store_summary_df, n)
    if most_bought_from_stores is not None:
        res = most_bought_from_stores.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Most store transactions not found"}), 404

@app.route('/top_spenders/<int:n>', methods=['GET'])
def get_top_spenders(n):
    top_spenders = data_analysis.top_spenders(user_transactions_df, people_df, n)
    if top_spenders is not None:
        res = top_spenders.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Top spenders not found"}), 404

@app.route('/favorite_store/<int:user_id>', methods=['GET'])
def get_favorite_store_by_user_id(user_id):
    favorite_store = data_analysis.favorite_store_by_user_id(user_transactions_df, user_id)
    if not pd.isna(favorite_store):
        return jsonify({"favorite_store": favorite_store}), 200
    else:
        return jsonify({"error": "Favorite store not found"}), 404 

@app.route('/top_senders/<int:n>', methods=['GET'])
def get_top_senders(n):
    top_senders = data_analysis.top_senders(user_transfers_df, people_df, n)
    if top_senders is not None:
        res = top_senders.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Top senders not found"}), 404
    
@app.route('/top_receivers/<int:n>', methods=['GET'])
def get_top_receivers(n):
    top_receivers = data_analysis.top_receivers(user_transfers_df, people_df, n)
    if top_receivers is not None:
        res = top_receivers.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Top receivers not found"}), 404

@app.route('/total_spent_by_city/<int:n>', methods=['GET'])
def get_total_spent_by_city(n):
    total_spent_by_city = data_analysis.total_spent_by_city(user_transactions_df, people_df, n)
    if total_spent_by_city is not None:
        res = total_spent_by_city.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Total spent by city not found"}), 404

@app.route('/total_transactions_by_city/<int:n>', methods=['GET'])
def get_total_transactions_by_city(n):
    total_transactions_by_city = data_analysis.total_transactions_by_city(user_transactions_df, people_df, n)
    if total_transactions_by_city is not None:
        res = total_transactions_by_city.to_dict(orient='records')
        return jsonify(res), 200
    else:
        return jsonify({"error": "Total transactions by city not found"}), 404


def main():
    df_people_json, df_people_yml, df_promotions, df_transactions, df_transfers = load_data([
        'data/people.json',
        'data/people.yml',
        'data/promotions.csv',
        'data/transactions.xml',
        'data/transfers.csv'
    ])

    data_matching_conforming(df_people_json, df_people_yml, df_promotions, df_transactions, df_transfers)

    conformed_data = load_conformed_data()

    global item_summary_df, people_df, promotions_df, store_summary_df, transactions_df, transfers_df, user_transactions_df, user_transfers_df
    item_summary_df = conformed_data['item_summary']
    people_df = conformed_data['people']
    promotions_df = conformed_data['promotions']
    store_summary_df = conformed_data['store_summary']
    transactions_df = conformed_data['transactions']
    transfers_df = conformed_data['transfers']
    user_transactions_df = conformed_data['user_transactions']
    user_transfers_df = conformed_data['user_transfers']



if __name__ == '__main__':
    main()
    app.run(debug=True)