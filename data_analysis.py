import pandas as pd

def get_user(people_df, user_id):
    """This function will return the user details of a specific user."""
    user = people_df[people_df['user_id'] == user_id]
    return user

def promotions_by_type_of_clients(promotions_df, people_df):
    """This function returns a DataFrame showing which type of clients are receiving which types of promotions."""
    merged_df = pd.merge(promotions_df, people_df, on='user_id', how='left')
    promotion_by_client_type = merged_df.groupby(['promotion', 'country'])
    # Count the number of users who received a promotion (already grouped by promotion and country)
    promotion_by_client_type = promotion_by_client_type['user_id'].size().reset_index(name='count')
    return promotion_by_client_type

def promotions_by_type_of_clients_with_acceptance_rate(promotions_df, people_df):
    """This function returns a DataFrame showing which type of clients are receiving which types of promotions."""
    merged_df = pd.merge(promotions_df, people_df, on='user_id', how='left')
    promotion_by_client_type = merged_df.groupby(['promotion', 'country'])
    promotion_by_client_type = promotion_by_client_type['user_id'].size().reset_index(name='count')

    yes_counts = merged_df[merged_df['responded'] == 'Yes'].groupby(['promotion', 'country']).size().astype(int).reset_index(name='yes_count')
    
    promotion_summary = pd.merge(promotion_by_client_type, yes_counts, on=['promotion', 'country'], how='left').fillna(0)

    promotion_summary['acceptance_rate'] = ((promotion_summary['yes_count'] / promotion_summary['count']).fillna(0) * 100).round(2)
    promotion_summary.drop('yes_count', axis=1, inplace=True)
    return promotion_summary

def items_sold_per_country(transactions_df, people_df):
    """This function will return the number of items sold in each country."""
    merged_df = pd.merge(transactions_df, people_df, on='user_id', how='left')
    items_sold_per_country = merged_df.groupby(['item','country'])
    items_sold_per_country = items_sold_per_country['quantity'] # Count the number of items sold
    final_res = items_sold_per_country.sum().sort_values(ascending=False).reset_index()
    return final_res

def promotion_responded(promotions_df, response):
    """This function will return the number of people who responded 'Yes' or 'No' to a promotion."""
    promotion_responded = promotions_df[promotions_df['responded'] == response].value_counts().count()
    return promotion_responded

def client_response_rate(promotions_df):
    """This function will return the percentage of people who responded to a promotion."""
    promotion_response_rate = promotions_df['responded'].value_counts(normalize=True).values
    return promotion_response_rate

def promotion_response_yes_buyer(promotions_df, transactions_df):
    """This function will return the number of people who responded 'Yes' to a promotion and made a purchase of the promoted item."""
    merged_df = pd.merge(promotions_df, transactions_df, on='user_id', how='inner')
    response_yes_buyer = merged_df[merged_df['responded'] == 'Yes'] # Filter only the people who responded 'Yes'
    response_yes_buyer = response_yes_buyer[response_yes_buyer['item'] == response_yes_buyer['promotion']] #filter only the people who bought the promoted item
    response_yes_buyer.reset_index(drop=True, inplace=True)
    return response_yes_buyer[['user_id', 'promotion', 'responded', 'item', 'price']]

def analyze_client_promotions(promotions_df, people_df, n=5):
    """This function will return the top n promotions given to clients."""
    merged_df = pd.merge(promotions_df, people_df, on='user_id', how='left')
    promotion_summary = merged_df.groupby('promotion')['user_id'].count().sort_values(ascending=False).reset_index()
    promotion_summary.columns = ['promotion', 'client_count']
    return promotion_summary.head(n)

def possible_conversion_strategy(user_transactions_df, promotions_df, people_df):
    """This function intends to show the potential clients who should be targeted based on their spending habits, as well as their most bought item."""
    no_response_df = promotions_df[promotions_df['responded'] == 'No']
    merged_df = pd.merge(no_response_df, people_df, on='user_id', how='left')
    high_spending = user_transactions_df[user_transactions_df['total_spent'] > 20]
    merged_df = pd.merge(user_transactions_df, merged_df, on='user_id', how='left')
    target_clients = merged_df[merged_df['user_id'].isin(high_spending['user_id'])].sort_values('total_spent',ascending=False).reset_index(drop=True)
    return target_clients[['user_id', 'total_spent', 'promotion', 'responded', 'favorite_item']]


def top_selling_items(item_summary_df, n=5):
    """This function will return the top n selling items."""
    top_selling_items = item_summary_df.sort_values('items_sold', ascending=False).head(n)
    return top_selling_items

def most_profitable_items(item_summary_df, n=5):
    """This function will return the top n most profitable items."""
    most_profitable_items = item_summary_df.sort_values('total_revenue', ascending=False).head(n)
    return most_profitable_items

def most_profitable_stores(store_summary_df, n=5):
    """This function will return the top n most profitable stores."""
    most_profitable_stores = store_summary_df.sort_values('total_revenue', ascending=False).head(n)
    return most_profitable_stores

def most_bought_from_stores(store_summary_df, n=5):
    """This function will return the top n stores where most items were bought."""
    most_bought_from_stores = store_summary_df.sort_values('total_transactions', ascending=False).head(n)
    return most_bought_from_stores


def top_spenders(user_transactions_df, people_df, n=5):
    """This function will return the top n spenders."""
    top_spenders = user_transactions_df.sort_values('total_spent', ascending=False).head(n)
    merged_df = pd.merge(top_spenders, people_df, on='user_id', how='left')
    return merged_df[['user_id', 'first_name','last_name','total_spent']]

def favorite_store_by_user_id(user_transactions_df, user_id):
    """This function will return the favorite store of a specific user."""
    favorite_stores = user_transactions_df[user_transactions_df['user_id'] == user_id]['favorite_store']
    if favorite_stores.empty:
        return None
    return favorite_stores.values[0]


def top_senders(user_transfer_df, people_df, n=5):
    """This function will return the top n senders."""
    top_senders = user_transfer_df.sort_values('total_sent', ascending=False).head(n)
    merged_df = pd.merge(top_senders, people_df, on='user_id', how='left')
    return merged_df[['user_id', 'first_name', 'last_name', 'total_sent']]

def top_receivers(user_transfer_df, people_df, n=5):
    """This function will return the top n receivers."""
    top_receivers = user_transfer_df.sort_values('total_received', ascending=False).head(n)
    merged_df = pd.merge(top_receivers, people_df, on='user_id', how='left')
    return merged_df[['user_id', 'first_name', 'last_name', 'total_received']]


def total_spent_by_city(user_transactions_df, people_df, n=5):
    """This function will return the total amount spent in each city."""
    merged_df = pd.merge(user_transactions_df, people_df, on='user_id', how='left')
    total_spent_by_city = merged_df.groupby(['city','country'])['total_spent'].sum().sort_values(ascending=False).reset_index().head(n)
    return total_spent_by_city

def total_transactions_by_city(user_transactions_df, people_df, n=5):
    """This function will return the total number of transactions in each city."""
    merged_df = pd.merge(user_transactions_df, people_df, on='user_id', how='left')
    total_transactions_by_city = merged_df.groupby(['city','country'])['transaction_count'].sum().sort_values(ascending=False).reset_index().head(n)
    return total_transactions_by_city
