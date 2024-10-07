import pandas as pd

# Main DataFrames
class PeopleDF:
    def __init__(self, df_people_json, df_people_yml):
        self.df_people_json = df_people_json
        self.df_people_yml = df_people_yml

    # standardize devices
    def get_owned_devices(self, row):
        devices = []
        if row['Android'] == 1:
            devices.append('Android')
        if row['Iphone'] == 1:
            devices.append('Iphone')
        if row['Desktop'] == 1:
            devices.append('Desktop')
        return devices
    
    def convert_devices_to_string(self, devices):
        if isinstance(devices, list):
            return ', '.join(devices)
        return devices

    def standardize_data(self):
        # standardize devices
        self.df_people_yml['devices'] = self.df_people_yml.apply(self.get_owned_devices, axis=1)
        self.df_people_yml.drop(columns=['Android', 'Iphone', 'Desktop'], inplace=True)
        self.df_people_yml['devices'] = self.df_people_yml['devices'].apply(self.convert_devices_to_string)

        self.df_people_json['devices'] = self.df_people_json['devices'].apply(self.convert_devices_to_string)

        # standardize location
        location_json = pd.json_normalize(self.df_people_json['location'])
        self.df_people_json = pd.concat([self.df_people_json, location_json], axis=1)
        self.df_people_json.drop(columns=['location'], inplace=True)
        self.df_people_json.rename(columns={'City': 'city', 'Country': 'country'}, inplace=True)

        self.df_people_yml[['city', 'country']] = self.df_people_yml['city'].str.split(', ', expand=True)

        # standardize name
        self.df_people_yml[['first_name', 'last_name']] = self.df_people_yml['name'].str.split(expand=True)
        self.df_people_yml.drop(columns=['name'], inplace=True)

        # rename other columns
        self.df_people_json.rename(columns={'telephone': 'phone'}, inplace=True)

    def merge_data(self):
        people_df = pd.merge(self.df_people_json, self.df_people_yml, on=['id', 'first_name', 'last_name', 'email', 'phone', 'city', 'country', 'devices'], how='outer', suffixes=('_json', '_yml'))
        people_df.rename(columns={'id': 'user_id'}, inplace=True)
        people_df.to_csv('new_data/people_df.csv', index=False)
        return people_df


class PromotionsDF:
    def __init__(self, df_promotions, df_people):
        self.df_promotions = df_promotions
        self.df_people = df_people

    def standardize_data(self):
        self.df_promotions['user_id'] = None # initialize user_id column in promotions
        self.df_promotions.rename(columns={'id': 'promotion_id'}, inplace=True) 
        for index, row in self.df_promotions.iterrows():
            # find user_id based on client_email or telephone
            if not pd.isna(row['client_email']):
                user_id = self.df_people.loc[self.df_people['email'] == row['client_email'], 'user_id']
            else:
                user_id = self.df_people.loc[self.df_people['phone'] == row['telephone'], 'user_id']
        
            if not user_id.empty:
                self.df_promotions.at[index, 'user_id'] = user_id.values[0] # add corresponding user_id to promotions

        self.df_promotions.drop(columns=['telephone', 'client_email'], inplace=True) # drop unnecessary columns since now we have user id

    def resulting_df(self):
        self.df_promotions.to_csv('new_data/promotions_df.csv', index=False)
        return self.df_promotions


class TransactionsDF:
    def __init__(self, df_transactions, df_people):
        self.df_transactions = df_transactions
        self.df_people = df_people

    def standardize_data(self):
        self.df_transactions.rename(columns={'id': 'transaction_id'}, inplace=True)
        for index, row in self.df_transactions.iterrows():
            # find user_id based on phone
            user_id = self.df_people.loc[self.df_people['phone'] == row['phone'], 'user_id']

            if not user_id.empty:
                self.df_transactions.at[index, 'user_id'] = int(user_id.values[0])
                
        self.df_transactions.drop(columns=['phone'], inplace=True)

        # correcting data types
        self.df_transactions['user_id'] = self.df_transactions['user_id'].astype(int) 
        self.df_transactions['price'] = pd.to_numeric(self.df_transactions['price'], errors='coerce')
        self.df_transactions['price_per_item'] = pd.to_numeric(self.df_transactions['price_per_item'], errors='coerce')
        self.df_transactions['quantity'] = pd.to_numeric(self.df_transactions['quantity'], errors='coerce')

    def resulting_df(self):
        self.df_transactions.to_csv('new_data/transactions_df.csv', index=False)
        return self.df_transactions


class TransfersDF:
    def __init__(self, df_transfers):
        self.df_transfers = df_transfers

    def standardize_data(self):
        self.df_transfers['transfer_id'] = range(1, len(self.df_transfers) + 1) # initialize transfer_id column
    
    def resulting_df(self):
        self.df_transfers.to_csv('new_data/transfers_df.csv', index=False)
        return self.df_transfers
            

# Derived DataFrames
class UserTransactionsDF:
    def __init__(self, df_transactions, df_people):
        self.df_transactions = df_transactions
        self.df_people = df_people

    def get_favorite_store(self, stores):
        if not stores.mode().empty:
            return stores.mode()[0]
        else:
            return None
    
    def get_favorite_item(self, items):
        if not items.mode().empty:
            return items.mode()[0]
        else:
            return None

    def merge_data(self):
        merged_df = pd.merge(self.df_transactions, self.df_people, on='user_id', how='right')
        # find total_spent, transaction_count, favorite_store, favorite_item
        aggregated_df = merged_df.groupby('user_id').agg(total_spent=('price', 'sum'),
                                                         transaction_count=('transaction_id', 'nunique'),
                                                         favorite_store=('store', self.get_favorite_store),
                                                         favorite_item=('item', self.get_favorite_item))
        merged_df = pd.merge(self.df_people, aggregated_df, on='user_id', how='left')
        merged_df.drop(columns=['first_name', 'last_name', 'email', 'phone', 'city', 'country', 'devices'], inplace=True)
        merged_df.to_csv('new_data/user_transactions_df.csv', index=False)
        return merged_df
        

class UserTransfersDF:
    def __init__(self, df_transfers, df_people):
        self.df_transfers = df_transfers
        self.df_people = df_people

    def merge_data(self):
        sent_total = self.df_transfers.groupby('sender_id')['amount'].sum()
        received_total = self.df_transfers.groupby('recipient_id')['amount'].sum()
        net_transferred = received_total.subtract(sent_total, fill_value=0)

        sent_count = self.df_transfers.groupby('sender_id')['transfer_id'].nunique()
        received_count = self.df_transfers.groupby('recipient_id')['transfer_id'].nunique()
        total_transfers = sent_count.add(received_count, fill_value=0).astype(int)

        new_df = pd.DataFrame()
        new_df['total_sent'] = sent_total
        new_df['total_received'] = received_total
        new_df['net_transferred'] = net_transferred
        new_df['transaction_count'] = total_transfers
        new_df.index.name = 'user_id'

        user_info = self.df_people[['user_id']]

        merged_df = pd.merge(user_info, new_df, on='user_id', how='left')
        merged_df.fillna(0, inplace=True)
        merged_df['transaction_count'] = merged_df['transaction_count'].astype(int)
        merged_df.to_csv('new_data/user_transfers_df.csv', index=False)

        return merged_df


class ItemSummaryDF:
    def __init__(self, transactions_df):
        self.transactions_df = transactions_df
    
    def aggregate_data(self):
        # find total_revenue, items_sold, transaction_count
        aggregated_df = self.transactions_df.groupby('item').agg(total_revenue=('price', 'sum'),
                                                                 items_sold=('quantity', 'sum'),
                                                                 transaction_count=('transaction_id', 'nunique')).reset_index()
        aggregated_df.to_csv('new_data/item_summary_df.csv', index=False)
        return aggregated_df

class StoreSummaryDF:
    def __init__(self, transactions_df):
        self.transactions_df = transactions_df
    
    def aggregate_data(self):
        #find total_revenue, items_sold, transaction_count
        aggregated_df = self.transactions_df.groupby('store').agg(
            total_revenue=('price', 'sum'),
            items_sold=('quantity', 'sum'),
            total_transactions=('transaction_id', 'nunique'),
        )

        item_qty = self.transactions_df.groupby(['store', 'item'])['quantity'].sum() 
        most_sold_item = item_qty.groupby(level=0).idxmax().apply(lambda x: x[1]) 
        aggregated_df['most_sold_item'] = most_sold_item

        item_price = self.transactions_df.groupby(['store', 'item'])['price'].sum()
        most_profitable_item = item_price.groupby(level=0).idxmax().apply(lambda x: x[1])
        aggregated_df['most_profitable_item'] = most_profitable_item

        aggregated_df['average_transaction_value'] = (aggregated_df['total_revenue'] / aggregated_df['total_transactions']).round(2)
        aggregated_df.to_csv('new_data/store_summary_df.csv')
        return aggregated_df
