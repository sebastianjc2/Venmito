import pandas as pd
import xml.etree.ElementTree as ET
import yaml

class DataLoader:
    """This class will load data from different file formats."""
    def __init__(self, file_path):
        self.file_path = file_path

    def load_json_data(self):
        """Load JSON data."""
        return pd.read_json(self.file_path)

    def load_yaml_data(self):
        """Load YAML data."""
        with open(self.file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
            return pd.DataFrame(yaml_data)

    def load_csv_data(self):
        """Load CSV data."""
        return pd.read_csv(self.file_path)

    def load_xml_data(self):
        """Load and format XML data."""
        tree = ET.parse(self.file_path)
        root = tree.getroot()
        data = []

        for transaction in root.findall('transaction'):
            transaction_id = transaction.get('id')
            phone = transaction.find('phone').text
            store = transaction.find('store').text
            
            # Flattening the items, this way I can have a single row per item
            items = transaction.find('items')
            for item in items.findall('item'):
                item_name = item.find('item').text
                price = item.find('price').text
                price_per_item = item.find('price_per_item').text
                quantity = item.find('quantity').text
                data.append({
                    'id': transaction_id,
                    'item': item_name,
                    'price': price,
                    'price_per_item': price_per_item,
                    'quantity': quantity,
                    'phone': phone,
                    'store': store
                })
        
        return pd.DataFrame(data)

    def load_data(self):
        """Load data based on the file extension."""
        file_extension = self.file_path.split('.')[-1]
        if file_extension == 'json':
            return self.load_json_data()
        elif file_extension == 'yml':
            return self.load_yaml_data()
        elif file_extension == 'csv':
            return self.load_csv_data()
        elif file_extension == 'xml':
            return self.load_xml_data()
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")
