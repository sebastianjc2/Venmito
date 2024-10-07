# Venmito Data Engineering Project

- Sebastian J. Caballero Díaz
- josiassebastian@gmail.com

## Introduction

Hello and welcome to this data engineering project for Venmito. Venmito is a payment company that allows users to transfer funds to other users and pay in participant stores. The company has several data files in various formats. Our goal is to organize all of this information to gain insights about our clients and transactions. The project is designed to facilitate data consumption for both non-technical and technical users, making it easier to integrate into other systems or applications.

### Approach

The project consists of several components:

- Data Ingestion: My program loads the data of each of the 5 files that are in the 'data/' folder, then formats the data accordingly (if needed) to store it in a Pandas DataFrame.
- Data Matching and Conforming: After successfully storing the data of the five files in dataframes, it matches and conforms the data to find common entities between the data, resolve any inconsistencies, and organize the data to fit the needs. During this process, the data is standardized and merged to create a unified dataset for analysis in the form of 8 new data files in csv, which are stored in the 'new_data/' folder.
- Data Analysis: Several methods are implemented to help extract information from the data and derive insights from it.
- Data Output: Built a simple API using Flask, providing a straightforward interface for the technical team to interact with the data. Also created a jupyter notebook where the data insights are displayed in the forms of graphs and tables for the non technical team.

### Technologies Used

- Python- Programming Language used for the project
- Flask- Framework for building the API
- Jupyter Notebook- Used for the data analysis and visualization
- Pandas- Data manipulation library used for the data processing and analysis.
- Other libraries: Including NumPy, Matplotlib, and others specified in `requirements.txt`

### Design Decisions

- Use of the Pandas library and csv files instead of creating a relational database and making use of technologies like PostgreSQL.
  - This decision was made pretty early on, since I thought it would be easier and more time efficient to use a library like Pandas, and not have to worry about setting up the database and schema. However, given more time, I would prefer to use an actual database.
- Matching and conforming
  - Spent a good amount of time on this part, deciding how to approach this problem. After some brainstorming, I ended up with 8 new datasets and wrote them to csv files for persistency.
  - Combined the people.json and people.yml data -> also had to decide between how to present the devices, location, and names, since they were conflicting formats between the 2 datasets, even though they had the same information. Decided to make the devices into one column, split the city and country in 2 columns, and leave the name as first_name and last_name, as it was in one of the files.
  - For transactions, I replaced the phone column with the corresponding user id. For Transfers, I created a transfer_id. For Promotions, I replaced the phone and email columns with the corresponding user id. This way, the user_id was in the 4 main dataframes people_df, transfers_df, transactions_df and promotions_df, essentially acting like a foreign key in the latter 3.
  - After having the 4 main dataframes, I created 4 new ones that are derived from the main ones that I thought might be useful for the later sections. The user transactions dataframe joins transactions and people by user id and keeps relevant info. The user transfers dataframe joins transfers and people by user id and keeps relevant info. The item summary dataframe takes some of the fields from transactions to create new ones such as an item's total revenue, the number of items sold, and the number of transactions that include this item. Similarly I created a store summary dataframe that also shows relevant information for each store such as revenue, total transactions, etc.
- For the Data Analysis section, I created a number of methods that would extract the information from the new data, providing insight on promotions, stores, items, users, and other extra data.
- For the output, I decided to go with a Jupyter Notebook approach for the Non-Technical team, that way the program can display graphs and tables for the data obtained in the data analysis. For the Technical team I decided to use flask and use flask endpoints to simulate a RESTful API where the data is converted into json format.

## How to run the program

Clone the repository into a folder of your choosing.
`git clone <url>`

Make sure to be in the folder that the program is in.
`cd <directory>`

Then, make sure to install the necessary requirements from the `requirements.txt` file.
`pip install -r requirements.txt`

Now, you can run the `main.py` file:
`$ python main.py`

Now the flask server should have started. Navigate to http://127.0.0.1:5000/ in your browser or http://localhost:5000/

You should see a message that says "Welcome to Venmito API!". Now you can navigate to the different routes, for example http://127.0.0.1:5000/top_spenders/5 to see the information of the top 5 users that have spent the most in a json format.

The technical team can navigate to the routes and get the information needed in json format. For the non-technical team, go to the `data_output.ipynb` file, and run the cells. Here, you can see the different information either in graphs or tables for all of the methods that are in the `data_analysis.py` file. This is the same information that you can find in the flask endpoints, but in a easier way to visualize.

### Future Improvements

- Validations and error handling: Although my program includes some validations, there can never be enough validations so with more time, more validations should be added.
- Add unit testing
- Database Integration
- Create API documentation
- Develop a frontend interface
