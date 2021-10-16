import csv
import json
import os
import sqlite3

# PUBLIC
file_name = 'data'
company_col_name = 'Company'
profit_col_name = 'Profit (in millions)'
table_name = 'companies'
top_hits = 20


# PRIVATE
full_path = os.path.realpath(__file__)
current_dir = os.path.dirname(full_path)
csv_file_name = file_name + '.csv'
db_file_name = file_name + '.db'
json_file_name = file_name + '.json'


def db_connect():
    db_connection = sqlite3.connect(db_file_name)
    return db_connection, db_connection.cursor()


def db_disconnect(db_connection, db_cursor):
    db_connection.commit()
    db_cursor.close()


def delete_file(fname):
    if os.path.exists(fname):
        os.remove(fname)


def create_json_file(data):
    delete_file(json_file_name)
    with open(json_file_name, 'w') as outfile:
        json.dump(data, outfile)


def create_db():
    delete_file(db_file_name)
    json_data = json.load(open(json_file_name))

    db_connection, db_cursor = db_connect()

    db_cursor.execute('CREATE TABLE ' + table_name +
                      ' (year, rank, company, revenue_m, profit_m)')

    for row in json_data:
        insert_row = [
            int(row['Year']),
            int(row['Rank']),
            row[company_col_name],
            float(row['Revenue (in millions)']),
            row[profit_col_name]
        ]

        db_cursor.execute('INSERT INTO ' + table_name +
                          ' VALUES (?, ?, ?, ?, ?)', insert_row)

    db_disconnect(db_connection, db_cursor)


def print_top_hits_sql(column, limit):
    db_connection, db_cursor = db_connect()
    db_cursor.execute('SELECT * FROM ' + table_name +
                      ' ORDER BY ' + column + ' DESC LIMIT ' + str(limit))

    for row in db_cursor.fetchall():
        print('> PART 3 (SQL):', str(row))

    db_disconnect(db_connection, db_cursor)


def print_top_hits_json(list, limit):
    sortedlist = sorted(
        list, key=lambda row: row[profit_col_name], reverse=True)
    count = 0

    for row in sortedlist:
        print('> PART 2 (JSON):', str(row))
        count += 1
        if (count >= limit):
            break


with open(current_dir + '\\' + csv_file_name, 'r') as file:

    reader = csv.DictReader(file)

    # Convert it to a list so we can use it like a mapped object with headers as keys
    csv_list = list(reader)

    # Count total rows (and invalid rows) while filtering to avoid the extra len() iterations
    total_rows = 0
    invalid_profit_rows = 0
    valid_profit_arr = []

    for row in csv_list:
        total_rows += 1
        profit = row[profit_col_name]
        if profit.isnumeric() is False:
            invalid_profit_rows += 1
            continue
        else:
            row[profit_col_name] = float(profit)
            # Adding rows "as-is", however, we can shorten the key names to avoid things like "..(in millions)" so our json file could be smaller
            valid_profit_arr.append(row)

    print('> PART 1: Number of total CSV data rows:', total_rows)
    print('> PART 1: Number of rows with valid numerical profit value:',
          total_rows - invalid_profit_rows)
    create_json_file(valid_profit_arr)
    print('> PART 2:', json_file_name,
          'created. Please check the project folder to confirm')



    
    #################### PART 2:
    print_top_hits_json(valid_profit_arr, top_hits)


    #################### PART 2 & 3 (+ extra SQL...just for fun):
    # create_db()
    # print_top_hits_sql('profit_m', top_hits)
    
