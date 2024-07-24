from langchain_openai import AzureOpenAIEmbeddings
from scipy.spatial.distance import cdist
from openai import AzureOpenAI
import cx_Oracle, json, os, re
import pandas as pd
import numpy as np

from config import *

client = AzureOpenAI(
    azure_endpoint=AZURE_OAI_ENDPOINT,
    api_key=AZURE_OAI_KEY,
    api_version=AZURE_OAI_VERSION
)

embeddings = AzureOpenAIEmbeddings(
    azure_deployment='stryker_embedding_ai',
    openai_api_version=AZURE_OAI_VERSION,
    api_key=AZURE_OAI_KEY,
    azure_endpoint=AZURE_OAI_ENDPOINT
)

def fetch_schema():
    connection = cx_Oracle.connect(
        user=ORACLE_USERNAME,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )

    try:
        cursor = connection.cursor()

        sql_query = "SELECT table_name FROM all_tables WHERE owner = 'SYSADM' AND table_name LIKE 'V_ARC%'"
        cursor.execute(sql_query)
        tables = [row[0] for row in cursor.fetchall()]

        columns = {}
        for table in tables:
            cursor.execute(f"SELECT column_name, data_type FROM all_tab_columns WHERE table_name = '{table}' AND owner = 'SYSADM'")
            table_columns = cursor.fetchall()

            filtered_columns = [(col_name, data_type) for col_name, data_type in table_columns if 'ROWID' not in col_name]
            columns[table] = filtered_columns
        
    
        df_data = []
        for table_name, cols in columns.items():
            for column_name, data_type in cols:
                df_data.append({
                    'Table_Name': table_name,
                    'Column_Name': column_name,
                    'Data_Type': data_type
                })
        
        return df_data

    finally:
        cursor.close()
        connection.close()


def parse_table_info(input_string):
    tables = []
    amt_tables = 0
    
    table_pattern = r"Table[s]?:\s*(.*?)\n"
    amt_tables_pattern = r"Amount of Tables:\s*(\d+)"
    column_pattern = r"Column[s]?:\s*(.*?)\n"
    amt_columns_pattern = r"Amount of Columns:\s*(\d+)"
    
    matches = re.findall(table_pattern, input_string, re.DOTALL)
    if matches:
        tables = [match.strip() for match in matches[0].split(",")]
    
    match = re.search(amt_tables_pattern, input_string)
    if match:
        amt_tables = int(match.group(1))

    matches2 = re.findall(column_pattern, input_string, re.DOTALL)
    if matches2:
        columns = [match2.strip() for match2 in matches2[0].split(",")]
    
    match2 = re.search(amt_columns_pattern, input_string)
    if match2:
        amt_columns = int(match2.group(1))
    
    return tables, amt_tables, columns, amt_columns


def find_tables(tables1, amt_tables1, df, df_embeddings):
    searched_tables = []
    for i in range(amt_tables1):
        value_str = f"{tables1[i]}"  
        query_vector = embeddings.embed_query(value_str)
        
        table_embeddings = np.array(df_embeddings['Embeddings'].tolist())
        similarities = 1 - cdist(np.expand_dims(query_vector, axis=0), table_embeddings, metric='cosine')[0]
        top_indices = similarities.argsort()[-4:][::-1]
        
        top_table_descriptions = []
        for idx in top_indices:
            table_name = df_embeddings.loc[idx, 'Unique_Table_Names']
            top_table_descriptions.append(table_name)
        
        for table_description in top_table_descriptions:
            indices = df.index[df['Cleaned_Table_Name'] == table_description].tolist()
            if indices:
                index = indices[0]
                table_name = df.loc[index, 'Table_Name']
                searched_tables.append(table_name)
    return searched_tables


def find_columns(columns1, amt_columns1, df):
    top_column_info = []
    for i in range(amt_columns1):
        value_str = f"{columns1[i]}"  
        query_vector = embeddings.embed_query(value_str)

        column_embeddings = np.array(df['column_name_cleaned_embedding'].tolist())
        similarities = 1 - cdist(np.expand_dims(query_vector, axis=0), column_embeddings, metric='cosine')[0]
        top_indices = similarities.argsort()[-2:][::-1]
        
        temp_top_column_info = []
        for idx in top_indices:
            table_name = df.loc[idx, 'Table_Name']
            column_name = df.loc[idx, 'Column_Name']
            data_type = df.loc[idx, 'Data_Type']
            formatted_info = f'("{column_name}", {data_type}, {table_name})'
            temp_top_column_info.append(formatted_info)

        top_column_info.extend(temp_top_column_info)

    return top_column_info


def create_column_schema(top_column_info):
    transformed_list = []
    for item in top_column_info:
        parts = item.strip('()').split(', ')
        column_name = parts[0].strip('"')
        data_type = parts[1]
        table_name = parts[2]
        transformed_item = f'("{column_name}", {data_type}, {table_name})'
        transformed_list.append(transformed_item)

    columns_schema = '\n'.join(transformed_list)
    return columns_schema


def clean_column_name(column_name):
    cleaned_name = column_name.replace('_', ' ').lower()
    return cleaned_name


def create_description(table_name):
    cleaned_name = table_name.replace('V_ARC_', '').replace('_', ' ').lower()
    return cleaned_name


def create_embedding(description):
    embedding = embeddings.embed_query(description)
    return embedding

def execute_sql_query(sql_query):
    connection = cx_Oracle.connect(
        user=ORACLE_USERNAME,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )
    
    try:
        cursor = connection.cursor()
        cursor.execute(sql_query) 
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        return cursor, columns, rows
    
    finally:
        cursor.close()
        connection.close()


# RUN THIS IF NEEDED TO UPDATE TABLE AND/OR COLUMN NAMES
# Function to update database schema data
def update_db_data():
    df_data = fetch_schema()
    df = pd.DataFrame(df_data)
    df['Cleaned_Table_Name'] = df['Table_Name'].apply(create_description)
    df['column_name_cleaned'] = df['Column_Name'].apply(lambda x: clean_column_name(x))
    df['column_name_cleaned_embedding'] = df['column_name_cleaned'].apply(lambda x: create_embedding(x))
    df['column_name_cleaned_embedding_json'] = df['column_name_cleaned_embedding'].apply(json.dumps)
    df = df.drop(columns=['column_name_cleaned_embedding'])
    csv_filename = 'data/embedding_csv.csv'
    df.to_csv(csv_filename, index=False)

# Function to export data to CSV
def export_excel(data):
    csv_filename = 'extracted_data.csv'
    data.to_csv(csv_filename, index=False)
    return csv_filename