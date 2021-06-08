import sqlite3
import time
import json
import numpy as np
import pandas as pd

# SQL PARAMETERS

VAT_similarity_db_path=(r"C:/Users/skanyhin001/Desktop/RESEARCH TOOL/EDRSR/VAT_similarity_database.db")
conn = sqlite3.connect(VAT_similarity_db_path)
cur = conn.cursor()

# SQL EXTRACTING TO LIST

sqlite_select_query = """SELECT * from Court_similarity"""
cur.execute(sqlite_select_query)
records = cur.fetchall()  # doc_id (0), therefore_stemmed(3)
data_list = []
doc_id_list = []
for row in records: 
    doc_id_list.append(row[0])
    data_list.append(row[3])

# NUMPY SIMILARITY CALCULATION


arr = np.zeros((len(data_list), len(data_list)))
for x in range(0, len(data_list)): 
    x_text = json.loads(data_list[x])        
    if x_text:
        similarity_list_SQL = []
        for y in range(0, len(data_list)):                
            y_text = json.loads(data_list[y])                
            if y_text:                   
                similarity_list = []
                for i in x_text:
                    for j in y_text:
                        similarity = len(set(i) & set(j)) / float(len(set(i) | set(j)))
                        if similarity > 0.65: 
                            similarity = 1
                            if y != x: 
                                similarity_list_SQL.append(doc_id_list[y])
                        else:
                            similarity = 0
                        similarity_list.append(similarity)
                arr[x, y] = max(similarity_list)
        if similarity_list_SQL: 
            similarity_list_SQL = list(set(similarity_list_SQL))
            doc_id = doc_id_list[x]                  
            cur.execute('Update Court_similarity set similarity_list=? where doc_id=?', 
                        (str(similarity_list_SQL), doc_id)) 
                    
# SQL COMMIT AND CLOSE

conn.commit()          
cur.close()          
conn.close()
            
# PANDAS DATAFRAME PROCESSING
    
similarity_df = pd.DataFrame(arr)  # transforming numpy array to pandas dataframe
similarity_df.columns = doc_id_list  # set columns names
similarity_df.index = doc_id_list  # set rows names
similarity_df = similarity_df.loc[(similarity_df != 0).any(axis=1)]  # drop zero rows
similarity_df = similarity_df.loc[:, (similarity_df != 0).any(axis=0)]  # drop zero columns
# print(similarity_df)

# SORTING DATAFRAME

columns = (list(similarity_df.columns))

for idx in range(2):    
    for i in columns:
        similarity_df = similarity_df.sort_values(by=i, axis=1, ascending = True)
    for i in columns:
        similarity_df = similarity_df.sort_values(by=i, axis=0, ascending = True)
    print("Sorting iteration № "+str(idx)+" is completed.")
