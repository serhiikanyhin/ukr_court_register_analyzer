import zipfile
import io
import requests
import json
import pandas as pd
import time
from modules.database import models


PD_CH_SIZE = 200000  # Pandas chunk size for handling large datasets


# Document text updaters


def update_texts(year):
    return 1


# Metadata tables functions


def update_documents(year):
    models.session.query(models.Document).filter_by(doc_year=year).delete()
    name = 'documents.csv'
    chunks = pd.read_csv(io.BytesIO(models.read_csv(name, str(year))), sep='\t', iterator=True, chunksize=PD_CH_SIZE)
    for num, df_chunk in enumerate(chunks):
        print("year:", str(year), "chunk:", num, time.strftime("%H:%M:%S", time.localtime()))
        df_chunk["doc_year"] = int(year)

        df_chunk = df_chunk[['doc_id', 'court_code', 'judgment_code',
                             'justice_kind', 'category_code', 'cause_num',
                             'receipt_date', 'judge', 'doc_url',
                             'status', 'date_publ', 'doc_year']]

        df_chunk = df_chunk[df_chunk.category_code.isin([5618, 5625, 5626, 5655,
                                                         5694, 5709, 11783, 11691,
                                                         11692, 40114, 40115, 40116])]  # 'ПДВ'

        df_chunk = df_chunk[df_chunk.judgment_code.isin([3])]  # 'Рішення'

        # df_chunk['category_code'] = df_chunk['category_code'].replace([np.nan, 11783], 41465)  # 'Інші процесуальні питання'
        # df_chunk['court_code'] = df_chunk['court_code'].replace(np.nan, 9999)  # 'Верховний Суд України'
        # df_chunk['judgment_code'] = df_chunk['judgment_code'].replace([np.nan, 0, 7, 8, 9, 15], 3)  # 'Рішення'
        # df_chunk['justice_kind'] = df_chunk['justice_kind'].replace([np.nan, 0], 4)  # 'Адміністративне'

        models.insert_many_documents(df_chunk)

    print("year:", str(year), "Documents updated", time.strftime("%H:%M:%S", time.localtime()))


def update_courts(year):
    # Courts
    file_name = 'courts.csv'
    bytes_data = models.read_csv(file_name, int(year))
    df = pd.read_csv(io.BytesIO(bytes_data), sep='\t')
    [models.put_court(row) for index, row in df.iterrows()]
    print("Courts updated, year:", year)


def update_structure(year):
    # Regions
    file_name = 'regions.csv'
    bytes_data = models.read_csv(file_name, int(year))
    df = pd.read_csv(io.BytesIO(bytes_data), sep='\t')
    [models.put_region(row) for index, row in df.iterrows()]
    # print("Regions updated, year:", year)

    # Instances
    file_name = 'instances.csv'
    bytes_data = models.read_csv(file_name, int(year))
    df = pd.read_csv(io.BytesIO(bytes_data), sep='\t')
    [models.put_instance(row) for index, row in df.iterrows()]
    # print("Instances updated, year:", year)

    # Cause categories
    file_name = 'cause_categories.csv'
    bytes_data = models.read_csv(file_name, int(year))
    df = pd.read_csv(io.BytesIO(bytes_data), sep='\t')
    [models.put_cause_category(row) for index, row in df.iterrows()]
    # print("Cause categories updated, year:", year)

    # Justice kinds
    file_name = 'justice_kinds.csv'
    bytes_data = models.read_csv(file_name, int(year))
    df = pd.read_csv(io.BytesIO(bytes_data), sep='\t')
    [models.put_justice_kind(row) for index, row in df.iterrows()]
    # print("Justice kinds updated, year:", year)

    # Judgment forms
    file_name = 'judgment_forms.csv'
    bytes_data = models.read_csv(file_name, int(year))
    df = pd.read_csv(io.BytesIO(bytes_data), sep='\t')
    [models.put_judgment_form(row) for index, row in df.iterrows()]
    # print("Judgment forms updated, year:", year)

    # Courts
    file_name = 'courts.csv'
    bytes_data = models.read_csv(file_name, int(year))
    df = pd.read_csv(io.BytesIO(bytes_data), sep='\t')
    [models.put_court(row) for index, row in df.iterrows()]
    # print("Courts updated, year:", year)

    print("Structure updated, year:", year, time.strftime("%H:%M:%S", time.localtime()))


#  Metadata files functions


def update_metadata_links():
    with open('files/metadata/links.json') as json_file:
        metadata_links = json.load(json_file)
        for year, link in metadata_links.items():
            models.put_metadata_link(year, link)
        print("metadata links updated for year", year, time.strftime("%H:%M:%S", time.localtime()))


def update_metadata_zip_files():
    metadata_links = models.read_meta_data_links()
    for i in metadata_links:
        year, link = i[0], i[1]
        file = requests.get(link, allow_redirects=True)
        zip_bytes = file.content
        models.put_metadata_zip_file(year, zip_bytes)
        print("metadata zip files updated for year", year, time.strftime("%H:%M:%S", time.localtime()))


def update_metadata_csv_files():
    metadata_links = models.read_meta_data_links()
    for i in metadata_links:
        year = int(i[0])
        zip_bytes = models.read_zip(year)
        zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))
        for csv_name in zip_file.namelist():
            csv_bytes = zip_file.open(csv_name).read()
            models.put_metadata_csv_file(year, csv_name, csv_bytes)
        print("metadata csv files updated for year", year, time.strftime("%H:%M:%S", time.localtime()))


# Support functions


def get_left_outer(left_df, right_df_model, column_name):
    right_df = pd.read_sql(models.session.query(right_df_model).statement, models.session.bind)
    right_df = right_df[[column_name]]
    outer_left_df = pd.merge(left_df, right_df, how='outer', on=column_name, indicator=True)
    outer_left_df = outer_left_df[outer_left_df['_merge'] == 'left_only']
    outer_left_list = list(outer_left_df[column_name].unique())
    return outer_left_list
