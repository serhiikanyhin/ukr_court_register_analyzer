import requests
import json
import time
import io
import zipfile
import asyncio
import pandas as pd
from modules.text import cleaner
from modules.database import models
from modules.database import models_graph
from concurrent.futures import ThreadPoolExecutor
import sys
import math


PD_CH_SIZE = 200000  # Pandas chunk size for handling large datasets
LST_CH_SIZE = 2000
MAX_WORKERS = 200
SIM_CH_SIZE = 1
P2W_CH_SIZE = 30000
P2P_CH_SIZE = 300


# Graph


def update_graph():

    def update_pars():
        paragraphs_list = [(i[0], i[1], i[2]) for i in
                           models.session.query(models.DocumentWord.doc_id,
                                                models.DocumentWord.word_par_num,
                                                models.func.count(models.DocumentWord.word_id))
                           .group_by(models.DocumentWord.doc_id, models.DocumentWord.word_par_num).all()]
        models_graph.create_paragraphs_multiple(paragraphs_list)

    def update_words():
        word_list = [(i[0], i[1]) for i in models.session.query(models.Vocabulary.word_id,
                                                                models.Vocabulary.word_text).all()]
        models_graph.create_words_multiple(word_list)

    def update_par_2_word_relations():
        vocab = [i.word_text for i in models.session.query(models.Vocabulary)]
        list_len = len([i for i in models.session.query(models.DocumentWord.doc_id)])
        print("Total length:", list_len, "par_2_word chunk number:", math.ceil(list_len / P2W_CH_SIZE))
        list_range = range(0, list_len)

        for num, i in enumerate(range(0, len(list_range), P2W_CH_SIZE)):
            par_2_word_rel = [(j[0], j[1], vocab.index(j[2])) for j in
                              models.session.query(models.DocumentWord.doc_id,
                                                   models.DocumentWord.word_par_num,
                                                   models.DocumentWord.word_text)
                                    .all()[i:i+P2W_CH_SIZE]]
            models_graph.set_par_2_word_relations(chunk=par_2_word_rel)
            print("Chunk:", num + 1, "Time:", time.strftime("%H:%M:%S", time.localtime()))

    def update_par_2_par_relations():
        list_len = len([i for i in models.session.query(models.DocumentWord.doc_id)])
        print("Total length:", list_len, "par_2_par chunk number:", math.ceil(list_len / P2P_CH_SIZE))
        list_range = range(0, list_len)

        for num, i in enumerate(range(0, len(list_range), P2P_CH_SIZE)):
            par_2_word_rel = [(j[0], j[1]) for j in
                              models.session.query(models.DocumentWord.doc_id,
                                                   models.DocumentWord.word_par_num)
                                    .all()[i:i+P2P_CH_SIZE]]
            models_graph.set_par_2_par_relations(chunk=par_2_word_rel, sim_cof=0.9, min_word_num=7)
            print("Chunk:", num + 1, "Time:", time.strftime("%H:%M:%S", time.localtime()))

    def update_connected_paragraphs():
        models.session.query(models.SimGroup).delete()
        search_result = models_graph.get_connected_paragraphs(min_par_num=10)
        conn_group = [[{"doc_id": doc_id, "word_par_num": group[1][doc_num], "group_id": group_id}
                      for doc_num, doc_id in enumerate(group[0])]
                      for group_id, group in enumerate(search_result)]
        conn_group = [j for i in conn_group for j in i]
        models.insert_many_sim_groups(conn_group)

    update_pars()
    update_words()
    update_par_2_word_relations()
    update_par_2_par_relations()
    update_connected_paragraphs()


# Vocabulary


def update_vocabulary():
    models.session.query(models.Vocabulary).delete()
    words = sorted(i.word_text for i in models.session.query(models.DocumentWord.word_text).distinct())
    words = [{
        "word_id": num,
        "word_text": i}
        for num, i in enumerate(words)]
    models.insert_vocabulary(words)


# Document text updaters


def update_doc_words(year):
    models.session.query(models.DocumentWord).filter_by(doc_year=year).delete()
    doc_id_urls = models.read_doc_links(year)
    doc_id_urls_chunks = [doc_id_urls[i:i + LST_CH_SIZE] for i in range(0, len(doc_id_urls), LST_CH_SIZE)]

    def update_chunk(num, chunk):
        records = []
        future = asyncio.ensure_future(get_words(chunk, records))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
        models.insert_many_doc_words(records)
        print("year:", str(year), "Document word chunk", num, "updated", time.strftime("%H:%M:%S", time.localtime()))

    [update_chunk(num, i) for num, i in enumerate(doc_id_urls_chunks)]


async def get_words(doc_id_urls, records):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        loop = asyncio.get_event_loop()
        for doc_id_url in doc_id_urls:
            loop.run_in_executor(executor, get_words_set, *(doc_id_url, records))


def get_words_set(doc_id_url, records):
    doc_id = doc_id_url[0]
    doc_url = doc_id_url[1]
    doc_year = doc_id_url[2]

    try:
        doc = requests.get(doc_url, allow_redirects=True)
    except requests.exceptions.MissingSchema:
        return False

    texts = cleaner.clean_rtf(doc.text)

    [[records.append({"doc_id": doc_id,
                      "word_par_num": num,
                      "word_text": word,
                      "doc_year": doc_year}) for word in text if word != ""] for num, text in enumerate(texts)]


def update_stop_words():
    df = pd.read_csv('files/stop_words.csv', encoding="cp1251")
    words = df['Слово'].tolist()
    [models.put_stop_words(word) for word in words]

    print("Stop words updated", time.strftime("%H:%M:%S", time.localtime()))


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


def update_metadata_zip_files(year, link):
    file = requests.get(link, allow_redirects=True)
    zip_bytes = file.content
    models.put_metadata_zip_file(year, zip_bytes)
    print("metadata zip files updated for year", year, time.strftime("%H:%M:%S", time.localtime()))


def update_metadata_csv_files(year):
    zip_bytes = models.read_zip(year)
    zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))
    for csv_name in zip_file.namelist():
        csv_bytes = zip_file.open(csv_name).read()
        models.put_metadata_csv_file(year, csv_name, csv_bytes)
    print("metadata csv files updated for year", year, time.strftime("%H:%M:%S", time.localtime()))
