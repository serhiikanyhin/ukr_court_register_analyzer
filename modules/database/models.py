import math
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, LargeBinary, Float, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


db_string = "postgresql://postgres:xxxx@localhost:5432/database"
db = create_engine(db_string)
database = declarative_base()
FILE_CH_SIZE = 52428800  # number of bytes in 50 megabytes
DB_CH_SIZE = 100000  # Chunk size for insert many
pd.options.mode.chained_assignment = None  # default='warn'


# Metadata tables


class Document(database):
    __tablename__ = 'documents'
    doc_id = Column(Integer, primary_key=True)
    court_code = Column(Integer, ForeignKey('courts.court_code'))
    judgment_code = Column(Integer, ForeignKey('judgment_forms.judgment_code'))
    justice_kind = Column(Integer, ForeignKey('justice_kinds.justice_kind'))
    category_code = Column(Integer, ForeignKey('cause_categories.category_code'))
    cause_num = Column(String)
    receipt_date = Column(String)
    judge = Column(String)
    doc_url = Column(String)
    status = Column(String)
    date_publ = Column(String)
    doc_year = Column(Integer)


class Court(database):
    __tablename__ = 'courts'
    court_code = Column(Integer, primary_key=True)
    name = Column(String)
    instance_code = Column(Integer, ForeignKey('instances.instance_code'))
    region_code = Column(Integer, ForeignKey('regions.region_code'))


class JudgmentForm(database):
    __tablename__ = 'judgment_forms'
    judgment_code = Column(Integer, primary_key=True)
    name = Column(String)


class JusticeKind(database):
    __tablename__ = 'justice_kinds'
    justice_kind = Column(Integer, primary_key=True)
    name = Column(String)


class CauseCategory(database):
    __tablename__ = 'cause_categories'
    category_code = Column(Integer, primary_key=True)
    name = Column(String)


class Instance(database):
    __tablename__ = 'instances'
    instance_code = Column(Integer, primary_key=True)
    name = Column(String)


class Region(database):
    __tablename__ = 'regions'
    region_code = Column(Integer, primary_key=True)
    name = Column(String)


class MetadataLink(database):
    __tablename__ = 'metadata_links'
    link_id = Column(Integer, primary_key=True)
    link_year = Column(Integer)
    link_url = Column(String)


class MetadataZipFile(database):
    __tablename__ = 'metadata_zip_files'
    zip_id = Column(Integer, primary_key=True)
    zip_year = Column(Integer)
    zip_chunk_num = Column(Integer)
    zip_binary = Column(LargeBinary)


class MetadataCsvFile(database):
    __tablename__ = 'metadata_csv_files'
    csv_id = Column(Integer, primary_key=True)
    csv_year = Column(Integer)
    csv_name = Column(String)
    csv_chunk_num = Column(Integer)
    csv_binary = Column(LargeBinary)


class StopWord(database):
    __tablename__ = 'stop_words'
    word_id = Column(Integer, primary_key=True)
    word_text = Column(String)


class DocumentWord(database):
    __tablename__ = 'documents_words'
    word_id = Column(Integer, primary_key=True)
    doc_id = Column(Integer, ForeignKey('documents.doc_id'))
    doc_year = Column(Integer)
    word_par_num = Column(Integer)
    word_text = Column(String)


class Vocabulary(database):
    __tablename__ = 'vocabulary'
    word_id = Column(Integer, primary_key=True)
    word_text = Column(String)


class SimGroup(database):
    __tablename__ = 'similarity_groups'
    doc_group_id = Column(Integer, primary_key=True)
    group_id = Column(Integer)
    doc_id = Column(Integer, ForeignKey('documents.doc_id'))
    word_par_num = Column(Integer)

# READ FUNCTIONS


def read_meta_data_links():
    meta_data_links = [(i.link_year, i.link_url) for i in session.query(MetadataLink)]
    return meta_data_links


def read_zip(year):
    binary_chunks_zip = session.query(MetadataZipFile)\
        .filter_by(zip_year=year)\
        .order_by(MetadataZipFile.zip_chunk_num.asc())
    zip_bytes = b''.join([i.zip_binary for i in binary_chunks_zip])
    return zip_bytes


def read_csv(file_name, year):
    binary_chunks_csv = session.query(MetadataCsvFile)\
        .filter_by(csv_name=file_name, csv_year=year)\
        .order_by(MetadataCsvFile.csv_chunk_num.asc())
    csv_bytes = b''.join([i.csv_binary for i in binary_chunks_csv])
    return csv_bytes


def read_doc_links(year):
    doc_links = [(i.doc_id, i.doc_url, i.doc_year) for i in session.query(Document).filter_by(doc_year=year)]
    return doc_links


def read_stop_words():
    stop_words = [i.word_text for i in session.query(StopWord)]
    return stop_words


# INSERT MANY


def insert_many_sim_groups(records):
    for rec_chunk in [records[i:i + DB_CH_SIZE] for i in range(0, len(records), DB_CH_SIZE)]:
        session.bulk_insert_mappings(SimGroup, rec_chunk)
        session.commit()


def insert_vocabulary(words):
    session.bulk_insert_mappings(Vocabulary, words)
    session.commit()


def insert_many_doc_words(records):
    session.bulk_insert_mappings(DocumentWord, records)
    session.commit()


def update_many_doc_paragraphs(records):
    session.bulk_update_mappings(Document, records)
    session.commit()


def insert_many_documents(df):
    for df_chunk in split_df(df, DB_CH_SIZE):
        df_dict = df_chunk.to_dict('records')
        try:
            session.bulk_insert_mappings(Document, df_dict)
            session.commit()

        except Exception as error:
            session.rollback()

            def delete_duplicates():
                db_doc_ids = [i.doc_id for i in session.query(Document.doc_id)]
                df_doc_ids = df_chunk['doc_id'].tolist()
                inner_doc_ids = list(set(db_doc_ids) & set(df_doc_ids))

                if len(inner_doc_ids) > 0:
                    print("Duplicates to delete:", len(inner_doc_ids))
                    query_obj = session.query(Document)
                    in_expression = Document.doc_id.in_(inner_doc_ids)
                    query_obj.filter(in_expression).delete()

            delete_duplicates()

            def recursion_func():
                if len(df) < 2:
                    print(error)
                else:
                    size = math.floor(len(df)/2)
                    number = int(len(df) / size)
                    frames = [df.iloc[i * size:(i + 1) * size].copy() for i in range(number + 1)]

                    for i in frames:
                        insert_many_documents(i)

            recursion_func()


# Put metadata functions


def put_document(df_row, year):
    document = session.query(Document).filter_by(doc_id=df_row['doc_id'], doc_year=year).first()

    category_code = df_row['category_code']
    if not df_row['category_code'].is_integer():
        category_code = 41465  # 'Інші процесуальні питання'

    if not document:
        document = Document(
            doc_id=df_row['doc_id'],
            court_code=df_row['court_code'],
            judgment_code=df_row['judgment_code'],
            justice_kind=df_row['justice_kind'],
            category_code=category_code,
            cause_num=df_row['cause_num'],
            receipt_date=df_row['receipt_date'],
            judge=df_row['judge'],
            doc_url=df_row['doc_url'],
            status=df_row['status'],
            date_publ=df_row['date_publ'],
            doc_year=year
        )
        session.add(document)
    else:
        document.doc_id = df_row['doc_id']
        document.court_code = df_row['court_code']
        document.judgment_code = df_row['judgment_code']
        document.justice_kind = df_row['justice_kind']
        document.category_code = category_code
        document.cause_num = df_row['cause_num']
        document.receipt_date = df_row['receipt_date']
        document.judge = df_row['judge']
        document.doc_url = df_row['doc_url']
        document.status = df_row['status']
        document.date_publ = df_row['date_publ']

    session.commit()


def put_court(df_row):
    court = session.query(Court).filter_by(court_code=df_row['court_code']).first()
    if not court:
        court = Court(
            court_code=df_row['court_code'],
            name=df_row['name'],
            instance_code=df_row['instance_code'],
            region_code=df_row['region_code']
        )
        session.add(court)
    else:
        court.name = df_row['name']
        court.instance_code = df_row['instance_code']
        court.region_code = df_row['region_code']
    session.commit()


def put_judgment_form(df_row):
    judgment_form = session.query(JudgmentForm).filter_by(judgment_code=df_row['judgment_code']).first()
    if not judgment_form:
        judgment_form = JudgmentForm(
            judgment_code=df_row['judgment_code'],
            name=df_row['name']
        )
        session.add(judgment_form)
    else:
        judgment_form.judgment_code = df_row['judgment_code']
        judgment_form.name = df_row['name']
    session.commit()


def put_justice_kind(df_row):
    justice_kind = session.query(JusticeKind).filter_by(justice_kind=df_row['justice_kind']).first()
    if not justice_kind:
        justice_kind = JusticeKind(
            justice_kind=df_row['justice_kind'],
            name=df_row['name']
        )
        session.add(justice_kind)
    else:
        justice_kind.justice_kind = df_row['justice_kind']
        justice_kind.name = df_row['name']
    session.commit()


def put_cause_category(df_row):
    cause_category = session.query(CauseCategory).filter_by(category_code=df_row['category_code']).first()
    if not cause_category:
        cause_category = CauseCategory(
            category_code=df_row['category_code'],
            name=df_row['name']
        )
        session.add(cause_category)
    else:
        cause_category.category_code = df_row['category_code']
        cause_category.name = df_row['name']
    session.commit()


def put_instance(df_row):
    instance = session.query(Instance).filter_by(instance_code=df_row['instance_code']).first()
    if not instance:
        instance = Instance(
            instance_code=df_row['instance_code'],
            name=df_row['name']
        )
        session.add(instance)
    else:
        instance.instance_code = df_row['instance_code']
        instance.name = df_row['name']
    session.commit()


def put_region(df_row):
    region = session.query(Region).filter_by(region_code=df_row['region_code']).first()
    if not region:
        region = Region(
            region_code=df_row['region_code'],
            name=df_row['name']
        )
        session.add(region)
    else:
        region.region_code = df_row['region_code']
        region.name = df_row['name']
    session.commit()


def put_metadata_link(year, link):
    meta_data_source = session.query(MetadataLink).filter_by(link_year=year).first()
    if not meta_data_source:
        meta_data_source = MetadataLink(link_url=link, link_year=year)
        session.add(meta_data_source)
    else:
        meta_data_source.zip_binary = link
    session.commit()
    return meta_data_source


def put_metadata_zip_file(year, zip_bytes):
    session.query(MetadataZipFile).filter_by(zip_year=year).delete()
    bytes_split = list(split_bytes_to_chunks(zip_bytes, FILE_CH_SIZE))
    for num, chunk in enumerate(bytes_split):
        chunk = MetadataZipFile(zip_binary=chunk, zip_year=year, zip_chunk_num=num)
        session.add(chunk)
        session.commit()


def put_metadata_csv_file(year, name, csv_bytes):
    session.query(MetadataCsvFile).filter_by(csv_year=year, csv_name=name).delete()
    bytes_split = list(split_bytes_to_chunks(csv_bytes, FILE_CH_SIZE))
    for num, chunk in enumerate(bytes_split):
        chunk = MetadataCsvFile(csv_binary=chunk, csv_year=year, csv_name=name, csv_chunk_num=num)
        session.add(chunk)
        session.commit()


def put_stop_words(word):
    stop_word = session.query(StopWord).filter_by(word_text=word).first()
    if not stop_word:
        stop_word = StopWord(
            word_text=word
        )
        session.add(stop_word)
    else:
        stop_word.word_text = word
    session.commit()


# Support functions


def split_df(df, n):
    df_chunks = [df[i:i+n] for i in range(0, df.shape[0], n)]
    return df_chunks


def split_bytes_to_chunks(bytes_array, chunk_size):
    for i in range(0, len(bytes_array), chunk_size):
        yield bytes_array[i:i+chunk_size]


Session = sessionmaker(db)
session = Session()
database.metadata.create_all(db)


def close_sessions():
    session.close_all()
