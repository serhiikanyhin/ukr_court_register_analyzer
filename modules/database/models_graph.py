from neo4j import GraphDatabase
import time


driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "xxxx"))
session = driver.session()
ADD_CH_SIZE = 1000


def create_paragraphs_multiple(paragraphs_list):

    def create_paragraphs(tx):
        chunks = [paragraphs_list[i:i + ADD_CH_SIZE] for i in range(0, len(paragraphs_list), ADD_CH_SIZE)]
        print("Number of paragraph chunks %s" % (len(chunks),))
        for num, chunk in enumerate(chunks):
            nodes = ["(:PARAGRAPH {doc_id: %s, par_num: %s, words_num: %s})"
                     % (i[0], i[1], i[2]) for i in chunk]
            query = "CREATE %s" % (", ".join(nodes),)
            tx.run(query)
            print("Paragraph chunk %s updated" % (num+1,))

    session.write_transaction(create_paragraphs)


def create_words_multiple(words_list):

    def create_words(tx):
        chunks = [words_list[i:i + ADD_CH_SIZE] for i in range(0, len(words_list), ADD_CH_SIZE)]
        print("Number of word chunks %s" % (len(chunks),))
        for num, chunk in enumerate(chunks):
            nodes = ["(:WORD {word_id: %s, word_text: '%s'})"
                     % (i[0], i[1]) for i in chunk]
            query = "CREATE %s" % (", ".join(nodes),)
            tx.run(query)
            print("Word chunk %s updated" % (num + 1,))

    session.write_transaction(create_words)


def set_par_2_word_relations(chunk):

    batch = [{"d": i[0], "p": i[1], "w": i[2]} for i in chunk]

    def add_batch(tx):
        query = 'WITH $batch as batch ' \
                'UNWIND batch as BATCH ' \
                'MATCH (A:WORD {word_id:BATCH.w}), (C:PARAGRAPH {doc_id:BATCH.d, par_num:BATCH.p}) ' \
                'CREATE (C) - [R:RELATION] -> (A)'
        tx.run(query, batch=batch)

    with driver.session() as dr_session:
        dr_session.write_transaction(add_batch)


def set_par_2_par_relations(chunk, sim_cof, min_word_num):

    batch = [{"d": i[0], "p": i[1]} for i in chunk]

    def add_batch(tx):
        query = 'WITH $batch as batch ' \
                'UNWIND batch as BATCH ' \
                'MATCH (A:PARAGRAPH {doc_id:BATCH.d, par_num:BATCH.p}) - [R1:RELATION] ' \
                '-> (W:WORD) <- [R2:RELATION] - (B:PARAGRAPH) ' \
                'WHERE A.words_num > %s AND B.words_num > %s ' \
                'WITH A, B, ' \
                'COUNT(DISTINCT (W)) / A.words_num AS cof_a, ' \
                'COUNT(DISTINCT (W)) / B.words_num AS cof_b ' \
                'WHERE cof_a > %s OR cof_b > %s '\
                'MERGE (A) - [C:CONNECTION] -> (B)' \
                % (min_word_num, min_word_num, sim_cof, sim_cof)
        tx.run(query, batch=batch)

    with driver.session() as dr_session:
        dr_session.write_transaction(add_batch)


def get_connected_paragraphs(min_par_num):
    def get_conn_pars(tx):
        creation_query = "CALL gds.graph.create('connection-graph', 'PARAGRAPH', 'CONNECTION');"
        get_query = "CALL gds.wcc.stream('connection-graph') " \
                    "YIELD nodeId, componentId " \
                    "WITH " \
                    "componentId, " \
                    "COUNT(nodeId) AS node_count, " \
                    "collect(gds.util.asNode(nodeId).doc_id) AS doc_ids, " \
                    "collect(gds.util.asNode(nodeId).par_num) AS doc_par_nums " \
                    "WHERE node_count > $min_par_num " \
                    "RETURN doc_ids, doc_par_nums;"
        deletion_query = "CALL gds.graph.drop('connection-graph');"

        tx.run(creation_query)
        result = tx.run(get_query, min_par_num=min_par_num)
        tx.run(deletion_query)
        return result.values()

    with driver.session() as dr_session:
        conn_pars = dr_session.write_transaction(get_conn_pars)
        return conn_pars
