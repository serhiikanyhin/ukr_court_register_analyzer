from modules.database import models, models_graph, updaters


if __name__ == '__main__':
    updaters.update_metadata_links()

    for year, link in models.read_meta_data_links():
        updaters.update_metadata_zip_files(year, link)

    years = [i[0] for i in models.read_meta_data_links()]

    for year in years:
        updaters.update_metadata_csv_files(year)

    for year in years:
        updaters.update_structure(year)

    for year in ['2017', '2018', '2019', '2020', '2021']:
        updaters.update_documents(year)

    updaters.update_stop_words()

    for year in ['2017', '2018', '2019', '2020', '2021']:
        updaters.update_doc_words(year)

    updaters.update_vocabulary()

    updaters.update_graph()

    models.close_sessions()
    models_graph.driver.close()
    print("main file run successfully")
