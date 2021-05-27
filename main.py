from modules.database import models, updaters


if __name__ == '__main__':
    # updaters.update_metadata_links()
    # updaters.update_metadata_zip_files()
    # updaters.update_metadata_csv_files()

    years = [i[0] for i in models.read_meta_data_links()]

    # for year in years:
    #     updaters.update_structure(year)

    # for year in years:
    #     updaters.update_courts(year)

    # for year in years:
    #     updaters.update_documents(year)

    for year in ['2017', '2018', '2019', '2020', '2021']:
        updaters.update_documents(year)

    models.close_sessions()
    print("main file run successfully")
