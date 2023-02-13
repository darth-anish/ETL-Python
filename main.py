import pandas as pd
from sqlalchemy import create_engine
import os

def get_connection():
    from dotenv import load_dotenv
    load_dotenv()
    # get envrionmental variables
    user = os.getenv('DBUSER')
    host = os.getenv('DBHOST')
    password = os.getenv('DBPASSWORD')
    dbname = os.getenv('DBNAME')
    port = os.getenv('DBPORT')

    return create_engine(
        url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, dbname
        )
    )

def read_pickle_data(path):
    dataframe = pd.read_pickle(path)
    return dataframe
    
if __name__ == "__main__":
    import os
    
    print('Loading the transformed data to a Postgresql database')
    dataset_path = os.path.join(os.getcwd(),'datasets/transformed.pkl')
    # read pickle as pandas dataframe
    df = read_pickle_data(dataset_path)
    #create sql engine
    engine = get_connection()
    # load this df to postgresql database
    df.to_sql('SurveyInfo',engine)
    print('Load successful')
