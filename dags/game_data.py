from airflow import DAG, operators
import datetime as dt 
import sys 
import os 
import yaml
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ADD PIPELINE DIRECTORY TO A PATH SO WE CAN IMPORT OUR CODE 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), r'..')))
import game_data_pipeline as gdp

def check_for_files(**kwargs): 
    
    """CHECK TO ENSURE THERE ARE FILES IN THE FILE DIRECTORY"""
    try:
        file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        
            
    except gdp.DataLoadError as e:
        print(e)
        raise e 
    except Exception as e:
        print(e)
        raise e 
    
    else:
        print(f"Files have been found")
        print(f"Total Files in directory : {len([file for file in os.listdir(file_path)])}")
        return os.path.getsize(file_path)


def run_gdp_pipeline(**kwargs):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), r'..')))
    import game_data_pipeline as gdp
    """RUNNING THE PIPELINE FROM AIRFLOW"""
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.yaml'))
    
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config_path = yaml.safe_load(f)
    else:
        config_path = {}
        
    df = gdp.run_pipeline()
    return df.shape[0]
    

def generate_report(**kwargs):
    print("=" * 50)
    print("AIRFLOW REPORT")
    print("=" * 50)
    print(f"Run date : {dt.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}")
    print(f"Result : Success")
    print(f"DAG : League Files Collated")
    
    
default_args = {
    "owner" : "data engineer", 
    "depends_on_past" : False, 
    "email_on_failure" : False, 
    "email_on_retry" : False, 
    "retries" : 1, 
    "retry_delay" : dt.timedelta(minutes=5)
    }

with DAG(
    'game_data_pipeline', 
    default_args = default_args, 
    description = 'Collating League of Legends Game Data', 
    schedule='@daily', 
    start_date=dt.datetime(2026, 11, 14), 
    catchup=False, 
    tags=['league data cleaning', 'collate game data', 'processing game data']
) as dag:
    
    check_for_data = PythonOperator(
        task_id = 'check_data_file', 
        python_callable = check_for_files
    )
    
    run_cleaning_pipeline = PythonOperator(
        task_id = 'clean_data_files', 
        python_callable = run_gdp_pipeline
        
    )
    summarise = PythonOperator(
        task_id = 'summarise_data_files', 
        python_callable = generate_report
        
    )
    
    check_for_data >> run_cleaning_pipeline >> summarise