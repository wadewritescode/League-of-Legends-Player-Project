import pandas as pd 
import numpy as np 
import logging 
import time 
import yaml
import os 
import datetime as dt
import sys

# CREATE CUSTOM EXCEPTIONS 
class PipelineError(Exception):
    pass 

class ApiError(PipelineError):
    pass 

class DataCompileError(PipelineError):
    pass

class DataLoadError(PipelineError):
    pass 

class DataValidationError(PipelineError):
    pass 

class DataTransformationError(PipelineError):
    pass 

class DataSaveError(PipelineError):
    pass 


### SET UP LOGGING DIRECTORY 
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def setup_logging(config):
    """READ IN THE LOGGING SETTINGS FROM CONFIG FILE"""
    log_config = config.get('logging')
    log_level = getattr(logging, log_config.get('level', 'INFO'))
    log_dir = os.path.join(SCRIPT_DIR, log_config.get('log_dir', 'INFO'))
    os.makedirs(log_dir, exist_ok=True)
    log_file = log_config.get('log_file')
    os.makedirs(os.path.dirname(log_dir), exist_ok=True)
    logging.basicConfig(
                        level=log_level,
                        format='%(asctime)s %(levelname)-9s %(message)s',
                        handlers = [logging.FileHandler(os.path.join(log_dir, log_file)), 
                                    logging.StreamHandler(sys.stdout)
                                    ])
    return logging.getLogger(__name__)

# LOAD IN THE CONFIG 
def load_config(config_path = None):
    """LOAD IN THE CONFIGURATION FOR THE PIPELINE BASED ON THE YAML FILE"""
    if config_path is None:
        config_path = os.path.join(SCRIPT_DIR,'config.yaml')
        
    if not os.path.exists(config_path):
        print(f"WARNING: Config file not found at {config_path}")
        return {}
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
    
# DEFINE THE RETRY SYSTEM 
def retry(max_retries = 3, delay = 2):
    """CREATE RETRY DECORATOR WITH EXPONENTIAL BACKOFF"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(__name__)
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f'JOB FAILED AFTER {max_retries} attempts : {e}')
                        raise 
                    wait = delay (2 ** (attempt - 1))
                    logger.warning(f"{attempt}/{max_retries} failed {e}... Retrying in {wait} seconds...")
                    time.sleep(wait)
        return wrapper 
    return decorator



### STEP ONE : LOAD DATA IN FROM CSV'S 
### NOTE : IN THE FUTURE, WE CAN MAKE STEP ONE THE API CALL TO GET THE DATA 
### HOWEVER DUE TO RATE LIMITS, WILL JUST PUT INTO A CSV FOR NOW. 

@retry(max_retries=3, delay = 2)
def load_data(file_path, logger):
    
    try:
        """LOAD ALL THE DATA INTO A DATAFRAME"""
        logger.info("=" * 30)
        logger.info('---STEP 1 : LOAD DATA---')
        logger.info("=" * 30)
        if not os.path.exists(file_path):
            raise DataLoadError(f"File directory not found at : {file_path}")
        
        logger.info(f"Reading files from {file_path}....")
        logger.info(f"Total Files in directory : {len([file for file in os.listdir(file_path)])}")
        logger.info(f"Beginning file merge")
        df = pd.concat([pd.read_csv(''.join(rf"{file_path}/{file}")) for file in os.listdir(file_path)])
        logger.info(f"File merge successful!")
        logger.info(f"Collated data has {df.shape[0]} rows and {df.shape[1]} columns")
        logger.info('---STEP 1 : COMPLETE---')
        
    except DataLoadError as e:
        logger.error(f'A data validation error occurred : {e}')
        raise
    except Exception:
        logging.exception("Unexpected Error has ocurred.")
        raise
    return df
        
        
def data_validation(df, config, logger):
    """VALIDATE EVERYTHING INCOMING AND ENSURE ITS COMPLETE AND VALID"""
    try:
        logger.info("=" * 30)
        logger.info('---STEP 2 : VALIDATE INCOMING DATA---')
        logger.info("=" * 30)
        cleaning = config.get('cleaning', {})
        required = config.get('required_columns', {})
        
        missing_cols = [col for col in required if col not in df.columns]
        if missing_cols:
            logger.error('Required data is missing from data asset')
            raise DataValidationError(f"Missing data : {missing_cols}")
        
        logger.info('Required data validated...')
        
        logger.info('Checking data quality...')
        null_count = df.isnull().sum().sum()
        duplicate_count = df.duplicated().sum()
        dtype_count = df.dtypes.value_counts().to_json()
        logger.info(f'data contains {null_count} nulls...')
        logger.info(f'data contains {duplicate_count} duplicates...')
        logger.info(f'data type counts...')
        logger.info(dtype_count)
    except DataValidationError as e:
        logger.error(f'A data validation error occurred : {e}')
        logging.exception("message")
        raise
    except Exception:
        logging.exception("message")
        raise
    
    return df
    
    
def data_cleaning(df, config, logger):
    """CLEAN THE INCOMING DATA"""
    logger.info("=" * 30)
    logger.info('---STEP 3 : CLEAN INCOMING DATA---')
    logger.info("=" * 30)
    initial_rows = len(df)
    
    try:
        cleaning = config.get('cleaning', {})
        min_game_time = cleaning.get('min_game_time', 1)
        
        # STEP 1 REMOVE EXACT DUPLICATES 
        logger.info('removing exact duplicates...')
        df = df.drop_duplicates()
        logger.info(f'removed {initial_rows - len(df)} duplicated rows...')
        
        # STEP 2 DROP ANY NULLS 
        row_count = len(df)
        logger.info('removing null values...')
        df = df.dropna(subset=['matchId', 'riotIdGameName', 'datapulldate'])
        logger.info(f'removed {row_count - len(df)} null rows...')
        
        # STEP 3 REMOVE ANY JUNK
        row_count = len(df)
        logger.info('Dropping unnamed first col...')
        df=df.iloc[::,1::]
        df = df[df['timePlayed']>= min_game_time]
        logger.info(f'Identified {row_count - len(df)} invalid games')
        
        if {row_count - len(df)} == 0:
            logger.info('Nothing to be removed')
        else:
            logger.info('Removed invalid games')
            
        # STEP 4 CLEAN THE REMAINING DATA 
        for col in [col for col in df.columns]:
            if df[col].dtype in ('str', 'object') and col != 'riotIdGameName' :
                df[col] = df[col].str.strip().str.title()
                df[col] = df[col].replace(['None', 'Nan', ''], np.nan)
                df[col] = df[col].fillna('Unknown')
                logger.info(f'String values for {col} have been standardised...')
        
            if  df[col].dtype == 'int':
                df[col] = df[col].astype('int64')
                df[col] = df[col].fillna(0)
                logger.info(f'Numeric values for {col} have been standardised...')
        logger.info('Columns have been cleaned...')

                
    except DataTransformationError as e:
        return logger.error(f'Error cleaning data : {e}')
    
    except Exception:
        logging.exception("Unexpected Error has ocurred.")
        raise
    return df
        
def enrich_data(df, logger):
    """ADD SOME ADDITIONAL METRICS TO THE DATAFRAME"""
    try :
    
        logger.info("=" * 30)
        logger.info(f'Calculating additional metrics...')
        logger.info("=" * 30)
        df['KDRatio'] = np.nan_to_num(np.divide(df['kills'], df['deaths']), nan=0)
        df['KDARatio'] = np.nan_to_num(np.divide(df['kills'] + df['assists'], df['deaths']), nan=0)
        df['WardsPerMinute'] = np.nan_to_num(np.divide(df['wardsPlaced'], df['timePlayed']), nan=0)
        logger.info(f'Additional metrics added...')
        
    except DataTransformationError as e:
        logger.error(f'A data validation error occurred : {e}')
        raise
    except Exception:
        logging.exception("Unexpected Error has ocurred.")
        raise
    return df
    
def generate_summary(df, logger):
    """GENERATE A SUMMARY OF EVERYTHING THAT HAS BEEN LOADED"""
    logger.info(f'Loading summary of games processed...')
    logger.info(f'Processed a total of {df['matchId'].nunique()} games...')      
    return df
    

def save_output(df, config, logger): 
    try :
        """SAVING THE COLLATED DATA INTO ONE LOCATION"""
        output_directory = os.path.join(SCRIPT_DIR, config.get('data', {}).get('output_dir', 'output'))
        os.makedirs(output_directory, exist_ok=True)
        time = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(output_directory, f'cleaned_league_game_data_{time}.csv')
        df.to_csv(output_file, index=False)
        logger.info(f'Saved output file to {output_file}')
        logger.info(f'File contains {len(df)} rows...')
    except DataSaveError as e:
        logger.error(f'An error occurred saving the data: {e}')
    except Exception:
        logging.exception("Unexpected Error has ocurred.")
    
    return df
    

### MAIN PIPELINE

def run_pipeline():
    configuration = load_config()
    logger = setup_logging(configuration)

    start_time = time.time()
    start_time_local = time.strftime('%y-%m-%d %H:%S', time.localtime())
    logger.info(f'Pipeline initiatied at {start_time_local}')
    logger.info("=" * 30)
    logger.info("Starting data pipeline...")
    logger.info("=" * 30)

    try:
        data_config = configuration.get('data', {})
        input_dir = data_config.get('input_dir', 'data')
        filepath = os.path.join(SCRIPT_DIR, input_dir)
        
        df=load_data(filepath, logger)
        df=data_validation(df, data_config, logger)
        df=data_cleaning(df, data_config, logger)
        df=enrich_data(df, logger)
        df=generate_summary(df, logger)
        df=save_output(df,data_config, logger)
    except PipelineError as e:
        logger.error(f'Error {e}')
        
    end_time = time.time()
    end_time_local = time.strftime('%Y-%m-%d %H:%S', time.localtime())
    logger.info(f'Pipeline completed at {end_time_local}')
    logger.info(f'Total job time {(end_time - start_time):.2f} seconds')
    return df
    
if __name__ == "__main__":
    run_pipeline()