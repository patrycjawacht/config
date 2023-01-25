import google.cloud.bigquery as gbq
import google.oauth2.credentials as goc

import subprocess as sps
import re
import sys
import os

import pandas as pd
import numpy as np 

import typing as tp
from sklearn.metrics import precision_recall_curve, roc_auc_score, auc
import logging

import datetime as dt
import yaml
import json

#####################

LOGGER = logging.getLogger(__name__)

python_path = os.path.join(
    os.path.dirname(os.path.abspath('__file__')),
    '../python'
)
sys.path.append(python_path)

#####################


def open_autofill_params(
    config_file: tp.Dict[str, tp.Union[list, tp.Dict]],
    params_file_name: str='fulfill_params.json',
)->tp.List[tp.Dict[str, tp.Any]]:
    """
    Load the json config_file or create one from the template if the file doesn't exist
    
    Arguments:
    ----------
        config_file: json file with fulfilled parameters to later on fetch to the word document
        params_file_name: yaml file with provided data and template 
        
    Returns:
    --------
        List of dictionaries with country and data
    """

    try:
        with open(f'{python_path}/{params_file_name}', 'r') as fp:
            autofull_params_file = json.load(fp)
    
    except IOError:
        print('File not found, will create a new one.')
        
        
        bq_client = get_bq_client(
            bq_location='location'
        )
        
        account_name = sps.check_output(
            f'gcloud config list account --format "value(core.account)"',
            shell=True
        ).decode().split('\n')
        
        account_name = account_name[0]
        
        date_last_updated = dt.datetime.strftime(
            dt.datetime.today(), '%d/%m/%Y'
        )
        
        autofull_params_file = []
    
        for country_data in config_file['run_list']:
            
            autocompl_config = config_file['fulfill_template'].copy()
            
            # model name
            model_name_sql = f'''
                SELECT 
                    distinct model 
                FROM 
                    dataset_name.table_{country_data['table_suffix']}
            '''
                
            model_name_tb = bq_client.query(
                query = model_name_sql
            ).to_dataframe()
            
            if len(model_name_tb)>1:
                logging.info("More than one model name. Chosen the first one")
            model_name = model_name_tb.model[0]
            
            autocompl_config['name'] = model_name
            autocompl_config['prepared_by'] = account_name
            autocompl_config['date_last_updated'] = date_last_updated
            
            autocompl_config['training_date'] = country_data['training_date']
            autocompl_config['version'] = country_data['version']
            autocompl_config['if_new'] = country_data['if_new']
            autocompl_config['country'] = country_data['country']
            autocompl_config['suffix_main'] = country_data['table_suffix']
            autocompl_config['cutoff'] = country_data['cutoff']
            
            for key, value in autocompl_config.items():
                if 'RUN_SUFFIX' in str(value):
                    autocompl_config[key] = str(value).replace(
                        "RUN_SUFFIX", 
                        country_data['table_suffix']
                    )
            
            autofull_params_file.append({
                'country': country_data['country'],
                'data': autocompl_config
            })
    
        with open(f'{python_path}/{params_file_name}', 'w') as fp:
            json.dump(autofull_params_file, fp)
            
    return autofull_params_file


def update_autofill_params(
    autofull_template_ls: tp.List[tp.Dict[str, tp.Any]],
    country: str,
    update_dict: tp.Dict[str, tp.Any],
    params_file_name: str='fulfill_params.json'
)->tp.List[tp.Dict[str, tp.Any]]:
    """
    Update the data for equivalent country stored inside the params_file_name json file. 
    The data to be updated are provided by update_dict dictionary with keys that match the 
    original template. If the key doesn't already exists then will be appended.
    
    Arguments:
    ----------
        autofull_template_ls: list of dicionaries with template for each country
        country: country for which the data will be updated
        update_dict: dicionary with key data to be updated
        params_file_name: the file to be updated
        
    Returns:
    --------
        autofull_template_ls
    """
    
    # if template already exists, update
    if any(country in country_data['country'] for country_data in autofull_template_ls):
    
        for country_data in autofull_template_ls:
            if country_data['country'] == country:
                country_data['data'].update(update_dict)
    
        with open(f'{python_path}/{params_file_name}', 'w') as wf:
            json.dump(autofull_template_ls, wf)
            
        logging.info(f"Updated the autofill_params for {country}")
        
    else:
        logging.info(f"{country} does not exist inside the autofill_params. Create refreshed autofill_params.")
        
    return autofull_template_ls


def get_bq_client(
    bq_location: str,
    bq_project: tp.Optional[str]=None,
    bq_access_token: tp.Optional[str]=None,
    env_bq_key: str='BQ_PROJECT',
    env_atoken_key: str='BQ_ACCESS_TOKEN'
)->gbq.Client:
    """
    Configure a client to accees big query. Things needed for configuration
    are access location, project and access token, for credentials. Location
    has to be passed, whereas other parts can be extracted from environmental
    variables or shell commands
    
    Arguments:
    ----------
        bq_location: location for the client 
        bq_project: project for the client
            if not passed (i.e. None), project will be taken from `BQ_PROJECT`
            environmental variable
        bq_access_token: access token for the client, if not passed (None), then it
            will be extracted from `BQ_ACCESS_TOKEN` environmental variable
        env_bq_key: name for the environmental variable that stores the project (see bq_project)
        env_atoken_key: name for the environmental variable that stores the access token (see bq_access_token)
        
    Returns:
    --------
        biguqery client
    """
    
    logging.basicConfig(level=logging.INFO)
    
    # attempt to locate project for the client
    if bq_project is None:
        logging.info('bq_project not passed')
        
        if env_bq_key not in os.environ:
            # extract project from the shell
            logging.info('Extracting bq_project from shell')
            
            conf_str = sps.check_output('gcloud config list', shell=True).decode()
            bq_project = re.findall(r"(?<=project = ).*", conf_str)[0]
        else:
            # project is set as environmental variable
            logging.info('Extracting bq_project from environment')
            bq_project = os.environ[env_bq_key]
            
    logging.info(f'bq_project={bq_project}')
            
    # attempt to locate big query token for the client
    if bq_access_token is None:
        logging.info('access token not passed')
        
        if env_atoken_key not in os.environ:
            # extract token from the shell
            logging.info('Extracting token from shell')
            bq_access_token = sps.check_output(
                'gcloud auth print-access-token', shell=True
            ).decode().replace('\n', '')
        else:
            # token is set as an environmental variable
            logging.info('Extracting token from environment')
            bq_access_token = os.environ[env_atoken_key]
    
    # create the client
    bq_client = gbq.Client(
        project=bq_project,
        location=bq_location,
        credentials=goc.Credentials(token=bq_access_token)
    )
    
    logging.info('Client created')
    
    return bq_client


def save_to_bq(
    df: pd.DataFrame,
    project_id: str,
    location: str,
    dest_dataset: str,
    dest_table_name: str,
)->None:
    """
    Save the dataframe to bigquery under f'{the dest_dataset}.{dest_table_name}'
    
    Arguments:
    ----------
        df: pandas dataframe
        project_id: project for the client 
        location: bigquery location for the client 
        dest_dataset: destination dataset_id
        dest_table_name: destination table_id 
        
    Returns:
    --------
        None
    """
    
    # write to bigquery
    df.to_gbq(
        project_id = project_id,
        location = location,
        destination_table=f"{dest_dataset}.{dest_table_name}"
#         if_exists = 'replace'
    )
    
    logging.info(f"Results written to {output_dataset}.{target_table_id}")