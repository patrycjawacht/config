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


def open_autodoc_params(
    config_file: tp.Dict[str, tp.Union[list, tp.Dict]],
    params_file_name: str='fulfill_params.json',
)->None:
    """
    
    """

    try:
        with open(f'{python_path}/{params_file_name}', 'r') as fp:
            autofull_params_file = json.load(fp)
    
    except IOError:
        print('File not found, will create a new one.')
        
#         bq_client = get_bq_client(
#             bq_location='location'
#         )
        
#         account_name = sps.check_output(
#             f'gcloud config list account --format "value(core.account)"',
#             shell=True
#         ).decode().split('\n')
        
#         account_name = account_name[0]
        
        date_last_updated = dt.datetime.strftime(
            dt.datetime.today(), '%d/%m/%Y'
        )
        
        autofull_params_file = []
    
        for country_data in config_file['run_list']:
            
            autocompl_config = config_file['fulfill_template'].copy()
            
#             # model name
#             model_name_sql = f'''
#                 SELECT 
#                     distinct model 
#                 FROM 
#                     dataset_name.table_{country_data['table_suffix']}
#             '''
                
#             model_name_tb = bq_client.query(
#                 query = model_name_sql
#             ).to_dataframe()
            
#             if len(model_name_tb)>1:
#                 logging.info("More than one model name. Chosen the first one")
#             model_name = model_name_tb.model[0]
            
#             autocompl_config['name'] = model_name
#             autocompl_config['prepared_by'] = account_name
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

def get_bq_client(
    project_id: str
)->gbq.Client:
    
    # hidden
    return None


def update_autodoc_params(
    autofull_template_ls: tp.List[tp.Dict[str, tp.Any]],
    country: str,
    update_dict: tp.Dict[str, tp.Any],
    params_file_name: str='fulfill_params.json'
)->None:
    """
    
    """
    
    if any(country in country_data['lob'] for country_data in autofull_template_ls):
    
        for country_data in autofull_template_ls:
            if country_data['country'] == country:
                country_data['data'].update(update_dict)
    
        with open(f'{python_path}/{params_file_name}', 'w') as wf:
            json.dump(autofull_template_ls, wf)
            
        logging.info(f"Updated the autofill_params for {country}")
        
    else:
        logging.info(f"{country} lob does not exist inside the autodoc_params. Create refreshed autofill_params.")
        
    return autofull_template_ls


def save_to_bq(
    df: pd.DataFrame,
    project_id: str,
    location: str,
    dest_dataset: str,
    dest_table_name: str,
)->None:
    """
    
    """
    
    # write to bigquery
    df.to_gbq(
        project_id = project_id,
        location = location,
        destination_table=f"{dest_dataset}.{dest_table_name}"
#         if_exists = 'replace'
    )
    
    logging.info(f"Results written to {output_dataset}.{target_table_id}")