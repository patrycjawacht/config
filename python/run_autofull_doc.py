import sys
import os
import yaml
import typing as tp
import logging
import google.cloud.storage as gcs
from cast.autofull.document import WordDocxxx #hidden

PYTHON_PATH = os.path.join(
    os.path.dirname(os.path.abspath('__file__')),
    '../python'
)

BASH_PATH = os.path.join(
    os.path.dirname(os.path.abspath('__file__')),
    '../bash'
)
sys.path.append(PYTHON_PATH)
sys.path.append(BASH_PATH)


def initialize_gcs(
    project_id: str
)->gcs.Client:
    """
    Initialize google cloud storage client
    
    Arguments:
    ---------
        project_id: project to initialize gcs client
        
    Returns:
    -------
        gcs Client
    """
    
    gcs_client = gcs.Client(
        project = project_id
    )
    
    return gcs_client


def upload_to_gcs(
    gcs_client: gcs.Client, 
    gcs_bucket: str, 
    gcs_folder_path: str,
    filenames: tp.List[str],
)->None:
    """
    Append the filenames to gcs bucket
    
    Arguments:
    ----------
        gcs_client: initialized gcs client
        gcs_bucket: gcs bucket to where save the files
        gcs_folder_path: gcs bucket path structure to append the files
        filenames: list of filenames to be appended to the gcs bucket
        
    Returns:
    --------
        None
    """
    
    # append file to gcs bucket
    for filename in filenames:
        
        bucket = gcs_client.bucket(gcs_bucket)
        dest_blob = bucket.blob(f'{gcs_folder_path}/{filename}')
        dest_blob.upload_from_filename(f'{filename}')  
        
        logging.info(f'{filename} uploaded to GCS bucket')

        
def upload_input_data(
    input_data_filename: str
)->tp.List[tp.Dict[str, tp.Any]]:
    """
    Open the input_data_filename yaml file
    
    Arguments:
    ----------
        input_data_filename: filename containing list of country and its data parameters 
        to fulfill the word document with
        
    Returns:
    --------
        input_data: lists of dicionaries with country and its data 
    """
    
    # load the yaml file with input parameters for autofill
    with open(f'{PYTHON_PATH}/{input_data_filename}') as rf:
        input_data = yaml.safe_load(rf)
    
    return input_data


def save_output_data(
    gcs_client: gcs.Client,
    input_data_dict: tp.Dict[str, tp.Any],
    doc_template_file: tp.Optional[str]='for_test.docx'
)->None:
    """
    Render the data parameters and fulfill the word document
    
    Arguments:
    ----------
        gcs_client:
        input_data_dict:
        doc_template_file:
        
    Returns:
    --------
        None
    """
    filenames=[]

    # for each country, rendering context/data into template
    for country_data in input_data_dict:
        
        doc = WordDocument(f'{PYTHON_PATH}/{doc_template_file}')
        
        filename = f"{PYTHON_PATH}/file_to_fill_{country_data['country']}.docx"
        
        doc.render(country_data['data'])
        doc.save(filename)
        
        logging.info(f"Document for {country_data['country']} updated")
        
        filenames.append(filename)
    
    # upload to GCS bucket
    upload_to_gcs(
        gcs_client = gcs_client,
        gcs_bucket = 'xxxx',
        gcs_folder_path = 'path/to/gcs/',
        filenames = filenames
    )
        
    
if __name__ == '__main__':
    """
    Fulfill the word document with provided parameters
    """
    
    FORMAT = '%(asctime)s %(message)s'
    logging.basicConfig(
        level=logging.DEBUG, 
        format=FORMAT
    )
    
    # initliazie gcs
    gcs_client = initialize_gcs(
        project_id = 'xxx'
    )
    
    # get the input data
    input_data = upload_input_data(
        input_data_filename = 'fulfill_params.json'
    ) 
    
    # save created files to gcs bucket
    save_output_data(
        input_data_dict = input_data,
        gcs_client = gcs_client
    )