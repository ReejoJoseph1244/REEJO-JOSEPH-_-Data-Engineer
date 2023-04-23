import requests
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
import csv
import boto3
from botocore.exceptions import NoCredentialsError
import logging

'''
For Documentation run the command prompt :  python -m pydoc -b 
Then click the Asssessment file to see the documentation.

This Python Program is Created For the Recruitment Assessment Test for the
Data Engineer / Python Engineer Assessment role
at SteelEye

@author : Reejo Joseph
@College : LPU
'''

logging.basicConfig(filename="Logfile.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
 
# # Creating an object
logger = logging.getLogger()
 
# # Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

def download_and_unzip(url):
    '''
    The function download the zip file from the link passed as Argument and
    unzip the zip file.

    Parameter : url , This parameter is used to mention the url of the DLTINS zip file to download. 
    '''
    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall()
    logger.info('Successfully downloaded and unzipped the zip file')

def xmlParser():
    '''
    The function parse through to the first download link whose file_type is DLTINS and return the zip link
    '''
    try:
        tree = ET.parse('myfile.xml')
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
    except Exception as e:
        print(f"Error: {e}")
    root = tree.getroot()
    logger.info('Admin logged in')
    for doctag in root.findall('.//doc'):
            if doctag.find('str[@name="file_type"]').text == 'DLTINS':
                downloadable_link = doctag.find('str[@name="download_link"]').text
                return downloadable_link
    
    logger.info('Xml Parsed Successfully')

def csv_converter():
    '''
    The function converts the xml file to csv file
    '''
    tree = ET.parse('DLTINS_20210117_01of01.xml')
    root = tree.getroot()
    with open("DLTINS_20210117_01of01.csv", "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"])

        for instr in root.findall(".//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts"):
            writer.writerow([instr.findtext("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id"), instr.findtext("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm"), instr.findtext("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp"), instr.findtext("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd"), instr.findtext("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy"), instr.findtext("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr")])

    logger.info('Zip file Successfully converted to csv')

#Function to Upload the SV to the AWS S3 Bucket
access_key = 'your_access_key'
secret_key = 'your_secret_key'
s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
def AWS_lambda_function(event='', context=''):
    '''
    The function upload the CSV file into the AWS S3 bucket.
    '''
    try:
        bucket_name = "Name_of_Bucket"
        key = "DLTINS_20210117_01of01.csv"
        # Read the contents of the CSV file
        with open('DLTINS_20210117_01of01.csv', 'rb') as f:
            file_content = f.read()

        # Upload the CSV file to S3
        s3.put_object(Bucket=bucket_name, Key=key, Body=file_content)
        print(f"{key} uploaded successfully to S3 bucket {bucket_name}")
    except NoCredentialsError:
        print(f": Mention your AWS credentials to upload ")
    except Exception:
        logger.warning("This is a sample AWS lamda function for the  Assessment Test, Mention your AWS credentials")
        logger.info('Program Executed Successfully')



class Main:
    '''
    This is a main class which executes first!
    It has a contructor which downloads the !st xml file and calls below functions for the following :
    1. Xmlparser() : which parse through the xml and return the zip file link to download.
    2. download_and_unzip(link) : it downloads and unzip the zip file by the link provided by the above function link.
    3. csv_converter() : The function converts the xml file to csv file 
    4. AWS_lambda_function() : The function upload the CSV file into the AWS S3 bucket.
    '''
    def __init__(self):
        logger.info('Main class starts executing')
        url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
        response = requests.get(url)
        if response.status_code == 200:
            with open("myfile.xml", "wb") as f:
                f.write(response.content)
        
        link=xmlParser()
        download_and_unzip(link)
        csv_converter()
        AWS_lambda_function()
        

if __name__ == '__main__':
    main = Main()
