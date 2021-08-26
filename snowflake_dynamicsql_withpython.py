#PreRequisties --> [Install Python -> open cmd prompt] pip install --upgrade snowflake-sqlalchemy
#Below are some optional commented out and may be reuired for file operations , DataScience
#import os
#from sqlalchemy import create_engine
#from snowflake.sqlalchemy import URL
#######################################################################
### Usage   : This Script Create list of Tables with column names predefined as per input files <src/files.txt>
###           creates file format (csv) , Internal stage table , Target Table (if not exist) , Loads the Stage (put) and Target Table(copy into)
### Author  : Sachin
### Version : Initial , Python Version: 3.9.6, Snowflake Year : Aug 2021
######################################################################
import pandas as pd
import sqlalchemy
import snowflake.connector
#Please replace the credential as per your snowflake account , Note: Connection issues could be because of Case sensitive
# or incorrect account name  in case account hosted on aws below is an example
conn = snowflake.connector.connect(
    user='username_casesensitive',
    password='Password_casesensitive',
    account='alphanumeric.ap-south-1.aws',
    database='MYDB',
    schema='MYDB_SCHEMA'
)

def stage_load_elt():
    df1=pd.read_csv("C:\\srcfiles\\fileslist.txt", sep =";")
    cs = conn.cursor()
    file_dir= 'C:\\srcfiles\\files'
    fileformat= 'csv'

    # Create CSV File format , Note the filed_delimiter has to be as per file , recheck in case of data not available finally
    print ('*** File Format : Create ' + fileformat + " file format mycsvformat")
    cs.execute("create or replace file format mycsvformat type = " + fileformat + " field_delimiter = ',' skip_header = 1")

    # Create Internal Stage and Load the data into Table from files
    for i,columns in df1.iterrows():
        print ('Executing ...')
        filename = columns[0].upper()
        filepath = file_dir+'\\'+filename

        print ('*** Stage Creation : Create Internal Stage:',  filename + "_INT_STAGE" )
        cs.execute("create or replace stage " + filename + '_INT_STAGE' + " file_format = mycsvformat")

        print ('*** Table Creation : Create Table :',  filename  )
        cs.execute("CREATE TABLE " + filename + ' (' + columns[1] + ' )'+ " IF NOT EXISTS" )

        print ('*** Stage Loading (put) : Create Internal Stage:',  filename + "_INT_STAGE" )
        print("put file://" + filepath + '.' + fileformat + '  @' + filename + "_INT_STAGE" + " auto_compress=true" )
        cs.execute("put file://" + filepath + '.' + fileformat + '  @' + filename + "_INT_STAGE" + " auto_compress=true" )

        print ('*** Table Load (Copy INTO) : ',columns[0] + ' from ' + columns[0] + "_INT_STAGE" )
        cs.execute("copy into "+ filename + " from @" +   filename+'_INT_STAGE/' + filename+'.csv.gz ' + " file_format = (format_name = mycsvformat error_on_column_count_mismatch=false)  " )

    conn.close()

stage_load_elt()
print ("*** Script execution completed *** ")