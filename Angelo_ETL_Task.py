#!/usr/bin/env python3.6.2
"""Angelo_ETL_Task.py"""
__author__      = "Angelo Bravo"

import pandas as pd
import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras
import requests
import datetime
import warnings
import time
from io import BytesIO
from zipfile import ZipFile



#-----------------EXTRACTION HELPER METHODS----------------------------------------------------------
#get csv file from zipfile response and convert dataframe
#input is response, and csv filename
def file_to_dataframe(response, filename):

    zipfile = ZipFile(BytesIO(response.content))
    print('File Extracted: ', filename)

    return pd.read_csv(zipfile.open(filename), low_memory=False)


#-----------------TRANSFORMATION HELPER METHODS----------------------------------------------------------
def get_c_full_name(loinc_code, loinc_path_dict, loinc_codetext_dict):
    #get code list for loinc_id
    code_list = loinc_path_dict[loinc_code].split('.')
    #add loinc_code to path
    code_list.append(loinc_code)

    #get name for each code
    name_list = []
    for code in code_list:
        #for each code only append its name to list of names)
        name_list.append(loinc_codetext_dict[loinc_code])


    full_name = '\\i2b2\\Laboratory'
    #combine list of codes and names and concatenate results
    for code_name in list(zip(code_list, name_list)):
        full_name += '\\(' + code_name[0] + ') ' + code_name[1]

    return full_name

def get_c_name(loinc_code, loinc_codetext_dict, loinc_concat_dict):
    c_name = ''
    if loinc_code.startswith("LP"):
        c_name = loinc_codetext_dict[loinc_code]
    else:
        c_name = loinc_concat_dict[loinc_code]

    return c_name


def get_c_visualattributes(loinc_code, parent_count_dict, loinc_status_dict):

    char_list = [None] * 3

    #check if code is a parent
    if loinc_code in parent_count_dict:
        #if it's a parent more than once, it is a multiple folder
        if parent_count_dict[loinc_code] > 1:
            char_list[0] = 'M'

        else:
            char_list[0] = 'F'
        #if it is a folder, it is editable
        char_list[2] = 'E'
    #if code is not a parent, it is a leaf (cannot be a root since all loinc_codes are children to \i2b2\Laboratory)
    else:
        char_list[0] = 'L'

    #check if code is active
    if loinc_status_dict[loinc_code] == 'ACTIVE':
        char_list[1] = 'A'
    else:
        char_list[1] = 'I'

    #collapse list to string
    c_visualattribute = ''.join(list(filter(None, char_list)))

    return c_visualattribute

def get_c_metadataxml(loinc_df, loinc_codetext_dict):

    #list to store xml trees
    c_metadataxml_list = []

    #for ever code in loinc dataframe
    for i in range(len(loinc_df)):
        #if code starts with LP build XML tree for code with metadata from hierarchy table
        if loinc_df['LOINC_NUM'].iloc[i].startswith("LP"):
            multiaxialhierarchy = ET.Element('MultiAxialHierarchy')
            loinc_num = ET.SubElement(multiaxialhierarchy, 'LOINC_NUM')
            loinc_num.text = loinc_df['LOINC_NUM'].iloc[i]
            code_text = ET.SubElement(multiaxialhierarchy, 'CODE_TEXT')
            code_text.text = loinc_codetext_dict[loinc_df['LOINC_NUM'].iloc[i]]
            #append xml to list
            c_metadataxml_list.append(ET.tostring(multiaxialhierarchy, encoding='unicode'))
        #else build xml tree with metadata from loinc table
        else:
            Loinc = ET.Element('Loinc')
            loinc_num = ET.SubElement(Loinc, 'LOINC_NUM')
            loinc_num.text = loinc_df['LOINC_NUM'].iloc[i]
            component= ET.SubElement(Loinc, 'COMPONENT')
            component.text = str(loinc_df['COMPONENT'].iloc[i])
            system = ET.SubElement(Loinc, 'SYSTEM')
            system.text = str(loinc_df['SYSTEM'].iloc[i])
            method_typ = ET.SubElement(Loinc, 'METHOD_TYP')
            method_typ.text = str(loinc_df['METHOD_TYP'].iloc[i])
            #append xml to list
            c_metadataxml_list.append(ET.tostring(Loinc, encoding='unicode'))


    return c_metadataxml_list

#due to some strings exceeding varchar limit, we will take a substring of the maximum allowed of varchars
def varchar_len(value, fixed_n):
    if len(value) > fixed_n:
        return value[:fixed_n]
    else:
        return value

#--------------------------MAIN EXTRACT, TRANSFORM, LOAD FUNCTIONS-----------------------------------------------------
#input is loinc.org username and password used to extract data
def extract(loinc_username, loinc_password):

    print("EXTRACTING DATA...............")
    #extract data from http post requests
    with requests.Session() as s:

        #session login to make authorized requests
        p = s.post('https://loinc.org/wp-login.php', data={'log' : loinc_username,  'pwd' : loinc_password})

        loinc_csv_resp = s.post('https://loinc.org/download/loinc-table-file-csv/', data = {'tc_submit' : 'Download', 'tc_accepted' : 1})
        hierarchy_csv_resp = s.post('https://loinc.org/download/loinc-multiaxial-hierarchy/', data = {'tc_submit' : 'Download', 'tc_accepted' : 1})

    #convert responses into data frames
    loinc_df = file_to_dataframe(loinc_csv_resp, 'Loinc.csv')
    hierarchy_df = file_to_dataframe(hierarchy_csv_resp, 'MultiAxialHierarchy.csv')

    return [loinc_df, hierarchy_df]


def transform(extracted_dataframes):

    print("TRANSFORMING DATA...............")

    #unpack dataframes
    loinc_df, hierarchy_df = extracted_dataframes

    # left join loinc dataframe with hierarchy dataframe on loinc_num
    merged_df = loinc_df.merge(hierarchy_df, 'left', left_on='LOINC_NUM', right_on='CODE')

    ###create dictionaries for O(1) retrieval of values of interest mapped to LOINC_NUM

    #create dictionary of code:path_to_root
    loinc_path_dict = pd.Series(merged_df.PATH_TO_ROOT.values, index = merged_df.LOINC_NUM).to_dict()

    #create dictionary of code:code_text
    loinc_codetext_dict = pd.Series(hierarchy_df.CODE_TEXT.values, index = hierarchy_df.CODE).to_dict()

    #create dictionary of  loinc_num:concat(['COMPONENT', 'PROPERTY', 'TIME_ASPCT', 'SYSTEM', 'SCALE_TYP', 'METHOD_TYP'])
    loinc_concat_dict = pd.Series(merged_df[['COMPONENT', 'PROPERTY', 'TIME_ASPCT', 'SYSTEM', 'SCALE_TYP', 'METHOD_TYP']].astype(str).
                                  apply(':'.join, axis=1).values, index = merged_df.LOINC_NUM).to_dict()

    #create dictionary with key as immediate parent and count as frequency of parent code to check if code is a folder, multiple folder, or leaf
    parent_count_dict = dict(hierarchy_df['IMMEDIATE_PARENT'].value_counts())

    # create dictionary of code:status
    loinc_status_dict = pd.Series(loinc_df.STATUS.values, index=loinc_df.LOINC_NUM).to_dict()

    #get unique loinc codes
    loinc_codes = merged_df.LOINC_NUM.unique()

    # get current datetime
    now = datetime.datetime.now()
    dt_string = now.strftime("%d-%m-%Y %H:%M:%S")

    #freeing up memory
    del merged_df, hierarchy_df

    #create empty dataframe for transformed columns
    output_df = pd.DataFrame({'C_HLEVEL' : []})

    #the levels in each path to root are seperated by periods, count them plus 1 to get the distance to root, and add 1 for fixed '\i2b2\Laboratory’ and add 1 for loinc code
    output_df['C_HLEVEL'] = list(map(lambda code: loinc_path_dict[code].count('.') + 3, loinc_codes))

    #map helper functions to each loinc_code
    output_df['C_FULLNAME'] = list(map(lambda code: get_c_full_name(code, loinc_path_dict, loinc_codetext_dict), loinc_codes))

    output_df['C_NAME'] = list(map(lambda code: get_c_name(code, loinc_codetext_dict, loinc_concat_dict), loinc_codes))

    output_df['C_SYNONYM_CD'] = 'N'

    output_df['C_VISUALATTRIBUTES'] = list(map(lambda code: get_c_visualattributes(code, parent_count_dict, loinc_status_dict), loinc_codes))

    output_df['C_TOTALNUM'] = None #null

    output_df['C_BASECODE'] = list(map(lambda code: 'LOINC:' + code, loinc_codes))

    output_df['C_METADATAXML'] = get_c_metadataxml(loinc_df, loinc_codetext_dict)

    output_df['C_FACTTABLECOLUMN'] = 'CONCEPT_CD'

    output_df['C_TABLENAME'] = 'CONCEPT_DIMENSION'

    output_df['C_COLUMNNAME'] = 'CONCEPT_PATH'

    output_df['C_COLUMNDATATYPE'] = ['N' if scale_typ == 'Qn' else 'T' for scale_typ in loinc_df['SCALE_TYP']]

    output_df['C_OPERATOR'] = 'LIKE'

    output_df['C_DIMCODE'] = output_df['C_FULLNAME']

    output_df['C_COMMENT'] = None #null

    output_df['C_TOOLTIP'] = output_df['C_FULLNAME']

    output_df['UPDATE_DATE'] =  dt_string

    output_df['DOWNLOAD_DATE'] = dt_string

    output_df['IMPORT_DATE'] = dt_string #will change this in load function if table is not yet created

    output_df['SOURCESYSTEM_CD'] = 'LOINC'

    output_df['VALUETYPE_CD'] = 'LAB'

    output_df['M_APPLIED_PATH'] = '@'

    output_df['M_EXCLUSION_CD'] = None #null

    output_df['C_PATH'] = list(map(lambda path: path[:path.rfind('\\')], output_df['C_FULLNAME'])) #get substring from 0 to index of last \

    output_df['C_SYMBOL'] = list(map(lambda code: loinc_codetext_dict[code], loinc_codes))

    warnings.filterwarnings("ignore", category=FutureWarning)
    #filter out rows which do not abide by null constraints of i2b2 table
    output_df = output_df[
        (output_df['C_HLEVEL'].notnull()) & (output_df['C_HLEVEL'] != '') &
        (output_df['C_FULLNAME'].notnull()) & (output_df['C_FULLNAME'] != '') &
        (output_df['C_NAME'].notnull()) & (output_df['C_NAME'] != '') &
        (output_df['C_SYNONYM_CD'].notnull()) & (output_df['C_SYNONYM_CD'] != '') &
        (output_df['C_VISUALATTRIBUTES'].notnull()) & (output_df['C_VISUALATTRIBUTES'] != '') &
        (output_df['C_FACTTABLECOLUMN'].notnull()) & (output_df['C_FACTTABLECOLUMN'] != '') &
        (output_df['C_TABLENAME'].notnull()) & (output_df['C_TABLENAME'] != '') &
        (output_df['C_COLUMNNAME'].notnull()) & (output_df['C_COLUMNNAME'] != '') &
        (output_df['C_COLUMNDATATYPE'].notnull()) & (output_df['C_COLUMNDATATYPE'] != '') &
        (output_df['C_OPERATOR'].notnull()) & (output_df['C_OPERATOR'] != '') &
        (output_df['C_DIMCODE'].notnull()) & (output_df['C_DIMCODE'] != '') &
        (output_df['M_APPLIED_PATH'].notnull()) & (output_df['M_APPLIED_PATH'] != '') &
        (output_df['UPDATE_DATE'].notnull()) & (output_df['UPDATE_DATE'] != '')]

    #apply function to non-fixed varchar columns to make sure each varchar column does not exceed varchar limit
    output_df['C_FULLNAME'] = output_df['C_FULLNAME'].apply(lambda value: varchar_len(value, 700))
    output_df['C_NAME'] = output_df['C_NAME'].apply(lambda value: varchar_len(value, 2000))
    output_df['C_BASECODE'] = output_df['C_BASECODE'].apply(lambda value: varchar_len(value, 50))
    output_df['C_FACTTABLECOLUMN'] = output_df['C_FACTTABLECOLUMN'].apply(lambda value: varchar_len(value, 50))
    output_df['C_TABLENAME'] = output_df['C_TABLENAME'].apply(lambda value: varchar_len(value, 50))
    output_df['C_COLUMNNAME'] = output_df['C_COLUMNNAME'].apply(lambda value: varchar_len(value, 50))
    output_df['C_COLUMNDATATYPE'] = output_df['C_COLUMNDATATYPE'].apply(lambda value: varchar_len(value, 50))
    output_df['C_OPERATOR'] = output_df['C_OPERATOR'].apply(lambda value: varchar_len(value, 10))
    output_df['C_DIMCODE'] = output_df['C_DIMCODE'].apply(lambda value: varchar_len(value, 700))
    output_df['C_TOOLTIP'] = output_df['C_TOOLTIP'].apply(lambda value: varchar_len(value, 900))
    output_df['C_PATH'] = output_df['C_PATH'].apply(lambda value: varchar_len(value, 700))
    output_df['C_SYMBOL'] = output_df['C_SYMBOL'].apply(lambda value: varchar_len(value, 50))

    return output_df

def load(pg_host, pg_port, pg_db, pg_user, pg_password, df_to_load):
    """ Connect to the PostgreSQL database server """
    try:

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = conn = psycopg2.connect(
            host = pg_host,
            port = pg_port,
            database = pg_db,
            user = pg_user,
            password = pg_password
        )


        # create a cursor
        cur = conn.cursor()
        #check if Loinc table exist
        cur.execute("SELECT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=%s)", ('i2b2',))

        #if table does not exist, create table
        if not cur.fetchone()[0]:
            cur.execute("""CREATE TABLE I2B2 
            ( C_HLEVEL INT NOT NULL,
            C_FULLNAME VARCHAR(700) NOT NULL,
            C_NAME VARCHAR(2000) NOT NULL,
            C_SYNONYM_CD CHAR(1) NOT NULL,
            C_VISUALATTRIBUTES CHAR(3) NOT NULL,
            C_TOTALNUM INT NULL,
            C_BASECODE VARCHAR(50) NULL,
            C_METADATAXML TEXT NULL,
            C_FACTTABLECOLUMN VARCHAR(50) NOT NULL,
            C_TABLENAME VARCHAR(50) NOT NULL,
            C_COLUMNNAME VARCHAR(50) NOT NULL,
            C_COLUMNDATATYPE VARCHAR(50) NOT NULL,
            C_OPERATOR VARCHAR(10) NOT NULL,
            C_DIMCODE VARCHAR(700) NOT NULL,
            C_COMMENT TEXT NULL,
            C_TOOLTIP VARCHAR(900) NULL,
            M_APPLIED_PATH VARCHAR(700) NOT NULL,
            UPDATE_DATE timestamp NOT NULL,
            DOWNLOAD_DATE timestamp NULL,
            IMPORT_DATE timestamp NULL,
            SOURCESYSTEM_CD VARCHAR(50) NULL,
            VALUETYPE_CD VARCHAR(50) NULL,
            M_EXCLUSION_CD VARCHAR(25) NULL,
            C_PATH VARCHAR(700) NULL,
            C_SYMBOL VARCHAR(50) NULL
            ) ;
            """)

            print("CREATED TABLE: I2B2")

        #else if table already exists, get min(import_date) as import_date
        else:
            cur.execute("SELECT MIN(IMPORT_DATE) FROM I2B2")
            import_date = cur.fetchone()[0]
            #if there is an import_date record, replace import_date in dataframe with earliest import_date in i2b2 table
            if import_date:
                df_to_load['IMPORT_DATE'] = import_date


        ###LOADING DATA
        df_to_load_columns = list(df_to_load)
        # create (col1,col2,...)
        columns = ",".join(df_to_load_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for col in df_to_load_columns]))

        # create INSERT INTO table (columns) VALUES('%s',...)
        insert_statement = "INSERT INTO {} ({}) {}".format('i2b2', columns, values)

        print("LOADING DATA...............")
        psycopg2.extras.execute_batch(cur, insert_statement, df_to_load.values)

        #print count of rows inserted
        update_date = df_to_load['UPDATE_DATE'][0]
        cur.execute('SELECT COUNT(*) FROM I2B2 WHERE UPDATE_DATE = \'%s\'' % update_date)
        print(cur.fetchone()[0], 'ROWS INSERTED')

        #output imported rows to csv file
        inserted_rows = 'COPY (SELECT * FROM I2B2 WHERE UPDATE_DATE = \'%s\') TO STDOUT WITH CSV HEADER' % update_date
        with open('i2b2_inserted_rows_%s.csv' % update_date[:10], 'w') as file:
            cur.copy_expert(inserted_rows,file)

        #print name of output csv file
        print('CSV File of Rows Loaded Generated: i2b2_inserted_rows_%s.csv' % update_date[:10])

        # close the communication with the PostgreSQL
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    start_time = time.clock()

    #load_data()
    extracted_dataframes = extract('aebravo','aaaa1234') #input your loinc.org username, password
    load_dataframe = transform(extracted_dataframes)
    load(pg_host="localhost", #input your postgres database connection configuration
         pg_port="5432",
         pg_db="postgres",
         pg_user="postgres",
         pg_password="postgres",
         df_to_load=load_dataframe)

    print('Execution Time: ', time.clock() - start_time, "seconds") #execution time

