import json
#import snowflake.connector
import pandas as pd
import csv
import fastparquet
from fastparquet import *
import glob
#pip install --upgrade snowflake-connector-python <- use this to install the snowflake connector package

## Load_[Source] --> Parse --> Create_Table_DDL --> Sarath's review and edits --> Load_SQL_DDL --> Split_Tables --> Create Mapping Document


### Helpful Parsing Functions

def Replacy(a,b):
    while a in b:
        b = b.replace(a,'|')
    return b



def Splitty(x,y):
    SL = ['\\','.']
    for item in SL:
        x = Replacy(item,x)
        x = x.split('|')
    return x[y]

def GetTableName(f):
    fpath = str(f).split(".")[0]
    tablename = str(fpath.split("\\")[-2])
    return tablename

### Load CSV

def Load_CSV(f):
    with open(f, 'r') as r:
        S = [item.split(",")for item in r.readlines()]
    return S




    
### Load Reference Tables

def Get_Line(f,x):
    Header = str(f[0][x])
    l = [item[x] for item in f if item[x] != ""]
    l.pop(0)
    s = list(set(l))
    return [Header,s]

def Test_Line(l):
    reflist = []
    for f in glob.glob(r'Adapt2_DataPoints\Reference\*.csv'):
        Ref = Load_CSV(f)
        y = 0
        while y < len(Ref):
            try:
                C = Get_Line(Ref,y)
                Header = C[0]
                n = 0
                for a in l:
                    if a in C[1]:
                        n += 1
                if n > 0:
                    reflist.append([f,Header,n/len(l)])
                else:
                    pass
            except:
                pass
            y += 1
    return reflist
                
                
def Find_Refs(f):
    T = Load_CSV(f)

    x = 0
    while x < len(T[0]):
        try:
            L = Get_Line(T,x)
            Header = L[0]
            l = L[1]
            D = Test_Line(l)
            if D[2] != []:
                print(Header,D)
                print("")
        except:
            pass
        x += 1

def Ref_Match():
    for file in glob.glob('Adapt2_DataPoints\*\*.csv'):
        print(file)
        Find_Refs(file)
        print("")


#Ref_Match()

def Get_Line(f,x,l):
    for f in glob.glob(r'Adapt2_DataPoints\Reference\*.csv'):
        Ref = Load_CSV(f)
        sl = [item[x] for item in Ref]



### Parse Snowflake DDL

def Load_SQL_DDL(f):
    with open(f, 'r') as r:
        S = r.readlines()
    return S

def Split_Tables(S):
    masterlist = []
    table = []
    for line in S:
        if 'CREATE OR REPLACE TABLE' in line:
            masterlist.append(table)
            table = []
            table.append(line)
        else:
            table.append(line)
    masterlist.append(table)
    masterlist.pop(0)
    for item in masterlist:
        for line in item:
            line = Replacy(' ',line)
            line = Replacy('(',line)
            line = Replacy(')',line)
            line = Replacy(',',line)
            line = Replacy('\t',line)
            line = Replacy('\n',line)
            line = line.split("|")
            while '' in line:
                line.remove('')
            if line == []:
                pass
            else:
                print(line)


#Split_Tables(Load_SQL_DDL('Data\Snowflake_DWH_Build v2.sql'))



### Load JSON Files

def Load_JSON(f):
    with open(f, 'r') as r:
        jmain = json.load(r)
    return jmain

def Parse_Layer(l,n):
    kvl = [[key, value,n] for key, value in l.items()]
    return kvl

def Rec_PL(f):
    Layer_List = []
    Layer_List.append(Parse_Layer(Load_JSON(f),str(f)))
    for x in Layer_List:
        for i in x:
            if type(i[1]) == list:
                a = 0
                while a < len(i[1]):
                    try:
                        Layer_List.append(Parse_Layer(i[1][a],i[0]))
                        #a += 1
                    except:
                        #print(a, 'failed')
                        #a += 1
                        continue
                    a += 1
    masterlist = []
    for d in Layer_List:
        typelist = []
        for item in d:
            typelist.append([item[2],item[0],str(type(item[1])).split("'")[1]])
        if typelist not in masterlist:
            masterlist.append(typelist)
            
    headers = []
    for item in masterlist:
        for h in item:
            if h[0] not in headers:
                headers.append(h[0])
    cleanlist = []
    for h in headers:
        uh = []
        for item in masterlist:
            for entry in item:
                if entry[0] == h:
                    if entry not in uh:
                        uh.append(entry)
        cleanlist.append(uh)
    u_entries_full = []
    for item in cleanlist:
        name = item[0][0]
        u_entries = []
        entries = list(set([entry[1] for entry in item]))
        for e in entries:
            types = [entry[2] for entry in item if entry[1] == e]
            u_entries.append([name,e,types])
        u_entries_full.append(u_entries)
    return u_entries_full
    
def Print_Tables(l):
    for item in l:
        print("insert into",str(item[0][0])+"_stg (")
        for entry in item:
            print(entry[1]+",")
        print(")")
        print("select")
        for entry in item:
            if 'int' in entry[2] or 'float' in entry[2]:
                print("try_to_number(value:"+entry[1]+"::string,38,2) "+entry[1]+",")
            else:
                print("value:"+entry[1]+"::string  "+entry[1]+",")
        print(str(item[0][0])+":snapshotTimestamp::Timestamp snapshotTimestamp,")
        print(str(item[0][0])+":createdTimestamp::Timestamp createdTimestamp,")
        print(str(item[0][0])+":description::String description,")
        print("current_timestamp()::timestamp_ntz DWH_CREATION_TIME,")
        print("'SKM3' DWH_CREATION_USER,")
        print("'123' DWH_LOAD_UUID")
        print("FROM etl_confidential."+str(item[0][0]))
        print(", lateral flatten( input =>"+str(item[0][0])+":"+str(item[0][0])+" );")
        print("delete from etl_confidential."+str(item[0][0])+";")
        print("")

def Create_Table_DDL(l,source):
    Directory = str('Create_Table_DDL\\'+source+"WDL_DDL.SQL")
    with open(Directory, 'w') as export:
        for item in l:
            tablename = str(source+item[0][0].upper())
            export.write('CREATE OR REPLACE TABLE '+tablename+'_STG (')
            export.write("\n")
            for entry in item:
                if entry[1].upper() == "SYSTEM_REFERENCE" or entry[1].upper() == "SORT_ORDER":
                    continue
                else:
                    if 'int' in entry[2] or 'float' in entry[2]:
                        export.write(entry[1].upper()+" NUMBER(38,2),")
                        export.write("\n")
                    elif 'bool' in entry[2]:
                        export.write(entry[1].upper()+" BOOLEAN,")
                        export.write("\n")
                    else:
                        export.write(entry[1].upper()+" VARCHAR(50),")
                        export.write("\n")
            export.write("DWH_CREATION_TIME TIMESTAMP_NTZ,")
            export.write("\n")
            export.write("DWH_CREATION_USER  VARCHAR(50),")
            export.write("\n")
            export.write(");")
            export.write("\n")
            export.write("\n")
                    
                
        

def Print_Tables_To_File(l):
    for item in l:
        tablename = str(item[0][0])
        Directory = str('SQL_Mapping_Files\\'+tablename+"_STG.SQL")
        with open(Directory, 'w') as export:
            export.write("insert into "+tablename+"_stg (")
            export.write("\n")
            for entry in item:
                export.write(entry[1]+",")
                export.write("\n")
            export.write(")")
            export.write("\n")
            export.write("select")
            export.write("\n")
            for entry in item:
                if 'int' in entry[2] or 'float' in entry[2]:
                    export.write("try_to_number(value:"+entry[1]+"::string,38,2) "+entry[1]+",")
                    export.write("\n")
                else:
                    export.write("value:"+entry[1]+"::string  "+entry[1]+",")
                    export.write("\n")
            export.write(tablename+":snapshotTimestamp::Timestamp snapshotTimestamp,")
            export.write("\n")
            export.write(tablename+":createdTimestamp::Timestamp createdTimestamp,")
            export.write("\n")
            export.write(tablename+":description::String description,")
            export.write("\n")
            export.write("current_timestamp()::timestamp_ntz DWH_CREATION_TIME,")
            export.write("\n")
            export.write("'SKM3' DWH_CREATION_USER,")
            export.write("\n")
            export.write("'123' DWH_LOAD_UUID")
            export.write("\n")
            export.write("FROM etl_confidential."+tablename)
            export.write("\n")
            export.write(", lateral flatten( input =>"+tablename+":"+tablename+" );")
            export.write("\n")
            export.write("delete from etl_confidential."+tablename+";")
            export.write("\n")
        
    

#Print_Tables(Rec_PL('Data\Instantaneous-20200102041400Z.json'))

def ParqParse(f):
    pf = ParquetFile(f)
    df = pf.to_pandas()
    fpath = str(f).split(".")[0]
    tablename = str(fpath.split("\\")[-2])
    table = []
    for col, d in zip(df.columns, df.dtypes):
        att = [tablename, col, str(d)]
        table.append(att)
    return table


def GetParqAtt(f):
    pf = ParquetFile(f)
    df = pf.to_pandas()
    fpath = str(f).split(".")[0]
    tablename = str(fpath.split("\\")[-2])
    table = []
    print(df.iloc[0])
    for col, d in zip(df.columns, df.dtypes):
        print([tablename, col, str(d)])
    


#print(ParqParse(r'Data\PowerSuite-2020110\2020110\master\fuel_type\LOAD00000001.parquet'))
#Parqs = [ParqParse(f) for f in glob.glob(r'Data\PowerSuite-2020110\*\*\*\*.parquet')]
#T = [ParqParse(r'Data\PowerSuite-2020110\2020110\master\unit_capabilities\LOAD00000001.parquet')]
#Create_Table_DDL(Parqs,"POWER_SUITE_")
#Print_Tables_To_File(T)

#GetParqAtt(r'Data\PowerSuite-2020110\2020110\master\unit_capabilities\LOAD00000001.parquet')


def Connections():
    pass
    # Copying Data from S3 into Snowflake

    # Connect to S3

    #con.cursor().execute("""
    #COPY INTO testtable FROM s3://<your_s3_bucket>/data/
        #CREDENTIALS = (
            #aws_key_id='{aws_access_key_id}',
            #aws_secret_key='{aws_secret_access_key}')
        #FILE_FORMAT=(field_delimiter=',')
    #""".format(
        #aws_access_key_id=AWS_ACCESS_KEY_ID,
        #aws_secret_access_key=AWS_SECRET_ACCESS_KEY))

    # Connect to Snowflake

    #ctx = snowflake.connector.connect(
        #user='<your_user_name>',
        #password='<your_password>',
        #account='<your_account_name>'
        #)
    #cs = ctx.cursor()
    #try:
        #cs.execute("SELECT current_version()")
        #one_row = cs.fetchone()
        #print(one_row[0])
    #finally:
        #cs.close()
    #ctx.close()

    # Load into Snowflake

    #conn.cursor().execute(
        #"CREATE OR REPLACE TABLE "
        #"test_table(col1 integer, col2 string)")

    #conn.cursor().execute(
        #"INSERT INTO test_table(col1, col2) VALUES " + 
        #"    (123, 'test string1'), " + 
        #"    (456, 'test string2')")




        


def ParExcel(f):
    dataset = []
    x1 = pd.ExcelFile(f)
    Admin_sheets = ['Change Log', 'BlankDim', 'BlankFact']
    sheets = [name for name in x1.sheet_names if name not in Admin_sheets]
    
    for s in sheets:
        df = x1.parse(s)
        for row in df.iterrows():
            index, data = row
            dataset.append(data.tolist())
            

    #unit_split = []
    #for item in dataset:
        #match = re.search('Description',str(item[0]),flags=0)
        #if match:           
            #unit_split = []
        #else:
            #if str(item[1]) != 'nan' and str(item[3]) != 'nan':
                #unit_split.append([Splitty(f,3)]+item)

    return [s,dataset[1]]


def Create_XMLs(f):
    
    TMDMAP = list(csv.reader(open(r'Talend_Metadata_Mapping.csv', 'rt'), delimiter=','))
    TMDMAP.pop(0)

    SFMD = list(set([item[2] for item in TMDMAP if item[2] != 'For Reference']))
    errorlog = []




    #dataset = []
    x1 = pd.ExcelFile(f)
    Admin_sheets = ['Change Log', 'BlankDim', 'BlankFact']
    sheets = [name for name in x1.sheet_names if name not in Admin_sheets]
    for s in sheets:
        dataset = []
        dfn = x1.parse(s)
        Table_Name = str(dfn.columns[1]+'.xml')
        Directory = str('XML_Mapping_Files\\'+Table_Name)

        df = x1.parse(s,header=9)
        for row in df.iterrows():
            index, data = row
            dataset.append(data.tolist())
        #dataset.pop(0)



        with open(Directory, 'w') as export:
            export.write('<?xml version="1.0" encoding="UTF-8"?><schema>')
    


            for line in dataset:
                if str(line[0]) == 'nan':
                    name = ""
                else:
                    name = line[0]
                if str(line[2]) == 'nan':
                    comment = ""
                else:
                    comment = line[2]
                datatype = ""
                originalDbColumnName = ""
                pattern = ""
                try:
                    if line[3] not in SFMD:
                        errorlog.append([Table_Name,line])
                        continue
                except:
                    continue
                else:
                    for entry in TMDMAP:
                        if entry[2] == line[3]:
                            datatype = entry[1]
                            originalDbColumnName = line[3]
                if datatype == 'id_Date':
                    pattern = '&quot;dd-MM-yyyy&quot;'
                if str(line[4]) == 'nan':
                    length = "-1"
                else:
                    length = str(line[4]).split('.')[0]
                if str(line[5]) == 'nan':
                    precision = "-1"
                else:
                    precision = str(line[5]).split('.')[0]
                key = 'false'
                if str(line[6]).upper() == 'Y':
                    key = 'true'
                nullable = 'true'
                if str(line[8]).upper() == 'N':
                    nullable = 'false'
                export.write('<column comment="'+comment+'" default="" key="'+key+'" label="'+name+'" length="'+length+'" nullable="'+nullable+'" originalDbColumnName="" originalLength="" pattern="'+pattern+'" precision="'+precision+'" talendType="'+datatype+'" type=""/>')
            export.write('</schema>')

    for line in errorlog:
        print(line)

#Create_XMLs('Data\GCS_Mapping_Spec.xlsm')
#Create_XMLs('Data\PowerSuite Mapping Spec.xlsm')
