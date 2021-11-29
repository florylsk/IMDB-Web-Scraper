import pandas as pd
import json as js

dataframe_p1=pd.read_csv("Pregunta1.csv",nrows=50)
print(dataframe_p1)
#remove all columns ending in .keyword
dataframe_p1 = dataframe_p1[dataframe_p1.columns.drop(list(dataframe_p1.filter(regex='.keyword')))]
#drop useless columns
dataframe_p1 = dataframe_p1[dataframe_p1.columns.drop(['_id','_score','_type','year_int','_index'])]

with open('Pregunta1.json','w',encoding='utf-8') as f:
    json=js.dumps(dataframe_p1.to_dict(orient='records'),ensure_ascii=False,indent=0).encode('utf8') #delete indent for more compressed json
    f.write(json.decode())

dataframe_p3=pd.read_csv("Pregunta3.csv",nrows=50)
print(dataframe_p3)
#remove all columns ending in .keyword
dataframe_p3 = dataframe_p3[dataframe_p3.columns.drop(list(dataframe_p3.filter(regex='.keyword')))]
#drop useless columns
dataframe_p3 = dataframe_p3[dataframe_p3.columns.drop(['_id','_score','_type','year_int','_index'])]

with open('Pregunta3.json','w',encoding='utf-8') as f:
    json=js.dumps(dataframe_p3.to_dict(orient='records'),ensure_ascii=False,indent=0).encode('utf8') #delete indent for more compressed json
    f.write(json.decode())