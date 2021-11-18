import pandas as pd
import json

pd_bad=pd.read_json("movies_final.json",encoding="utf-8")
pd_na=pd_bad.fillna("Not Available")
pd_final=pd_na.replace("Not Available",'')
print(pd_final)
with open('movies_final_cleaned.json','w',encoding='utf-8') as f:
    json=json.dumps(pd_final.to_dict(orient='records'),ensure_ascii=False,indent=0).encode('utf8') #delete indent for more compressed json
    f.write(json.decode())