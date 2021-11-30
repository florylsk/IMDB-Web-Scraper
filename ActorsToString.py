import pandas as pd
import json

pd_bad=pd.read_json("movies_final_cleaned.json",encoding="utf-8")
pd_bad=pd_bad.rename(columns={"Top Cast":"TopCast"})
print(pd_bad.columns)
pd_bad['TopCast'] = pd_bad.TopCast.apply(lambda x: x[1:-1].split(','))
print(pd_bad)
with open('movies_final_cleaned_final.json','w',encoding='utf-8') as f:
    json=json.dumps(pd_bad.to_dict(orient='records'),ensure_ascii=False,indent=0).encode('utf8') #delete indent for more compressed json
    f.write(json.decode())


