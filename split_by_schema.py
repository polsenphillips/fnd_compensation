import pandas as pd
import time

timestr = time.strftime("%Y-%m-%d-%H-%M")
schema_table = "files/schema_table.csv"
old_schemas = ["2009v1.0","2009v1.1","2009v1.2","2009v1.3","2009v1.4","2009v1.7","2010v3.2","2010v3.4","2010v3.6","2010v3.7","2011v1.2","2011v1.3","2011v1.4","2011v1.5","2012v2.0","2012v2.1","2012v2.2","2012v2.3","2012v3.0"]

o = pd.read_csv(schema_table, skip_blank_lines = True, dtype = {"object_id": "object"})
o.dropna(inplace = True)

o1 = o[o["schema"].isin(old_schemas)]
o2 = o[~o["schema"].isin(old_schemas)]

pd.DataFrame(o1["object_id"]).to_csv("files/old_ids.csv", float_format='%20.0f', index = False)
pd.DataFrame(o2["object_id"]).to_csv("files/new_ids.csv", float_format='%20.0f', index = False)