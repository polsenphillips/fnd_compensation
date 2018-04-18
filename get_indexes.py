import pandas as pd
import time
from irsx.xmlrunner import XMLRunner

timestr = time.strftime("%Y-%m-%d-%H-%M") 

url = "https://s3.amazonaws.com/irs-form-990/index_"
filing_years = range(2011, 2019)
index_list = []
fiscal_years = [2013, 2014, 2015]

for year in filing_years:
    df = pd.read_csv(url + str(year) + ".csv", dtype = {"EIN": "object", "TAX_PERIOD": "object"}, error_bad_lines = False)
    index_list.append(df)

# One dataframe containing all years' efiling indexes
ix_all = pd.concat(index_list)

# Deriving a fiscal year end field 
fye_list = []
for period in ix_all["TAX_PERIOD"].astype("str"):
    fye_list.append(int(period[:4]))

ix_all = ix_all.assign(FYE = fye_list)

# A dataframe only containing records for private foundations' filings in the 2013, 2014 and 2015 fiscal years
ix_pf_all = ix_all[(ix_all["RETURN_TYPE"] == "990PF") & (ix_all[ix_all["FYE"].isin(fiscal_years)])] 

#Only EINs with at least three filings

ein_count_list = ix_pf_all["EIN"].value_counts()
ein_trips_list = ein_count_list[ein_count_list >=3]
ein_trips_list = pd.Series(ein_trips_list.index)

pf_three_years = ix_pf_all[ix_pf_all["EIN"].isin(ein_trips_list)] 
pf_three_years["OBJECT_ID"].to_csv("files/object_ids.csv", index = False) 

all_object_ids = pf_three_years["OBJECT_ID"]



