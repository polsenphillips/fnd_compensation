import csv
import pandas as pd
import time
from irsx.xmlrunner import XMLRunner

timestr = time.strftime("%Y-%m-%d-%H-%M")
old_schema_results = "files/old_schema_results.csv"
new_schema_results = "files/new_schema_results.csv"

# Pulling useful fields from the BMF table 

region_1 = pd.read_csv("https://www.irs.gov/pub/irs-soi/eo1.csv", dtype = {"EIN": "object"})
region_2 = pd.read_csv("https://www.irs.gov/pub/irs-soi/eo2.csv", dtype = {"EIN": "object"})
region_3 = pd.read_csv("https://www.irs.gov/pub/irs-soi/eo3.csv", dtype = {"EIN": "object"})
region_4 = pd.read_csv("https://www.irs.gov/pub/irs-soi/eo4.csv", dtype = {"EIN": "object"})

bmf_region_tables = [region_1, region_2, region_3, region_4]

bmf = pd.concat(bmf_region_tables)
bmf.drop_duplicates(inplace = True) #have multiple headers in there

bmf_key_fields = ["EIN", "NAME", "CITY", "STATE", "GROUP", "SUBSECTION", "AFFILIATION", \
              "CLASSIFICATION", "RULING", "DEDUCTIBILITY", "FOUNDATION", "ACTIVITY", \
              "ORGANIZATION", "STATUS", "NTEE_CD"]

bmf2 = bmf[bmf_key_fields]              


# Renaming columns based on the crosswalk file and concatenating the two dataframes
df1 = pd.read_csv(old_schema_results)
df2 = pd.read_csv(new_schema_results)

df1.rename(columns = {"/ReturnHeader/Timestamp":"/ReturnHeader/ReturnTs",
"/ReturnHeader/TaxPeriodEndDate":"/ReturnHeader/TaxPeriodEndDt",
"/ReturnHeader/ReturnType":"/ReturnHeader/ReturnTypeCd",
"/ReturnHeader/TaxYear":"/ReturnHeader/TaxYr",
"/IRS990PF/AmendedReturn":"/IRS990PF/AmendedReturnInd",
"/IRS990PF/FinalReturn":"/IRS990PF/FinalReturnInd",
"/IRS990PF/StatementsRegardingActivities/PrivateOperatingFoundation":"/IRS990PF/StatementsRegardingActyGrp/PrivateOperatingFoundationInd",
"/IRS990PF/FMVAssetsEOY":"/IRS990PF/FMVAssetsEOYAmt",
"/IRS990PF/MethodOfAccountingCash":"/IRS990PF/MethodOfAccountingCashInd",
"/IRS990PF/AnalysisOfRevenueAndExpenses/OthEmplSlrsWgsRevAndExpnss":"/IRS990PF/AnalysisOfRevenueAndExpenses/OthEmplSlrsWgsRevAndExpnssAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/OthEmplSlrsWgsDsbrsChrtblPrps":"/IRS990PF/AnalysisOfRevenueAndExpenses/OthEmplSlrsWgsDsbrsChrtblAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/TotOprExpensesRevAndExpnss":"/IRS990PF/AnalysisOfRevenueAndExpenses/TotOprExpensesRevAndExpnssAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/TotOprExpensesDsbrsChrtblPrps":"/IRS990PF/AnalysisOfRevenueAndExpenses/TotOprExpensesDsbrsChrtblAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/ContriGiftsPaidRevAndExpnss":"/IRS990PF/AnalysisOfRevenueAndExpenses/ContriPaidRevAndExpnssAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/ContriGiftsPaidDsbrsChrtblPrps":"/IRS990PF/AnalysisOfRevenueAndExpenses/ContriPaidDsbrsChrtblAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/CompOfcrDirTrstRevAndExpnss":"/IRS990PF/AnalysisOfRevenueAndExpenses/CompOfcrDirTrstRevAndExpnssAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/CompOfcrDirTrstDsbrsChrtblPrps":"/IRS990PF/AnalysisOfRevenueAndExpenses/CompOfcrDirTrstDsbrsChrtblAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/TotalExpensesDsbrsChrtblPrps":"/IRS990PF/AnalysisOfRevenueAndExpenses/TotalExpensesDsbrsChrtblAmt",
"/IRS990PF/SupplementaryInformation/TotGrantOrContriPaidDuringYear":"/IRS990PF/SupplementaryInformationGrp/TotalGrantOrContriPdDurYrAmt",
"/IRS990PF/SupplementaryInformation/TotGrantOrContriApprovedFuture":"/IRS990PF/SupplementaryInformationGrp/TotalGrantOrContriApprvFutAmt",
"/IRS990PF/QualifyingDistributions/QualifyingDistributions":"/IRS990PF/QualifyingDistriPartXIIGrp/QualifyingDistributionsAmt",
"/IRS990PF/SummaryOfDirectCharitableActy/Description1":"/IRS990PF/SummaryOfDirectChrtblActyGrp/Description1Txt",
"/IRS990PF/SummaryOfDirectCharitableActy/Description2":"/IRS990PF/SummaryOfDirectChrtblActyGrp/Description2Txt",
"/IRS990PF/SummaryOfDirectCharitableActy/Description3":"/IRS990PF/SummaryOfDirectChrtblActyGrp/Description3Txt",
"/IRS990PF/SummaryOfDirectCharitableActy/Description4":"/IRS990PF/SummaryOfDirectChrtblActyGrp/Description4Txt",
"/IRS990PF/SummaryOfDirectCharitableActy/Expenses1":"/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses1Amt",
"/IRS990PF/SummaryOfDirectCharitableActy/Expenses2":"/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses2Amt",
"/IRS990PF/SummaryOfDirectCharitableActy/Expenses3":"/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses3Amt",
"/IRS990PF/SummaryOfDirectCharitableActy/Expenses4":"/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses4Amt"}, inplace = True) 

dataframes = [df1, df2]

df3 = pd.concat(dataframes)

#Reformatting the EIN field

df3_ein_list = []

for ein in df3["/ReturnHeader/Filer/EIN"]: 
    if len(str(ein)) < 9:
        df3_ein_list.append(str(ein).zfill(9))
    else: 
        df3_ein_list.append(str(ein))

df3 = df3.assign(EIN_formatted = df3_ein_list) 

# Joining the BMF fields to the new dataframe. Some of the XML records won't find a match on the BMF because some 990PF filers are not registered with the IRS.
df4 = pd.merge(df3, bmf2, how = "left", left_on = "EIN_formatted", right_on = "EIN")


#Make a FYE field
fye_list = []

for date in df4["/ReturnHeader/TaxPeriodEndDt"]: 
    fye_list.append(date[:4])

df5 = df4.assign(FYE = fye_list)

# Combine direct charitable expenses fields
df6 = df5.assign(DIRECT_CHARITABLE_EXPENSES_TOTAL = (df5["/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses1Amt"].fillna(0) +
df5["/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses2Amt"].fillna(0) +
df5["/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses3Amt"].fillna(0) +
df5["/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses4Amt"].fillna(0)))

# Testing 
# df6.to_csv("~/Downloads/df6_test.csv", index  = False)

# Write function to standardize the OPERATING_FOUNDATION field and 
def recast_operating_values(value):
    if isinstance(value, basestring):
        if value == "true":
            new_value = 1
        elif value == "false": 
            new_value = 0
        elif value == "1":
            new_value = 1
        else: #if item == "0"
            new_value = 0
    else: #This is for the null values (np.NaN), which are floats
        new_value = 0
    return new_value

df6["OPERATING_FOUNDATION"] = df6["/IRS990PF/StatementsRegardingActyGrp/PrivateOperatingFoundationInd"].apply(recast_operating_values)

# Drop operating foundations
df7 = df6[df6["OPERATING_FOUNDATION"] == 0]



##################################################################
# This needs to be checked
# Drop amended filings



##################################################################

# Select only foundations with three consecutive fiscal years of data
# This is not working.


df10 = df9[df9["FYE"].isin([2013, 2014, 2015])]
df10_count = df10["EIN_formatted"].value_counts(ascending = False)
df10_trips = list(df10_count[df10_count >= 3].index)
df11 = df10[df10["EIN_formatted"].isin(df10_trips)]

df11.to_csv("~/Desktop/foundation_salaries_payout/code/scripts/files/final_data" + timestr + ".csv", index = False)




##################################################################
# Testing: One from each schema year. 
# No mixed types. 
# No missing rows. X
# correct number of operating PFs  with three years. 
# Most recent amended version
# Derived fields working
# Check my proportions against the Council on Foundations
# Fix columns with multiple types
















