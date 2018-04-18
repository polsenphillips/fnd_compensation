import csv
import pandas as pd
import requests.exceptions
import time
from irsx.xmlrunner import XMLRunner
from irsx.filing import InvalidXMLException

#Works for schemas: 2013v3.0 through part of 2016. Assuming that there's no way a FY2015 filing could use TY2016 schema.

timestr = time.strftime("%Y-%m-%d-%H-%M")
xml_runner = XMLRunner(documentation = True, csv_format = True)
df = pd.read_csv("files/new_ids.csv")
object_list = list(df["object_id"])


fieldnames = ["schema", #May need to join with the BMF to get the foundation type.
"object_id",
"/ReturnHeader/ReturnTs",
"/ReturnHeader/Filer/EIN",
#"/ReturnHeader/Filer/Name/BusinessNameLine1", This field has had its name changed multiple times. Just use the name in the BMF.
"/ReturnHeader/TaxPeriodEndDt",
"/ReturnHeader/ReturnTypeCd",
"/ReturnHeader/TaxYr",
"/IRS990PF/AmendedReturnInd",
"/IRS990PF/FinalReturnInd",
"/IRS990PF/StatementsRegardingActyGrp/PrivateOperatingFoundationInd",
"/IRS990PF/FMVAssetsEOYAmt", 
"/IRS990PF/MethodOfAccountingCashInd",
"/IRS990PF/AnalysisOfRevenueAndExpenses/OthEmplSlrsWgsRevAndExpnssAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/OthEmplSlrsWgsDsbrsChrtblAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/TotOprExpensesRevAndExpnssAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/TotOprExpensesDsbrsChrtblAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/ContriPaidRevAndExpnssAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/ContriPaidDsbrsChrtblAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/CompOfcrDirTrstRevAndExpnssAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/CompOfcrDirTrstDsbrsChrtblAmt",
"/IRS990PF/AnalysisOfRevenueAndExpenses/TotalExpensesDsbrsChrtblAmt",
"/IRS990PF/SupplementaryInformationGrp/TotalGrantOrContriPdDurYrAmt",
"/IRS990PF/SupplementaryInformationGrp/TotalGrantOrContriApprvFutAmt",
"/IRS990PF/QualifyingDistriPartXIIGrp/QualifyingDistributionsAmt",
"/IRS990PF/SummaryOfDirectChrtblActyGrp/Description1Txt",
"/IRS990PF/SummaryOfDirectChrtblActyGrp/Description2Txt",
"/IRS990PF/SummaryOfDirectChrtblActyGrp/Description3Txt",
"/IRS990PF/SummaryOfDirectChrtblActyGrp/Description4Txt",
"/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses1Amt",
"/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses2Amt",
"/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses3Amt",
"/IRS990PF/SummaryOfDirectChrtblActyGrp/Expenses4Amt"]


def parse_fields(object_id):
	try:
		filing_obj = xml_runner.run_filing(object_id)
		results = filing_obj.get_result()		
		data = {}
		if results:
			for sked in results:
				for this_result in sked["csv_line_array"]:
					if this_result["xpath"] in fieldnames:
						data.update({this_result["xpath"]: this_result["value"]})
					else:
						pass
		data.update({"schema": str(filing_obj.get_version())})
		data.update({"object_id": str(object_id)})
	except (AttributeError, InvalidXMLException):
		print("Error: " + str(object_id))
		with open("log_file.txt", "w") as log_file:
			log_file.write("AttributeError or InvalidXMLException:" + str(object_id) + "\n")
			log_file.close()
	except (requests.exceptions.ConnectionError):
		print("Error: " + str(object_id))
		with open("log_file.txt", "w") as log_file:
			log_file.write("ConnectionError:" + str(object_id) + "\n")
			log_file.close()				
	return data


def main():
	with open("files/new_schema_results.csv", "w") as out_file:
		dictwriter = csv.DictWriter(out_file, fieldnames=fieldnames, restval='', extrasaction='ignore')
		dictwriter.writeheader()
		for object_id in object_list:
			try:
				dictwriter.writerow(parse_fields(object_id))
			except (AttributeError, InvalidXMLException):
				print("Error: " + str(object_id))
		out_file.close()


if __name__ == "__main__":
	main()



	

