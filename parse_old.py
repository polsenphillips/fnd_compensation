import csv
import pandas as pd
import requests.exceptions
import time 
from irsx.xmlrunner import XMLRunner
from irsx.filing import InvalidXMLException

#Works for schemas: 2009v1.0;2009v1.1;2009v1.2;2009v1.3;2009v1.4;2009v1.7;2010v3.2;2010v3.4;2010v3.6;2010v3.7;2011v1.2;2011v1.3;2011v1.4;2011v1.5;2012v2.0;2012v2.1;2012v2.2;2012v2.3;2012v3.0

timestr = time.strftime("%Y-%m-%d-%H-%M")
xml_runner = XMLRunner(documentation = True, csv_format = True)
df = pd.read_csv("files/old_ids.csv") #Update based on file name
object_list = list(df["object_id"])


fieldnames = ["schema", #May need to join with the BMF to get the foundation type.
"object_id",
"/ReturnHeader/Timestamp",
"/ReturnHeader/Filer/EIN",
#"/ReturnHeader/Filer/Name/BusinessNameLine1", This field has had its name changed multiple times. Just use the name in the BMF.
"/ReturnHeader/TaxPeriodEndDate",
"/ReturnHeader/ReturnType",
"/ReturnHeader/TaxYear",
"/IRS990PF/AmendedReturn",
"/IRS990PF/FinalReturn",
"/IRS990PF/StatementsRegardingActivities/PrivateOperatingFoundation",
"/IRS990PF/FMVAssetsEOY", 
"/IRS990PF/MethodOfAccountingCash",
"/IRS990PF/AnalysisOfRevenueAndExpenses/OthEmplSlrsWgsRevAndExpnss",
"/IRS990PF/AnalysisOfRevenueAndExpenses/OthEmplSlrsWgsDsbrsChrtblPrps",
"/IRS990PF/AnalysisOfRevenueAndExpenses/TotOprExpensesRevAndExpnss",
"/IRS990PF/AnalysisOfRevenueAndExpenses/TotOprExpensesDsbrsChrtblPrps",
"/IRS990PF/AnalysisOfRevenueAndExpenses/ContriGiftsPaidRevAndExpnss",
"/IRS990PF/AnalysisOfRevenueAndExpenses/ContriGiftsPaidDsbrsChrtblPrps",
"/IRS990PF/AnalysisOfRevenueAndExpenses/CompOfcrDirTrstRevAndExpnss",
"/IRS990PF/AnalysisOfRevenueAndExpenses/CompOfcrDirTrstDsbrsChrtblPrps",
"/IRS990PF/AnalysisOfRevenueAndExpenses/TotalExpensesDsbrsChrtblPrps",
"/IRS990PF/SupplementaryInformation/TotGrantOrContriPaidDuringYear",
"/IRS990PF/SupplementaryInformation/TotGrantOrContriApprovedFuture",
"/IRS990PF/QualifyingDistributions/QualifyingDistributions",
"/IRS990PF/SummaryOfDirectCharitableActy/Description1",
"/IRS990PF/SummaryOfDirectCharitableActy/Description2",
"/IRS990PF/SummaryOfDirectCharitableActy/Description3",
"/IRS990PF/SummaryOfDirectCharitableActy/Description4",
"/IRS990PF/SummaryOfDirectCharitableActy/Expenses1",
"/IRS990PF/SummaryOfDirectCharitableActy/Expenses2",
"/IRS990PF/SummaryOfDirectCharitableActy/Expenses3",
"/IRS990PF/SummaryOfDirectCharitableActy/Expenses4"]


def parse_fields(object_id):
	data = {}
	try:
		filing_obj = xml_runner.run_filing(object_id)
		results = filing_obj.get_result()
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
	with open("files/old_schema_results.csv", "w") as out_file:
		dictwriter = csv.DictWriter(out_file, fieldnames=fieldnames, restval='', extrasaction='ignore')
		dictwriter.writeheader()
		for object_id in object_list:
				dictwriter.writerow(parse_fields(object_id))
		out_file.close()


if __name__ == "__main__":
	main()








