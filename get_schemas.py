import csv
import pandas as pd
import requests.exceptions
import time
from irsx.xmlrunner import XMLRunner
from irsx.filing import InvalidXMLException
from requests.exceptions import ConnectionError

timestr = time.strftime("%Y-%m-%d-%H-%M")


xml_runner = XMLRunner(documentation = True, csv_format = True)
fieldnames  = ["object_id", "schema"]
object_ids_table = "files/all_object_ids_2018-03-02-11-29.csv"

def get_schema(object_id):
	data = {}
	try: 
		filing_obj = xml_runner.run_filing(int(object_id))
		if filing_obj:
			data.update({"object_id":str(object_id)})
			data.update({"schema":str(filing_obj.get_version())})
	except (AttributeError, InvalidXMLException):
		print("Error: " + str(object_id))
		with open("files/get_schema_errors/invalidXML_log_file.txt", "w") as log_file: #Why can't i put these in a separate "errors" folder?
			log_file.write("AttributeError or InvalidXMLException:" + str(object_id) + "\n")
			log_file.close()
	except (ConnectionError):
		print("Error: " + str(object_id))
		with open("files/get_schema_errors/connectionerror_log_file.txt", "w") as log_file: #Why can't i put these in a separate "errors" folder?
			log_file.write("ConnectionError:" + str(object_id) + "\n")
			log_file.close()
	return data	


with open(object_ids_table, "r") as f:
	reader = csv.reader(f)
	object_list = [row[0] for row in reader]


with open("files/schema_table.csv", "w") as out_file:
		dictwriter = csv.DictWriter(out_file, fieldnames=fieldnames, restval='', extrasaction='ignore')
		dictwriter.writeheader()
		for object_id in object_list:
			dictwriter.writerow(get_schema(object_id))



