# Angelo-Bravo-ETL-Task


Please view 'ETL_Task_Description.docx' in the repository for specific details of the task.

The 'Angelo_ETL_Task.py' Python script extracts 2 csv files from loinc.org via HTTP requests, transforms the extracted data, and loads it into a PostgreSQL database, according to the format specified in https://www.i2b2.org/software/files/PDF/current/Ontology_Design.pdf.

The script outputs a CSV file with the rows inserted into the Postgres database.

The ETL should take about 25 seconds to execute.


To run the ETL:

	•	Input your loinc.org username and password as parameters in the extract(username, password) function

	•	Input your PostgreSQL database connection parameters in the load(pg_host, pg_port, pg_db, pg_user, pg_password, df_to_load) function.
		Only modify the following parameters: pg_host, pg_port, pg_db, pg_user, pg_password




Python Version: 3.6.2
PostgreSQL Version: 12.4

Required Python Packages/Modules:
	•	pandas
	•	xml
	•	psycopg2
	•	requests
	•	datetime
	•	warnings
	•	time
	•	io
	•	zipfile
