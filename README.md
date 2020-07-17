# dataflow_csv2parquet
Oracle OCI: Fn python code to start a Data Flow application &amp; Data Flow conversion from .csv to .parquet on files in object storage

Initialize a Function in OCI with the following variables:
- app_id <your app ID>
- app_name	<your name>
- compartment_ocid	<compartment OCID>
- input_bucket	<name of your input bucket in Object storage>
- output_bucket	<name of your output bucket in Object storage>
- namespace	<your object storage namestace>
- py_app	<full path csv2parquet.py in object storage: e.g. oci://codeBucket@namespace/csv2parquet.py>

  
Upload csv2parquet.py to object storage

Create an event to trigger your fn-application when a new file is uploaded to the input bucket

Create a folder on your local system with all these files appart from csv2parquet.py
Follow the Oracle documentation to upload the function to a function repository and setup functions
