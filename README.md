# dataflow_csv2parquet
Oracle OCI: Fn python code to start a Data Flow application &amp; Data Flow conversion from .csv to .parquet on files in Object Storage.

1. Initialize a Function in OCI with the following variables:
- app_id \<your Data Flow application OCID>
- app_name	\<your name>
- compartment_ocid	\<compartment OCID>
- input_bucket	\<name of your input bucket in Object storage>
- output_bucket	\<name of your output bucket in Object storage>
- namespace	\<your object storage namestace>
- py_app	\<full path csv2parquet.py in object storage: e.g. oci://codeBucket@namespace/csv2parquet.py>

  
1. Upload csv2parquet.py to object storage

1. Create an event to trigger your fn-application when a new file is uploaded to the input bucket.

1. Create a folder on your local system with all these files appart from csv2parquet.py
1. Follow the Oracle documentation to upload the function to a function repository and setup functions
