# initialise modules..
import oci
import os
import io
import json
import csv
from fdk import response
import logging
logging.basicConfig(level=logging.INFO)

# use oracle resource principal provider to extract credentials from rpst token..
def handler(ctx, data: io.BytesIO=None):
   signer = oci.auth.signers.get_resource_principals_signer()
   resp = do(signer)
   return response.Response(ctx,
      response_data=json.dumps(resp),
      headers={"Content-Type": "application/json"})

def do(signer):
   # return data..
   file_list = None
   file_list = []
   
   # helper variables
   counter = 0
   checker = False

   # configurations of function
   app_name = os.environ['app_name']
   py_app = os.environ['py_app']
   compartment_ocid = os.environ['compartment_ocid']
   object_storage_namespace = os.environ['namespace']
   input_bucket = os.environ['input_bucket']
   output_bucket = os.environ['output_bucket']

   # initialize clients..
   object_storage = oci.object_storage.ObjectStorageClient({}, signer=signer)
   data_flow = oci.data_flow.DataFlowClient({}, signer=signer)

   # list of all input and output objects 
   input_object_list = object_storage.list_objects(object_storage_namespace, input_bucket)
   output_object_list = object_storage.list_objects(object_storage_namespace, output_bucket)

   
   # for each file in bucket do 
   for i in input_object_list.data.objects:
      
      #test if the file is a csv
      if i.name.endswith(".csv"):
      
         # get metadata and open file
         csv_filename = i.name
         csv_filename_clean = csv_filename[:-4]
         parquet_filename = csv_filename_clean + '.parquet'
   
         # test if file was converted before
         checker = False
         for o in output_object_list.data.objects:
           
           if csv_filename_clean == o.name[:-9]:
               logging.info("File " + csv_filename + " was converted before - skipping")
               checker = True
               
         # if file wasn't converted, do convert
         if checker == False: 
         
            # counting files
            counter = counter+1
            
            csv_input_path = 'oci://' + input_bucket + '@' + object_storage_namespace + '/' + csv_filename
            parquet_output_path = 'oci://' + output_bucket + '@' + object_storage_namespace + '/' + parquet_filename
      
            # Create a new Data Flow Application
            input_parameter = oci.data_flow.models.ApplicationParameter(
                name="input_bucket",
                value=csv_input_path,
            )
            output_parameter = oci.data_flow.models.ApplicationParameter(
                name="output_bucket",
                value=parquet_output_path
            )
            create_application_details = oci.data_flow.models.CreateApplicationDetails(
                compartment_id=compartment_ocid,
                display_name=app_name+"_"+str(counter),
                driver_shape="VM.Standard2.1",
                executor_shape="VM.Standard2.1",
                num_executors=1,
                spark_version="2.4.4",
                file_uri=py_app,
                language="PYTHON",
                arguments=["${input_bucket}", "${output_bucket}"],
                parameters=[input_parameter, output_parameter],
            )
            logging.info("Creating the Data Flow Application for file " + csv_filename)
            application = data_flow.create_application(
                create_application_details=create_application_details
            )
            if application.status != 200:
                logging.info("Failed to create Data Flow Application for file " + csv_filename)
                logging.info(application.data)
            else:
                logging.info("Data Flow Application ID is " + application.data.id)
      
            # Create a Run from this Application
            logging.info("Creating the Data Flow Run")
            create_run_details = oci.data_flow.models.CreateRunDetails(
                compartment_id=compartment_ocid,
                application_id=application.data.id,
                display_name="Running: " + app_name + " Converting: " + csv_filename
            )
            run = data_flow.create_run(create_run_details=create_run_details)
            if run.status != 200:
                logging.info("Failed to create Data Flow Run")
                logging.info(run.data)
            else:
                logging.info("Data Flow Run ID is " + run.data.id + "Started on file: " + csv_filename)
     
     
