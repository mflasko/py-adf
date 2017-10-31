from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import timedelta, datetime, timezone
import time

class DataFactory:

    def __init__(self, name, resource_group, context, default_linked_svcs = None):
        self.name = name
        self.resource_group = resource_group
        self.adf_client = DataFactoryManagementClient(context.credentials, context.subscription_id)
        self.factory = None
        # dict of default linked services       
        # self.def_linked_svcs = default_linked_svcs  # ['<LS type name>'] -> ('<LS Name>', LS Object)
        
    
    def create(self):
        df_resource = Factory(location='eastus')
        self.factory = self.adf_client.factories.create_or_update(self.resource_group, self.name, df_resource)
        while self.factory.provisioning_state != 'Succeeded':
            self.factory = self.adf_client.factories.get(self.resource_group, self.name)
            time.sleep(1)
        
    def upcreate_pipeline(self, pipeline):
        return self.adf_client.pipelines.create_or_update(self.resource_group, self.name, pipeline.name, pipeline.pipeline_ref)

    @property
    def linked_svc_resolver(self):
        return self.default_ls_resolver

    @linked_svc_resolver.setter
    def linked_svc_resolver(self, func):
        self.default_ls_resolver = func 

    def upcreate_dataset(self, name, dataset):
        if dataset.linked_service_name == None or dataset.linked_service_name == "":
            if self.default_ls_resolver != None:
                # lookup linked service for dataset 
                ls_ref = self.default_ls_resolver(dataset)
                dataset.linked_service_name = ls_ref
            else:
                #todo through error as can't resovle linked service for the dataset
                pass
        return self.adf_client.datasets.create_or_update(self.resource_group, self.name, name, dataset)

    def upcreate_linkedservices(self, linked_svcs):
        for key in linked_svcs:
            name, ls = linked_svcs[key]
            self.upcreate_linkedservice(name, ls)

    def upcreate_linkedservice(self, name, linked_service):
        self.adf_client.linked_services.create_or_update(self.resource_group, self.name, name, linked_service)

    def run_pipeline(self, name, params=None):
        return self.adf_client.pipelines.create_run(self.resource_group, self.name, name, params)
    
    def watch_pipeline_run(self, pipeline_run):
        line_str = '------------------------------------------'

        print(line_str)
        print('Pipeline: ' + 'put name here')
        run_id = pipeline_run.run_id
        print('Run ID: ' + run_id)
        print(line_str)

        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        tomorrow = datetime.now(timezone.utc) + timedelta(days=1)

        print()
        print("Getting pipeline run status...")
        while True:
            try:
                pl_run = self.adf_client.pipeline_runs.get(self.resource_group, self.name, run_id)
                print("Pipeline run status: " + pl_run.status)
                if pl_run.status == 'Suceeded' or pl_run.status == 'Failed':
                    break

                print()
                print("Getting status of activity runs...")
                act_runs = list(self.adf_client.activity_runs.list_by_pipeline_run(self.resource_group, self.name, run_id, yesterday, tomorrow))
                # Print the status of activity runs
                should_break = True
                for act_run in act_runs:
                    print(act_run.activity_name, act_run.status)
                    if (act_run.output != None and act_run.output != {}):
                        print(act_run.output)
                    if act_run.status != 'Succeeded' or act_run.status == 'Failed':
                        should_break = False
                print (line_str)
                if should_break:
                    break
                time.sleep(10)
            except ErrorResponseException:
                pass
                print('Exception:' + ErrorResponseException) 



    # def get_reference(self, type):
    #     return LinkedServiceReference(self.linked_services[type][0])

    # get LS type for a dataset 
    # def __get_linked_svc_type(dataset):
    #     return DataFactory.ds_type_to_ls_type[dataset.type]
        
    #todo: finish the full map
    # ds_type_to_ls_type = dict()
    # ds_type_to_ls_type['AmazonS3Object'] = 'AmazonS3'
    # ds_type_to_ls_type['AzureBlob'] = 'AzureStorage'
    # ds_type_to_ls_type['AzureSqlDWTable'] = 'AzureSqlDW'

        # get LS type associated with a data set or activity type


    