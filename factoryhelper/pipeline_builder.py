from azure.mgmt.datafactory.models import *

class PipelineBuilder:

    def __init__(self, name, params=None, resolve_linked_svc_func=None):
        self.name = name
        self.anchor_activity = None
        self.activities = []
        self.params = params
        self.default_ls_resolver = resolve_linked_svc_func
        self.on_complete_pl = None

    def copy(self, name, source_dataset, sink_dataset, depends_on=None, dependancy_type='Success'): 
        datasetref_in = DatasetReference(source_dataset.name)
        datasetref_out = DatasetReference(sink_dataset.name)  
        act = CopyActivity(name, inputs=[datasetref_in], 
                                 outputs=[datasetref_out], 
                                 source=FileSystemSource(), 
                                 sink=BlobSink())
        self._set_act_dependancy(act, depends_on, dependancy_type)
        return act

    def exec_pipeline(self, name, pipeline, description=None, depends_on=None, parameters=None, wait_on_completion=None):
        act = ExecutePipelineActivity(name, pipeline.pipeline_ref, description=description, depends_on=depends_on, parameters=parameters, wait_on_completion=wait_on_completion)
        self._set_act_dependancy(act, depends_on, dependancy_type)
        return act        

    def usql(self, name, script_path, script_linked_service=None, description=None, depends_on=None, linked_service_name=None, policy=None, degree_of_parallelism=None, priority=None, parameters=None, version=None, compilation_mode=None):    
        act = DataLakeAnalyticsUSQLActivity(name, script_path, script_linked_service=script_linked_service, description=description, depends_on=depends_on, linked_service_name=linked_service_name, policy=policy, degree_of_parallelism=degree_of_parallelism, priority=priority, parameters=parameters, runtime_version=version, compilation_mode=compilation_mode)
        if act.linked_service_name == None:
            if self.default_ls_resolver != None:
                act.linked_service_name = self.default_ls_resolver(act)
            else:
                pass
                #todo throw exception as can't resolve linked service
        if act.script_linked_service == None:
            if self.default_ls_resolver != None:
                # usql scripts must be stored in a blob account
                act.script_linked_service = self.default_ls_resolver('AzureBlob')
            else:
                #todo throw exception as can't resolve linked service
                pass
        self._set_act_dependancy(act, depends_on, dependancy_type)
        return act        

    def get_metadata(self, name, dataset, description=None, depends_on=None, policy=None, field_list=None):
        #if not isinstance(dataset, AzureBlobDataset):
            #todo: throw error as only blob is supported 
        act = GetMetadataActivity(name, DatasetReference(dataset.name), description=description, field_list=None, depends_on=depends_on, linked_service_name=None)
        if act.linked_service_name == None:
            if self.default_ls_resolver != None:
                act.linked_service_name = self.default_ls_resolver(act)
            else:
                #todo throw exception as can't resolve linked service
                pass
        self._set_act_dependancy(act, depends_on, dependancy_type)
        return act        

    def script(self, name, command, description=None, depends_on=None, linked_service_name=None, policy=None, resource_linked_service=None, folder_path=None, reference_objects=None, extended_properties=None):
        act = CustomActivity(name, command, description=description, depends_on=depends_on, linked_service_name=linked_service_name, policy=policy, resource_linked_service=resource_linked_service, folder_path=folder_path, reference_objects=reference_objects, extended_properties=extended_properties)
        #todo: attempt to resolve linked services if they are not set, also can signature be simplified
        self._set_act_dependancy(act, depends_on, dependancy_type)
        return act        

    def lookup(self, name, source, dataset, description=None, depends_on=None, dependancy_type='Success'):
        act = LookupActivity(name, source, DatasetReference(dataset.name), description=description, depends_on=depends_on, dependancy_type=dependancy_type)
        self._set_act_dependancy(act, depends_on, dependancy_type)
        return act

    def pyspark(self, name, root_path, script_path, depends_on=None, linked_service_name=None, dependancy_type='Success'):
        act = HDInsightSparkActivity(name, linked_service_name=linked_service_name, root_path=root_path, entry_file_path=script_path)
        if act.linked_service_name == None:
            if self.default_ls_resolver != None:
                act.linked_service_name = self.default_ls_resolver(act)
            else:
                #todo throw exception as can't resolve linked service
                pass
        self._set_act_dependancy(act, depends_on, dependancy_type)
        return act

    #todo: add hdi hive, pig and mapreduce activities 

    def sqlsvr_storedproc(self, name, stored_proc_name, stored_proc_params=None, Description=None, depends_on=None, dependancy_type='Success'):
        act = SqlServerStoredProcedureActivity(name, stored_proc_name, description=description, stored_procedure_parameters=stored_proc_params)
        self._set_act_dependancy(act, depends_on, dependancy_type)
        return act

    def web(self, name, method, url, body, depends_on=None, dependancy_type='Success'):
        act = WebActivity(name, method=method, url=url, body=body)
        self._set_act_dependancy(act, depends_on, dependancy_type)
        return act

    def on_complete(self, pipeline, parameters=None):
        #once activities in this pipeline complete, exec this pipeline
        self.on_complete_pl = pipeline
        if isinstance(pipeline, PipelineBuilder):
            pipeline = pipeline.pipeline
        self.exec_pipeline("OnCompletion", pipeline, parameters=parameters)

    def _set_act_dependancy(self, act, depends_on, dependency_type):
        if depends_on is None:
            depends_on = self.anchor_activity
        self.set_act_dependancy(act, depends_on, dependency_type)
        self.activities.append(act)
    
    def set_act_dependancy(self, act, depends_on_act, dependancy_type):
        # first activity in pipeline
        if self.anchor_activity == None:
            self.anchor_activity = act
            return

        # set predecessor activity       
        if depends_on_act == None or dependancy_type == None:
            return
        if dependancy_type == 'Success':
            self.on_success(depends_on_act, act)
        elif dependancy_type == 'Failed':
            self.on_fail(depends_on_act, act)            

    def on_success(self, first_activity, second_activity):
        ad = ActivityDependency(first_activity.name, ['Succeeded'])
        if second_activity.depends_on == None:
            second_activity.depends_on = [ad]
        else:
            second_activity.depends_on.append(ad)
        
    def on_fail(self, first_activity, second_activity):
        ad = ActivityDependency(first_activity.name, ['Failed'])
        if second_activity.depends_on == None:
            second_activity.depends_on = [ad]
        else:
            second_activity.depends_on.append(ad)
    
    @property
    def pipeline(self):
        
        return Pipeline(self.name, PipelineResource(activities=self.activities, parameters=self.params))


    @property
    def linked_svc_resolver(self):
        return self.default_ls_resolver

    @linked_svc_resolver.setter
    def linked_svc_resolver(self, func):
        self.default_ls_resolver = func 


class Pipeline:

    def __init__(self, name, pipeline_ref):
        self.name = name
        self.pipeline_ref = pipeline_ref