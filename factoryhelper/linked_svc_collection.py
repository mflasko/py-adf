from azure.mgmt.datafactory.models import *

class LinkedServiceCollection(dict):

    # dict of default linked services 
    # ['<LS type name>'] -> ('<LS Name>', LS Object)
    
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    def __setitem__(self, key, val):
        # todo: check val = ('<LS Name>', LS Object) 
        dict.__setitem__(self, key, val)

    def update(self, *args, **kwargs):
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def set_linked_svc(self, name, linked_svc):
        self[linked_svc.type] = (name, linked_svc)

    def get_linked_svc(self, name):
        for key in self:
            ls_name, ls = self[key]
            if ls_name == name:
                return ls 
        return None

    def create_linked_svc_ref(self, obj):
        # obj is dataset name, activity name, dataset instance or activity instance
        ls_type = LinkedServiceCollection.get_linked_svc_type(obj)
        return LinkedServiceReference(self[ls_type][0])

    def get_linked_svc_type(obj):
        if isinstance(obj, Dataset):
            return LinkedServiceCollection.ds_type_to_ls_type[obj.type]
        elif isinstance(obj, Activity):
            return LinkedServiceCollection.act_type_to_ls_type[obj.type]
        elif isinstance(obj, str):
            type_name = ds_type_to_ls_type[obj]
            if type_name == None:
                return act_type_to_ls_type[obj]
        
        #todo throw type error
        return None

    # data set type to linked service type map 
    ds_type_to_ls_type = dict()
    ds_type_to_ls_type['AmazonS3Object'] = 'AmazonS3'
    ds_type_to_ls_type['AzureBlob'] = 'AzureStorage'
    ds_type_to_ls_type['AzureSqlDWTable'] = 'AzureSqlDW'

    # activity type to linked service type map
    act_type_to_ls_type = dict()
    act_type_to_ls_type['HDInsightSpark'] = 'HDInsight'
    act_type_to_ls_type['WebActivity'] = None #no LS needed to execute this activity
    act_type_to_ls_type['GetMetadata'] = None #no LS needed to execute this activity
    act_type_to_ls_type['DataLakeAnalyticsU-SQL'] = 'AzureDataLakeAnalytics'
    
