from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from factoryhelper import *



def set_linked_services(ls_coll):
    #Define Blob Storage Linked Service
    storage_linkedservice_name = 'StorageLinkedService'
    storage_string = SecureString('<TODO: your Azure blob storage connection string here>')
    ls = AzureStorageLinkedService(connection_string=storage_string)
    ls_coll[ls.type] = (storage_linkedservice_name, ls)

    #Create Amazon Access Key Linked Service
    amazons3_linkedservice_name = 'DemoAwsAccessKeyLinkedService' 
    amazons3_accesskeyid = '<TODO: your s3 key ID here>'
    amazons3_secretaccesskey = SecureString('<TODO: your s3 access key here>')
    ls = AmazonS3LinkedService(access_key_id=amazons3_accesskeyid, secret_access_key=amazons3_secretaccesskey)
    ls_coll[ls.type] = (amazons3_linkedservice_name, ls)

    #Create HdInsight Linked Service
    hdi_linkedservice_name = 'HDILinkedService'
    hdi_user = '<TODO: your HDI cluster user name here>'
    hdi_password = SecureString('<TODO: your ADF cluster password here>' )
    hdi_uri = 'https://<TODO: your HDInsight Cluster Name here>.azurehdinsight.net/'
    hdi_s_connection = SecureString('<TODO: connection string to Azure blob storage account to associate with the HDInsight cluster>')
    hdinsight_linkedservice_reference = LinkedServiceReference(storage_linkedservice_name)
    ls = HDInsightLinkedService(cluster_uri=hdi_uri, user_name=hdi_user, password=hdi_password, linked_service_name=hdinsight_linkedservice_reference)
    ls_coll[ls.type] = (hdi_linkedservice_name, ls)

    return ls_coll

def GetClientContext():
    subscription_id = '<TODO: azure subscription id>'
    credentials = ServicePrincipalCredentials(
                    client_id='<TODO: client id>',
                    secret='<TODO: secret>',
                    tenant='<TODO: tenant id>')
    ctx = ClientContext(subscription_id, credentials)
    return ctx
       

df = DataFactory('mflaskodemo1', 'mflaskodemo', GetClientContext())
df.create()

# default linked services
ls_coll = set_linked_services(LinkedServiceCollection())
df.upcreate_linkedservices(ls_coll)  # deploy linked svcs to factory
df.linked_svc_resolver = ls_coll.create_linked_svc_ref  # use LS collection method to auto resolve LS bindings

#build parameterized pipeline
pb = PipelineBuilder('DemoPipeline',
                        {'s3Key' : ParameterSpecification('String'),
                            's3BucketName' : ParameterSpecification('String'),
                            'blobContainer': ParameterSpecification('String'),
                            'blobFile' : ParameterSpecification('String'),
                            'mailTo': ParameterSpecification('String'),
                            'successEmailUrl': ParameterSpecification('String'),
                            'errorEmailUrl': ParameterSpecification('String'), 
                            'pysparkRoot' : ParameterSpecification('String'),
                            'pysparkFile' : ParameterSpecification('String')},
                            ls_coll.create_linked_svc_ref)  # use LS collection method to auto resolve LS bindings

# create data sets
s3_dataset = df.upcreate_dataset('amazonS3Dataset', 
                                    AmazonS3Dataset(None, 
                                        bucket_name='@{pipeline().parameters.s3BucketName}', 
                                        key='@{pipeline().parameters.s3Key}'))
blob_dataset = df.upcreate_dataset('SinkBlobDataset', 
                                    AzureBlobDataset(None, 
                                        folder_path=Expression('@{pipeline().parameters.blobContainer}'),
                                        file_name='@{pipeline().parameters.blobFile}'))

# copy s3 to blob
copy_act = pb.copy('CopyS3toBlob', s3_dataset, blob_dataset)
pb.anchor_activity = copy_act

# send emails when copy complete
pb.web('SendCopyEmailActivity', method='POST',
                url='@{pipeline().parameters.successEmailUrl}',
                body={'dataFactoryName': '@{pipeline().DataFactory}',
                        'message': '@{activity(\'CopyS3toBlob\').output.throughput}',
                        'pipelineName': '@{pipeline().Pipeline}',
                        'receiver':'@{pipeline().parameters.mailTo}'}
)
# send email if copy fails
pb.web('SendFailEmailActivity', method='POST',
                    url='@{pipeline().parameters.errorEmailUrl}',
                    body={'dataFactoryName': '@{pipeline().DataFactory}',
                            'message': '@{activity(\'CopyS3toBlob\').error.message}',
                            'pipelineName': '@{pipeline().Pipeline}',
                            'receiver':'@{pipeline().parameters.mailTo}'},
                    dependancy_type='Failed')

# transform w/ spark when copy complete
pb.pyspark('SparkActivity', root_path='@{pipeline().parameters.pysparkRoot}', 
                            script_path='@{pipeline().parameters.pysparkFile}')

# deploy pipeline to data factory
pl = pb.pipeline
df.upcreate_pipeline(pl)

# Execute the pipeline (aka. create a Pipeline Run)
pl_run = df.run_pipeline(pl.name, {'s3Key' : '<TODO: s3 bucket key, example: data/partitioneddata/year=2017/month=8',
                                's3BucketName' : '<TODO: s3 bucket name>',
                                'blobContainer': '<TODO: Azure blob storage container name>',
                                'blobFile' : "<TODO: file name>",
                                'mailTo' : '<TODO: email address to send copy success/failure email to>',
                                'successEmailUrl': '<TODO: URL to a Logic App instance to send on data copy success emails>',
                                'errorEmailUrl': '<TODO: URL to a Logic App instance to send on data copy error emails>',
                                'pysparkRoot' : '<TODO: Azure blob path to a folder where the pyspark file is located, example: adfspark\\pyFiles>',
                                'pysparkFile' : '<TODO: pyspark file name, example: processdata.py'})

# print pipeline status until done
df.watch_pipeline_run(pl_run)

print('All Done :)')





    