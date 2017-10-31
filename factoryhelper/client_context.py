from azure.common.credentials import ServicePrincipalCredentials

class ClientContext:    
    def __init__(self, subscription_id, service_principle):
        self.subscription_id = subscription_id
        self.credentials = service_principle