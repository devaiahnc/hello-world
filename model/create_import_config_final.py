import sys, os
import requests
import time
import datetime as dt
sys.path.append('/opt/infoworks/apricot-meteor/infoworks_python')
from infoworks.sdk import client
import json

server_ip='172.30.1.130'
print(server_ip)
server_port=2999
print(server_port)
base_url = 'http://{server_ip}:{server_port}'.format(server_ip=server_ip, server_port=server_port)
print(base_url)
auth_token="AU66mMKxL9fEwHEUe9cMnlK9RTDNLkIfp7vcYdT9cSs="
# Args
domain_name=str(sys.argv[1])
pipeline_name=str(sys.argv[2])
domain_id=str(sys.argv[3])

query=open(pipeline_name+'.sql')
json_file = open(pipeline_name+'.json')

sdk_client = client.InfoworksClientSDK()
sdk_client.initialize_client_with_user(protocol='http', ip=server_ip, port='2999', auth_token_raw=auth_token)



def create_pipeline(pipeline_name):
        #parameters to be used in the rest Api call
        params_create_pipeline={"auth_token":auth_token}
        data_create_pipeline={ "domain_name" :domain_name,"description": "Pipeline created for testing","domainId": {"$type": "oid","$value":domain_id},"isAdvancedPipeline": False,"name": pipeline_name,"batch_engine": "HIVE"}
        #Making the Rest API call and returning the response
        create_pipeline_response=requests.post('{base_url}/v1.1/pipeline/create.json'.format(base_url=base_url), params=params_create_pipeline,data=json.dumps(data_create_pipeline))
        return (create_pipeline_response)
	print("Pipeline is created")

def get_pipelineid(pipeline_name):
        #parameters to be used in the rest Api call
        params_get_pipelineid={'entity_name':pipeline_name,'entity_type':'pipeline','parent_entity_name':domain_name,'parent_entity_type':'domain','auth_token':auth_token}

        #Making the Rest API call and storing the response
        get_pipelineid_response=requests.get('{base_url}/v1.1/entity/id.json'.format(base_url=base_url), params=params_get_pipelineid )
        get_pipelineid_response_config = json.loads(get_pipelineid_response.text)
        pipelineid = get_pipelineid_response_config['result']['entity_id']['$value']
        return (pipelineid)

def import_sql(query):
        #parameters to be used in the rest Api call
        params_import_sql={'pipeline_id':pipelineid,'auth_token':auth_token}
        data_import_sql={"query":query.read()}
        #Making the Rest API call and storing the response
        data_import_sql_response=requests.post('{base_url}/v1.1/pipeline/sql_import.json'.format(base_url=base_url), params=params_import_sql,data=json.dumps(data_import_sql))
        import_sql_response_config=json.loads(data_import_sql_response.text)


def configure_pipeline(pipeline_name):
        params_configure_pipeline_stage1={'pipeline_id':pipelineid,'auth_token':auth_token}
        data_configure_pipeline_stage1='{"import_configuration": {"quoted_identifier": "DOUBLE_QUOTE", "sql_dialect": "LENIENT"}}'
        #Making the Rest API call and storing the response
        data_configure_pipeline_response_stage1=requests.post('{base_url}/v1.1/pipeline/sql_import/configure.json'.format(base_url=base_url), params=params_configure_pipeline_stage1,data=data_configure_pipeline_stage1)

        get_pipeline_configs_stage1=json.loads(data_configure_pipeline_response_stage1.text)
        stage1_configs=get_pipeline_configs_stage1['result']['import_response']['mappings']
	if any("to_entity_id" in d for d in stage1_configs):
	    print("configuring pipeline..")
	else:
	    print json.dumps(stage1_configs)
	    print("to_entity_id not found for the schema provided")
	    exit()

	data_configure_pipeline_stage2={"import_configuration": { "quoted_identifier": "DOUBLE_QUOTE", "sql_dialect": "LENIENT" }, "mappings": stage1_configs,"pipeline": {"pipeline_parameters": []}}
	#Making the Rest API call and storing the response
        data_configure_pipeline_response_stage2=requests.post('{base_url}/v1.1/pipeline/sql_import/configure.json'.format(base_url=base_url), params=params_configure_pipeline_stage1,data=json.dumps(data_configure_pipeline_stage2))
	

def get_target_config(pipeline_name):
#       parameters to be used in the rest Api call
        params_get_target_config={"entity_id":pipelineid,"entity_type":"pipeline","auth_token":auth_token}
        #Making the Rest API call and storing the response
        get_target_config_response=requests.get('{base_url}/v1.1/entity/configuration.json'.format(base_url=base_url), params=params_get_target_config)
        get_target_config_response_config = json.loads(get_target_config_response.text)
        target_config = get_target_config_response_config['result']['configuration']
	#print target_config
        return (target_config)

def configure_target(configs):
        params_configure_target={"pipeline_id":pipelineid,"auth_token":auth_token}
        data_configure_target=configs
#       Making the Rest API call and storing the response
        get_configure_target_response=requests.post('{base_url}/v1.1/pipeline/configure.json'.format(base_url=base_url), params=params_configure_target,data=json.dumps(data_configure_target))
	print get_configure_target_response.text

create_pipeline(pipeline_name)
pipelineid=get_pipelineid(pipeline_name)
import_sql(query)
configure_pipeline(pipeline_name)

configs=get_target_config(pipeline_name)

for x in configs['pipeline']['model']['nodes'].items():
	if x[1]['type']=="TARGET":
		x[1]['properties'] = json.load(json_file)
#print configs['configuration']
for key in configs['iw_mappings']:
       entity_id = key["entity_id"]
       key["to_entity_id"] = entity_id
       #print key
configure_target(configs)

