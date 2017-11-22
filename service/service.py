import logging
import os
import sys
import json
import requests
from urllib.parse import urljoin
from time import sleep
import sesamclient
from copy import copy
from random import randint

from sesamclient import entity_json

logger = None
overwrite_systems = False
overwrite_pipes = False
delete_pipes = True
update_interval = 1800


def get_target_pipes(node):
    pipes = {}


    for pipe in node["api_connection"].get_pipes():
        if pipe.id.startswith("system:"):
            continue

        effective_config = pipe.config.get("effective")
        if effective_config:
            sink = effective_config.get("sink")

            if sink:
                system_id = sink["system"]

                if sink["type"] != "dataset":
                    pipes[pipe.id] = system_id

    return pipes

def get_source_pipes(node):
    pipes = {}


    for pipe in node["api_connection"].get_pipes():
        if pipe.id.startswith("system:"):
            continue

        effective_config = pipe.config.get("effective")
        if effective_config:
            source = effective_config.get("source")

            if source:
                system_id = source["system"]

                if source["type"] not in ["dataset","merge","union","merge_datasets", "diff_datasets", "embedded"]:
                    pipes[pipe.id] = system_id

    return pipes


def stop_and_disable_pipes(pipes):
    for pipe in pipes:
        pump = pipe.get_pump()
        # Stop the pipe
        if "stop" in pump.supported_operations:
            pump.stop()

        if "disable" in pump.supported_operations:
            pump.disable()

def get_staged_entities(api_connection, pipe, stage="sink"):

    result = api_connection.session.get(api_connection.sesamapi_base_url + "/pipes/%s/entities?stage=%s" % (pipe.id, stage))
    if result.status_code != 200:
        result.raise_for_status()
    return iter(result.json())

def get_testentities(master_node, dataset_dep, pipe_dep, datasets, pipes, datasource, source_entity_id):
    pipe_entities = None

    #logger.info("Find entity '%s' in dataset '%s'..." % (source_entity_id, datasource))
    entity = datasource.get_entity(source_entity_id)

    if entity and datasource.id in dataset_dep:
        pipe = next((x for x in pipes if x.id == dataset_dep[datasource.id]), None)
        if pipe.config['effective']['source']["type"] not in ["dataset","merge"]:
            logger.info("Found source entity '%s' in dataset '%s'..." % (entity["_id"], datasource))
            id = entity["_id"].split(":")[-1]
            new_entity = [pipe.id, entity]
            for pipe_entity in get_staged_entities(master_node["api_connection"],pipe, stage="before-transforms"):
                if "_id" in pipe_entity:
                    if pipe_entity["_id"] == id:
                        new_entity = [[pipe.id, pipe_entity]]
                        break
                else:
                    logger.warning("Find entity '%s' in dataset '%s'..." % (source_entity_id, datasource))
        else:
            for dataset_id in pipe_dep[pipe.id]:
                dataset = next((x for x in datasets if x.id == dataset_id), None)
                find_entities = [entity["_id"]]
                if "$ids" in entity:
                    for entity_id in entity["$ids"]:
                        if entity_id.replace("~:", "") not in find_entities:
                            find_entities.append(entity_id.replace("~:", ""))
                for entity_id in find_entities:
                    testentity = get_testentities(master_node, dataset_dep, pipe_dep, datasets, pipes, dataset, entity_id)
                    if testentity:
                        if not pipe_entities:
                            pipe_entities = testentity
                        else:
                            pipe_entities.extend(testentity)

    return pipe_entities

def generate_testdata(master_node, target_node):

    logger.info("Generate testdata...")

    systems = master_node["api_connection"].get_systems()
    pipes = master_node["api_connection"].get_pipes()
    datasets = master_node["api_connection"].get_datasets()

    dataset_dep = {}
    for pipe in pipes:
        sink = pipe.config['effective']['sink']
        if sink["type"] == "dataset":
            if sink["dataset"] not in dataset_dep:
                dataset_dep[sink["dataset"]] = pipe.id

    pipe_dep = {}
    for pipe in pipes:
        source = pipe.config['effective']['source']
        pipe_dep[pipe.id] = []
        if "dataset" in source:
            dataset = next((x for x in datasets if x.id == source["dataset"]), None)
            if dataset:
                pipe_dep[pipe.id].append(dataset.id)

        if "datasets" in source:
            for source in source["datasets"]:
                dataset = next((x for x in datasets if x.id == source.split(" ")[0]), None)
                if dataset:
                    pipe_dep[pipe.id].append(dataset.id)



    master_pipes = get_target_pipes(master_node)
    source_pipes = get_source_pipes(master_node)


    test_data = []

    for pipe_id in master_pipes:
        pipe = next((x for x in pipes if x.id == pipe_id), None)
        logger.info("Processing pipe '%s'.." % pipe.id)
        pipe_entities = [] 

        for dataset_id in pipe_dep[pipe.id]:
            dataset = next((x for x in datasets if x.id == dataset_id), None)

            testentities = dataset.get_entities(limit=10000)

            for entity in testentities:
                if "_deleted" in entity and not entity["_deleted"]:
                    testentity = get_testentities(master_node, dataset_dep, pipe_dep, datasets, pipes, dataset, entity["_id"])
                    if testentity:
                        pipe_entities.extend(testentity)
                        if len(pipe_entities) > 9:
                            break
        if len(pipe_entities) > 0:
            test_data.extend(pipe_entities)


    for pipe in pipes:
        indexes = { i for i, x in enumerate(test_data) if x[0] == pipe.id }
        if indexes:
            target_pipe = target_node["api_connection"].get_pipe(pipe.id)
            config = pipe.config.get("original")
            source = config["source"]
            entities = []
            for index in indexes:
                entities.extend([test_data[index][1]])
            json_ent = entity_json.entities_to_json(entities)
            new_source = {
                            "type": "conditional",
                            "alternatives": {
                              "prod": source,
                              "test": {
                                "type": "embedded",
                                "entities": json.loads(json_ent)
                              }
                            },
                            "condition": "$ENV(node-env)"
                          }

            config["source"] = new_source
            try:
                target_pipe.modify(config, force=False)
            except BaseException as e:
                logger.exception("Error: %s with config:  %s" % (e, config))



def copy_environment_variables(master_node, slave_nodes):
    try:
        env_vars = master_node["api_connection"].get_env_vars()

        if env_vars:
            for slave_node in slave_nodes:
                slave_env_vars = slave_node["api_connection"].get_env_vars()
                if slave_env_vars != env_vars:
                    logger.info("Master and slave env vars are different - copying env vars from master "
                                "to slave node %s" % slave_node["_id"])
                    slave_node["api_connection"].post_env_vars(env_vars)
    except BaseException as e:
        logger.exception("Copying env vars from master to slave node failed. Make sure the JWT tokens used "
                         "are issued to 'group:Admin'!")


def assert_same_secret_keys(master_node, slave_nodes):

    master_secret_keys = set(master_node["api_connection"].get_secrets())
    logger.debug("master secrets: %s" % master_secret_keys)

    can_run = True
    for slave_node in slave_nodes:
        slave_secret_keys = set(slave_node["api_connection"].get_secrets())
        logger.debug("Slave '%s' secrets: %s" % (slave_node["_id"], slave_secret_keys))
        if not master_secret_keys.issubset(slave_secret_keys):
            logger.error("Slave node '%s' is missing secret keys from master!" % slave_node["_id"])
            can_run = False

    if not can_run:
        logger.warning("Master and slave secrets mismatch!")
        #logger.error("Master and slave secrets mismatch, exiting")
        #sys.exit(1)


if __name__ == '__main__':
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger('orchestrator-service')

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    logger.setLevel(logging.DEBUG)

    # Read config from env vars

    if "MASTER_NODE" not in os.environ:
        logger.error("MASTER_NODE configuration missing!")
        master_node = {
            "_id": "m1",
            "endpoint": "https://sesam.bouvet.no/node",
            "jwt_token": "",
            "use_binary_source": True
        }
    else:
        master_node = json.loads(os.environ["MASTER_NODE"])

    if "TARGET_NODE" not in os.environ:
        logger.error("TERGET_NODE configuration missing!")
        target_node = {
            "_id": "t1",
            "endpoint": "https://601248e4.sesam.cloud",
            "jwt_token": "",
            "use_binary_source": True
        }
    else:
        master_node = json.loads(os.environ["TARGET_NODE"])


    if not master_node["endpoint"].endswith("/"):
        master_node["endpoint"] += "/"

    logger.info("Master API endpoint is: %s" % master_node["endpoint"] + "api")

    master_node["api_connection"] = sesamclient.Connection(sesamapi_base_url=master_node["endpoint"] + "api",
                                                           jwt_auth_token=master_node["jwt_token"],
                                                           timeout=60*10)

    if not target_node["endpoint"].endswith("/"):
        target_node["endpoint"] += "/"

    logger.info("Target API endpoint is: %s" % master_node["endpoint"] + "api")

    target_node["api_connection"] = sesamclient.Connection(sesamapi_base_url=target_node["endpoint"] + "api",
                                                           jwt_auth_token=target_node["jwt_token"],
                                                           timeout=60*10)

    logger.info("Duplicate master and target...")
    config = master_node["api_connection"].get_config()
    #target_node["api_connection"].upload_config(config)
    #copy_environment_variables(master_node, target_node)
    #stop_and_disable_pipes(target_node["api_connection"].get_pipes())
    vars = master_node["api_connection"].get_env_vars()
    vars.update({"node-env": "test"})
    target_node["api_connection"].put_env_vars(vars)

    generate_testdata(master_node, target_node)


