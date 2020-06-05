import os
import logging
import requests
from time import sleep

logging.basicConfig(level=logging.INFO)

endpoint = os.environ.get("CONNECTORENDPOINT", "http://127.0.0.1")
port = os.environ.get("CONNECTORPORT", 8083)


connectorUrl = "{}:{}".format(endpoint, port)

healthy = ["RUNNING"]


def get_connector_names(theUrl):
    response = requests.get("{}/connectors".format(theUrl))
    response_json = response.json()
    return response_json


def get_connectors_health(connect_url, connector_names):
    statuses = []
    for connector_name in connector_names:
        statuses.append(get_connector_health(connect_url, connector_name))
    return statuses


def get_connector_health(connect_url, connectorName):
    connector_status = get_connector_status(connect_url, connectorName)
    connector_state = connector_status["connector"]["state"].upper()
    connector_worker = connector_status["connector"]["worker_id"]
    return {
        "name": connectorName,
        "state": connector_state,
        "worker_id": connector_worker,
        "tasks": connector_status["tasks"]
    }


def get_connector_status(connect_url, connector_name):
    response = requests.get(
        "{}/connectors/{}/status".format(connect_url, connector_name))
    response_json = response.json()
    return response_json


def get_unhealthy_connectors(connect_url):
    # Get unhealthy connectors
    unhealthy = []
    try:
        connectorsHealth = get_connectors_health(
            connectorUrl, get_connector_names(connectorUrl))
        for connector in connectorsHealth:
            if connector["state"]not in healthy:
                unhealthy.append(connector)
    except Exception as error:
        logging.info(
            "Exception happened during in the python monitoring script " + error)
    return unhealthy


def restart_connector(connect_url, connector_name):
    logging.info("Connector '{}' is being resatrted :".format(connector_name))
    try:
        requests.post(
            "{}/connectors/{}/restart".format(connect_url, connector_name))
    except Exception as error:
        logging.info(
            "Exception happened during in the python monitoring script " + error)


def resume_connector(connect_url, connector_name):
    logging.info("Connector '{}' is being resumed :".format(connector_name))
    try:
        requests.put(
            "{}/connectors/{}/resume".format(connect_url, connector_name))
    except Exception as error:
        logging.info(
            "Exception happened during in the python monitoring script " + error)


print("Here the list of connectors")


# Get the unhealthy connectors
while(True):
    unhealthy = get_unhealthy_connectors(connectorUrl)
    if not unhealthy:
        logging.info("All the connectors are healthy.")
    else:
        for x in unhealthy:
            if x["state"].lower() == "PAUSED".lower():
                resume_connector(connectorUrl, x["name"])
            elif x["state"].lower() == "FAILED".lower():
                restart_connector(connectorUrl, x["name"])
            else:
                logging.info(
                    "Connector '{}' was unassigned.".format(x["name"]))
    sleep(4)
