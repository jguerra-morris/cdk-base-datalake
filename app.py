#!/usr/bin/env python3
import json
import aws_cdk as cdk
from utils import create_name

from cdk_base_datalake.networking_stack import NetworkingStack
from cdk_base_datalake.data_consume_stack import DataConsumeStack
from cdk_base_datalake.data_storage_layer import DataStorageLayerStack
from cdk_base_datalake.ingestion_stack import IngestionStack

app = cdk.App()

# Get context variable "env" (default to "dev")
environment = app.node.try_get_context("env") or "dev"

# Load JSON vars file for this environment
with open(f"vars/{environment}.json") as f:
    vars_config = json.load(f)

project = vars_config["project"]
region = vars_config["region"]

# Store globals in context so all stacks can use them
app.node.set_context("project", project)
app.node.set_context("region", region)
app.node.set_context("env", environment)

network_stack = NetworkingStack(app, create_name(app, "stack", "networking"))
data_consume_stack = DataConsumeStack(app, create_name(app, "stack", "data-consume"), network_stack.vpc)
data_storage_stack = DataStorageLayerStack(app, create_name(app, "stack", "data-storage"))
IngestionStack(app, create_name(app, "stack", "ingestion"), data_storage_stack.scripts_bucket, data_storage_stack.raw_bucket)

app.synth()
