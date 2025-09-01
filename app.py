#!/usr/bin/env python3
import json
import aws_cdk as cdk
from utils import create_name

from cdk_base_datalake.networking_stack import NetworkingStack

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

NetworkingStack(app, create_name(app, "stack", "networking"))

app.synth()
