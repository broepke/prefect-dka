# Prefect Workflow Orchestration

[Prefect](https://www.prefect.io) is an open-source **orchestration** tool for data engineering. It is a Python-based tool that allows you to **define**, **schedule**, and **monitor** your data pipelines. Prefect is a great tool for data engineers and data scientists who want to automate their **data pipelines**. 

In this project I have a collection of utilities for running prefect along with various *Flows*, or workflows. There are utilities for working with:

- Snowflake
- Slack
- Twilio
- Wikipedia

And more.  
 
![Prefect Workflow Orchestration](prefect.png)

## Prefect Managed Execution

Prefect Cloud can run your flows on your behalf with prefect:managed work pools. Flows run with this work pool do not require a worker or cloud provider account. Prefect handles the infrastructure and code execution for you.

[https://docs.prefect.io/latest/guides/managed-execution/](https://docs.prefect.io/latest/guides/managed-execution/)

## EC2 Execution

There is a script in the EC2 folder which can be used for _User Data_ in EC2's **Launch Template** settings.

## Docker

There is a Docker file in the repo but this isn't being used.  There was testing to work with ECS but never fully got it running.