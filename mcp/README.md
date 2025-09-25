# Demo MCP server

This directory contains a demo MCP server that integrates with a locally running event-based DAG processor API.

## Test prompts

Tested with Gemini CLI, with working directory set to the Airflow repo root:

> Generate an Airflow DAG that runs daily, checks the today's weather using an open API and saves reasponse to a file in the /tmp/weather directory, named after the run date. Store the DAG in the files/dags/weather_dag.py file. Then, validate, fix, and upload it, assuming the bundle is 'dags-folder' and it's based in the files/dags folder.

Also worked:

> Generate an Airflow DAG that runs daily, checks the tomorrow's weather using an open API and send an email to xyz@google.com if it's raining. Store the DAG in the files/dags/weather_2_dag.py file. Then, validate, fix, and upload it, assuming the bundle is 'dags-folder' and it's based in the files/dags folder.