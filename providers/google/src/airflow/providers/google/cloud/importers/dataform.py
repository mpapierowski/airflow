# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import re

from google.api_core.exceptions import NotFound

from airflow.dag_processing.importers.dag_importer import DagImporter, DagsImportResult
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataform import DataformHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.helpers import validate_key


_COMPILATION_RESULT_REGEX = r"projects/([^/]+)/locations/([^/]+)/repositories/([^/]+)/compilationResults/([^/]+)"
_REPOSITORY_REGEX = r"projects/([^/]+)/locations/([^/]+)/repositories/([^/]+)"
_LOCATION_REGEX = r"projects/([^/]+)/locations/([^/]+)"


def _get_action_name(action):
    try:
        validate_key(action.target.name)
    except AirflowException:
        return action.file_path.split("/")[-1].split(".")[0]
    return action.target.name


def _create_actions_dag(project_id: str, repository_id: str, actions):
    from airflow.sdk import DAG
    
    dag = DAG(dag_id=repository_id, schedule=None)
    tasks = {}
    action_to_task_name = {}
    for action in actions:
        task_name = _get_action_name(action)
        action_to_task_name[action.target.name] = task_name
        task = None
        if action.operations.queries:
            task = BigQueryInsertJobOperator(dag=dag, task_id=task_name, project_id=project_id,
            configuration={
                "query": {
                    "query": action.operations.queries[0],
                    "useLegacySql": False,
                    }
                })
        elif action.notebook:
            task = BashOperator(dag=dag, task_id=task_name, bash_command="echo 'not implemented yet!'")
        else:
            raise NotImplementedError(f"Unsupported action: {action}")
        tasks[task_name] = task
    for action in actions:
        for dependency in action.operations.dependency_targets:
            tasks[action_to_task_name[dependency.name]] >> tasks[action_to_task_name[action.target.name]]
    return dag



class PipelinesDagImporter(DagImporter):
    def __init__(self):
        self._hook: DataformHook | None = None

    def import_path(self, dagpath: str, options: ImportOptions | None = None) -> Generator[DagsImportResult, None, None]:
        try:
            workspace_id = "default"
            project_id, region, repository_id = re.search(_REPOSITORY_REGEX, dagpath).groups()
            workspace_name = f"projects/{project_id}/locations/{region}/repositories/{repository_id}/workspaces/{workspace_id}"
            compilation_result = self.hook().create_compilation_result(
                project_id=project_id,
                region=region,
                repository_id=repository_id, 
                compilation_result={"workspace": workspace_name})
            if compilation_result.compilation_errors:
                raise ValueError(compilation_result.compilation_errors)
            _, _, _, compilation_result_id = re.search(_COMPILATION_RESULT_REGEX, compilation_result.name).groups()
            actions = self.hook().query_compilation_result_actions(
                project_id=project_id,
                region=region,
                repository_id=repository_id,
                compilation_result_id=compilation_result_id
            )
            
            dag = _create_actions_dag(project_id, repository_id, actions.compilation_result_actions)
            dag.fileloc = dagpath
            return DagsImportResult(dags = {dagpath: dag},
                import_warnings = {},
                import_errors = {})
        except Exception as err:
            return DagsImportResult(dags = {},
                import_warnings = {},
                import_errors = {dagpath: str(err)})


    def list_paths(self, subpath: str) -> Iterable[str]:
        base = re.search(r"(.+)/projects", subpath).groups()[0]
        project_id, region = re.search(_LOCATION_REGEX, subpath).groups()

        page_result = self.hook().list_repositories(
            project_id=project_id, 
            region=region,
            repository_filter="labels:bigquery-workflow")
        
        if re.search(_REPOSITORY_REGEX, subpath):
            for repo in page_result.repositories:
                if re.search(_REPOSITORY_REGEX, subpath).group(3) == re.search(_REPOSITORY_REGEX, repo.name).group(3):
                    return ['/'.join([base, repo.name])]
            return []

        return ['/'.join([base, repo.name]) for repo in page_result.repositories]
        
    def dag_path_exists(self, dagpath: str) -> bool:
        project_id, region, repo = re.search(_REPOSITORY_REGEX, dagpath).groups()
        try:
            self.hook().get_repository(
                project_id=project_id, 
                region=region,
                repository_id=repo)
        except NotFound:
            return False
        return True
    
    def hook(self) -> DataformHook: 
        if not self._hook:
            self._hook = DataformHook()
        return self._hook
