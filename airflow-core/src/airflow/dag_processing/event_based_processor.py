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

from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.importers.dag_importer import DagsImportResult
from airflow.dag_processing.importers.local_python_importer import LocalPythonImporter
from airflow.models.dagbag import DagBag, sync_bag_to_db
from airflow.sdk.execution_time.task_runner import BaseDagBundle
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.stats import Stats
from airflow.utils.module_loading import import_string
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

if TYPE_CHECKING:
    from airflow.dag_processing.importers.dag_importer import DagImporter


class ParseRequest(BaseModel):
    bundle: str
    path: str | None = None


class ParseResponse(BaseModel):
    import_errors: dict[str, str] = {}
    import_warnings: dict[str, list[str]] = {}
    dags: dict[str, dict[Any, Any]] = {}


ParseAndUpdateRequest = ParseRequest


class ParseAndUpdateResponse(BaseModel):
    import_errors: dict[str, str] = {}
    dag_ids: list[str] = []


class EventBasedProcessor:
    """Implements the API for parsing and ingesting DAGs on demand."""

    def list_bundles(self) -> list[BaseDagBundle]:
        bundles = list(DagBundlesManager().get_all_dag_bundles())
        return bundles

    def parse_bundle(self, req: ParseRequest) -> ParseResponse:
        """Parses the bundle to"""
        bundle = _get_initialized_bundle(req.bundle)
        import_results = _get_import_results_from_bundle(bundle, req.path)

        resp = ParseResponse()
        for result in import_results:
            resp.import_errors.update(result.import_errors)
            for path, warnings in result.import_warnings:
                resp.import_warnings[path] = list(warnings)
            for dag_id, dag in result.dags.items():
                resp.dags[dag_id] = SerializedDAG.serialize_dag(dag)
        return resp

    def parse_and_update_bundle(self, req: ParseAndUpdateRequest) -> ParseAndUpdateResponse:
        bundle = _get_initialized_bundle(req.bundle)
        import_results = _get_import_results_from_bundle(bundle, req.path)

        dagbag = DagBag(collect_dags=False)
        for result in import_results:
            for dag in result.dags.values():
                dag.relative_fileloc = str(Path(dag.fileloc).relative_to(bundle.path))
                dagbag.bag_dag(dag)
        sync_bag_to_db(dagbag, bundle.name, bundle.version)

        resp = ParseAndUpdateResponse()
        for result in import_results:
            resp.import_errors.update(result.import_errors)
            resp.dag_ids.extend(result.dags.keys())
        return resp


def _get_initialized_bundle(bundle_name: str) -> BaseDagBundle:
    bundles = {bundle.name: bundle for bundle in DagBundlesManager().get_all_dag_bundles()}
    if bundle_name not in bundles:
        raise HTTPException(status_code=404, detail=f"Bundle '{bundle_name}' not found")
    bundle = bundles[bundle_name]
    bundle.initialize()
    return bundle


def _get_import_results_from_bundle(bundle: BaseDagBundle, subpath: str | None):
    scanned_path = bundle.path
    if subpath:
        scanned_path = scanned_path / subpath.removeprefix("/")
        # Do not jump out of the bundle directory for request paths like '../'.
        if not scanned_path.resolve().is_relative_to(bundle.path):
            raise HTTPException(
                status_code=400, detail=f"Invalid path: '{subpath}'; want path relative to bundle base path"
            )

    importer = (
        import_string(bundle.dag_importer_class)() if bundle.dag_importer_class else LocalPythonImporter()
    )
    import_results: list[DagsImportResult] = []
    for potential_dag_path in importer.list_paths(str(scanned_path)):
        if not importer.dag_path_exists(potential_dag_path):
            continue
        import_results.extend(importer.import_path(str(potential_dag_path)))
    return import_results


DagBundlesManager().sync_bundles_to_db()

handler = EventBasedProcessor()

app = FastAPI(
    title="Event-based ingester API",
    description="The API for validating and ingesting DAGs.",
    root_path="/bundles",
    version="2",
)


@app.get("")
def list_bundles():
    return handler.list_bundles()


@app.post("/parse")
def parse_bundle(req: ParseRequest) -> ParseResponse:
    return handler.parse_bundle(req)


@app.post("/parse_update")
def parse_and_update_bundle(req: ParseAndUpdateRequest) -> ParseAndUpdateResponse:
    return handler.parse_and_update_bundle(req)
