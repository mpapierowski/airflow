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

import os
from collections.abc import Sequence
from pathlib import Path

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class PipelinesBundle(BaseDagBundle):
    """
    Pipelines DAG bundle - exposes BQ Pipelines as Airflow DAGs.



    :param gcp_conn_id: Airflow connection ID for GCP.
    :param bucket_name: The name of the GCS bucket containing the DAG files.
    :param prefix:  Optional subdirectory within the GCS bucket where the DAGs are stored.
                    If None, DAGs are assumed to be at the root of the bucket (Optional).
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        gcp_conn_id: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id or GoogleBaseHook.default_conn_name
        self.impersonation_chain = impersonation_chain
        self.project_id = project_id
        self.location = location
        self.dag_importer_class = "airflow.providers.google.cloud.importers.dataform.PipelinesDagImporter"

    def initialize(self) -> None:
        with self.lock():
            self.path.mkdir(parents=True, exist_ok=True)
        super().initialize()

    @property
    def path(self) -> Path:
        return self.base_dir.joinpath(f"projects/{self.project_id}/locations/{self.location}")

    def refresh(self) -> None:
        pass

    def get_current_version(self) -> str | None:
        return None
