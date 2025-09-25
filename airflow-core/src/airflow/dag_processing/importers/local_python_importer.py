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

import contextlib
import importlib
import logging
import os
import signal
import sys
import traceback
from typing import Generator
import warnings
import zipfile
from datetime import datetime


from airflow import settings
from airflow.configuration import conf
from airflow.dag_processing.importers.dag_importer import DagImporter, DagsImportResult, ImportOptions
from airflow.exceptions import AirflowTaskTimeout
from airflow.utils.docs import get_docs_url
from airflow.utils.file import (
    correct_maybe_zipped,
    get_unique_dag_module_name,
    list_py_file_paths,
    might_contain_dag,
)
from airflow.utils.log.logging_mixin import LoggingMixin


@contextlib.contextmanager
def timeout(seconds=1, error_message="Timeout"):
    import logging

    log = logging.getLogger(__name__)
    error_message = error_message + ", PID: " + str(os.getpid())

    def handle_timeout(signum, frame):
        """Log information and raises AirflowTaskTimeout."""
        log.error("Process timed out, PID: %s", str(os.getpid()))
        raise AirflowTaskTimeout(error_message)

    try:
        try:
            signal.signal(signal.SIGALRM, handle_timeout)
            signal.setitimer(signal.ITIMER_REAL, seconds)
        except ValueError:
            log.warning("timeout can't be used in the current context", exc_info=True)
        yield
    finally:
        with contextlib.suppress(ValueError):
            signal.setitimer(signal.ITIMER_REAL, 0)


@contextlib.contextmanager
def _capture_with_reraise() -> Generator[list[warnings.WarningMessage], None, None]:
    """Capture warnings in context and re-raise it on exit from the context manager."""
    captured_warnings = []
    try:
        with warnings.catch_warnings(record=True) as captured_warnings:
            yield captured_warnings
    finally:
        if captured_warnings:
            for cw in captured_warnings:
                warnings.warn_explicit(
                    message=cw.message,
                    category=cw.category,
                    filename=cw.filename,
                    lineno=cw.lineno,
                    source=cw.source,
                )


class LocalPythonImporter(DagImporter, LoggingMixin):
    def __init__(self):
        super().__init__()
        self.safe_mode = conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE")
        self.dagbag_import_error_tracebacks = conf.getboolean("core", "dagbag_import_error_tracebacks")
        self.dagbag_import_error_traceback_depth = conf.getint("core", "dagbag_import_error_traceback_depth")
        # the file's last modified timestamp when we last read it
        self.file_last_changed: dict[str, datetime] = {}
        self.has_logged_skipped_files = False


    def import_path(self, dagpath: str, options: ImportOptions | None = None) -> Generator[DagsImportResult, None, None]:
        from airflow.sdk.definitions._internal.contextmanager import DagContext
        skipped_result = DagsImportResult(dags = {}, import_warnings = {}, import_errors = {}, skipped_paths=[dagpath])
        try:
            file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(dagpath))
            if (
                options and options.skip_unchanged
                and dagpath in self.file_last_changed
                and file_last_changed_on_disk == self.file_last_changed[dagpath]
            ):
                yield skipped_result
                return
        except Exception as e:
            self.log.exception(e)
            yield skipped_result
            return
        
        self.file_last_changed[dagpath] = file_last_changed_on_disk

        # Ensure we don't pick up anything else we didn't mean to
        DagContext.autoregistered_dags.clear()
        formatted_captured_warnings = {}
        import_errors = {}

        with _capture_with_reraise() as captured_warnings:
            if dagpath.endswith(".py") or not zipfile.is_zipfile(dagpath):
                mods = self._load_modules_from_file(dagpath, self.safe_mode, import_errors)
            else:
                mods = self._load_modules_from_zip(dagpath, self.safe_mode, import_errors)

        if captured_warnings:
            formatted_warnings = []
            for msg in captured_warnings:
                category = msg.category.__name__
                if (module := msg.category.__module__) != "builtins":
                    category = f"{module}.{category}"
                formatted_warnings.append(f"{msg.filename}:{msg.lineno}: {category}: {msg.message}")
            formatted_captured_warnings[dagpath] = tuple(formatted_warnings)

        found_dags = self._process_modules(mods)

        yield DagsImportResult(
            dags = {d.dag_id:d for d in found_dags},
            import_warnings = formatted_captured_warnings,
            import_errors = import_errors,
        )

    def list_paths(self, subpath: str):
        subpath = str(correct_maybe_zipped(subpath))
        return list_py_file_paths(subpath, safe_mode=self.safe_mode)

    def dag_path_exists(self, dagpath: str) -> bool:
        return os.path.isfile(dagpath)
    
    def modified_time(self, dagpath:str):
        return os.path.getmtime(dagpath)

    def _load_modules_from_file(self, filepath, safe_mode, import_errors):
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        def handler(signum, frame):
            """Handle SIGSEGV signal and let the user know that the import failed."""
            msg = f"Received SIGSEGV signal while processing {filepath}."
            self.log.error(msg)
            import_errors[filepath] = msg

        try:
            signal.signal(signal.SIGSEGV, handler)
        except ValueError:
            self.log.warning("SIGSEGV signal handler registration failed. Not in the main thread")

        if not might_contain_dag(filepath, safe_mode):
            # Don't want to spam user with skip messages
            if not self.has_logged_skipped_files:
                self.has_logged_skipped_files = True
                self.log.info("File %s assumed to contain no DAGs. Skipping.", filepath)
            return []

        self.log.debug("Importing %s", filepath)
        mod_name = get_unique_dag_module_name(filepath)

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        DagContext.current_autoregister_module_name = mod_name

        def parse(mod_name, filepath):
            try:
                loader = importlib.machinery.SourceFileLoader(mod_name, filepath)
                spec = importlib.util.spec_from_loader(mod_name, loader)
                new_module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = new_module
                loader.exec_module(new_module)
                return [new_module]
            except KeyboardInterrupt:
                # re-raise ctrl-c
                raise
            except BaseException as e:
                # Normally you shouldn't catch BaseException, but in this case we want to, as, pytest.skip
                # raises an exception which does not inherit from Exception, and we want to catch that here.
                # This would also catch `exit()` in a dag file
                DagContext.autoregistered_dags.clear()
                self.log.exception("Failed to import: %s", filepath)
                if self.dagbag_import_error_tracebacks:
                    import_errors[filepath] = traceback.format_exc(
                        limit=-self.dagbag_import_error_traceback_depth
                    )
                else:
                    import_errors[filepath] = str(e)
                return []

        dagbag_import_timeout = settings.get_dagbag_import_timeout(filepath)

        if not isinstance(dagbag_import_timeout, (int, float)):
            raise TypeError(
                f"Value ({dagbag_import_timeout}) from get_dagbag_import_timeout must be int or float"
            )

        if dagbag_import_timeout <= 0:  # no parsing timeout
            return parse(mod_name, filepath)

        timeout_msg = (
            f"DagBag import timeout for {filepath} after {dagbag_import_timeout}s.\n"
            "Please take a look at these docs to improve your DAG import time:\n"
            f"* {get_docs_url('best-practices.html#top-level-python-code')}\n"
            f"* {get_docs_url('best-practices.html#reducing-dag-complexity')}"
        )
        with timeout(dagbag_import_timeout, error_message=timeout_msg):
            return parse(mod_name, filepath)

    def _load_modules_from_zip(self, filepath, safe_mode, import_errors):
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        mods = []
        with zipfile.ZipFile(filepath) as current_zip_file:
            for zip_info in current_zip_file.infolist():
                zip_path = Path(zip_info.filename)
                if zip_path.suffix not in [".py", ".pyc"] or len(zip_path.parts) > 1:
                    continue

                if zip_path.stem == "__init__":
                    self.log.warning("Found %s at root of %s", zip_path.name, filepath)

                self.log.debug("Reading %s from %s", zip_info.filename, filepath)

                if not might_contain_dag(zip_info.filename, safe_mode, current_zip_file):
                    # todo: create ignore list
                    # Don't want to spam user with skip messages
                    if not self.has_logged_skipped_files:
                        self.has_logged_skipped_files = True
                        self.log.info(
                            "File %s:%s assumed to contain no DAGs. Skipping.", filepath, zip_info.filename
                        )
                    continue

                mod_name = zip_path.stem
                if mod_name in sys.modules:
                    del sys.modules[mod_name]

                DagContext.current_autoregister_module_name = mod_name
                try:
                    sys.path.insert(0, filepath)
                    current_module = importlib.import_module(mod_name)
                    mods.append(current_module)
                except Exception as e:
                    DagContext.autoregistered_dags.clear()
                    fileloc = os.path.join(filepath, zip_info.filename)
                    self.log.exception("Failed to import: %s", fileloc)
                    if self.dagbag_import_error_tracebacks:
                        import_errors[filepath] = traceback.format_exc(
                            limit=-self.dagbag_import_error_traceback_depth
                        )
                    else:
                        import_errors[filepath] = str(e)
                finally:
                    if sys.path[0] == filepath:
                        del sys.path[0]
        return mods

    def _process_modules(self, mods):
        from airflow.sdk import DAG
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        top_level_dags = {(o, m) for m in mods for o in m.__dict__.values() if isinstance(o, DAG)}

        top_level_dags.update(DagContext.autoregistered_dags)

        DagContext.current_autoregister_module_name = None
        DagContext.autoregistered_dags.clear()

        found_dags = []

        for dag, mod in top_level_dags:
            dag.fileloc = mod.__file__
            found_dags.append(dag)
        return found_dags
