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
"""Compatibility helpers for the Task SDK's optional dependency on airflow-core.

The Task SDK is meant to be installable and usable without ``apache-airflow-core``.
A handful of code paths (backfill, serialization round-trips through core models,
executor wiring, etc.) genuinely need core at runtime. Those sites should import
core lazily, inside the function that needs it, and wrap the import in
:func:`require_core` so a missing install produces an actionable message instead
of a bare ``ImportError`` deep in the stack.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = ["require_core"]

_INSTALL_HINT = "Install it with: pip install 'apache-airflow-task-sdk[core]'"


@contextmanager
def require_core(feature: str) -> Iterator[None]:
    """Guard a lazy import of ``apache-airflow-core``.

    Wrap the core import for ``feature`` in this context manager. If core is not
    installed the raw :class:`ImportError` is re-raised with a friendly,
    actionable message naming the feature and how to install core.

    Example::

        def run_backfill(...):
            with require_core("Backfilling"):
                from airflow.models.backfill import Backfill
            ...
    """
    try:
        yield
    except ImportError as exc:
        raise ImportError(f"{feature} requires apache-airflow-core. {_INSTALL_HINT}") from exc
