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
"""
Public Task SDK home for trigger base classes.

This package is the canonical location for the trigger base classes and the
import path that providers and the SDK should use. The legacy
``airflow.triggers.base`` path re-exports from here for backwards compatibility.
"""

from __future__ import annotations

from airflow.sdk.triggers.base import (
    BaseEventTrigger,
    BaseTaskEndEvent,
    BaseTrigger,
    DiscrimatedTriggerEvent,
    StartTriggerArgs,
    TaskFailedEvent,
    TaskSkippedEvent,
    TaskSuccessEvent,
    TriggerEvent,
    trigger_event_discriminator,
)

__all__ = [
    "BaseEventTrigger",
    "BaseTaskEndEvent",
    "BaseTrigger",
    "DiscrimatedTriggerEvent",
    "StartTriggerArgs",
    "TaskFailedEvent",
    "TaskSkippedEvent",
    "TaskSuccessEvent",
    "TriggerEvent",
    "trigger_event_discriminator",
]
