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

from airflow.sdk import TaskInstanceState
from airflow.sdk.triggers.base import (
    BaseEventTrigger,
    BaseTrigger,
    StartTriggerArgs,
    TaskSuccessEvent,
    TriggerEvent,
)


class _DummyTrigger(BaseTrigger):
    def __init__(self, value="x", **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def serialize(self):
        return ("airflow.sdk.triggers.tests.DummyTrigger", {"value": self.value})

    async def run(self):
        yield TriggerEvent({"value": self.value})


class TestBaseTrigger:
    def test_repr_uses_serialized_classpath_and_kwargs(self):
        t = _DummyTrigger(value="hello")
        assert repr(t) == "<airflow.sdk.triggers.tests.DummyTrigger value=hello>"

    def test_supports_triggerer_queue_default(self):
        assert BaseTrigger.supports_triggerer_queue is True
        assert BaseEventTrigger.supports_triggerer_queue is False


class TestTriggerEvent:
    def test_event_repr(self):
        assert repr(TriggerEvent({"a": 1})) == "TriggerEvent<{'a': 1}>"

    def test_task_success_event_carries_state(self):
        ev = TaskSuccessEvent()
        assert ev.task_instance_state == "success"

    def test_task_success_event_payload_is_state_value(self):
        # The payload must be the enum *value* ("success"), the historical wire
        # value, not the enum's repr ("TaskInstanceState.SUCCESS"). Downstream
        # consumers (e.g. asset-change extras) treat the payload as that string.
        ev = TaskSuccessEvent()
        assert ev.payload == "success"
        assert ev.payload == TaskInstanceState.SUCCESS


class TestStartTriggerArgs:
    def test_defaults(self):
        args = StartTriggerArgs(trigger_cls="x", next_method="execute_complete")
        assert args.trigger_kwargs is None
        assert args.timeout is None
