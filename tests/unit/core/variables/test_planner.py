# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, Mock, patch

from tdp.core.variables.messages import ValidationMessageBuilder
from tdp.core.variables.planner import ServiceUpdatePlanner


class TestServiceUpdatePlanner:
    def test_plan_updates_merges_by_service_when_merge_true(self, tmp_path):
        source1 = tmp_path / "source1"
        source2 = tmp_path / "source2"
        service1 = source1 / "hive"
        service2 = source2 / "hive"
        service1.mkdir(parents=True)
        service2.mkdir(parents=True)

        collections = MagicMock()
        builder = Mock(spec=ValidationMessageBuilder)
        builder.for_override.return_value = "OVERRIDE_MSG"
        builder.for_service.return_value = None

        planner = ServiceUpdatePlanner(
            collections=collections, validation_builder=builder
        )

        sources = [("source1", source1), ("source2", source2)]

        def scan_side_effect(path):
            if path == source1:
                return {"hive": service1}
            elif path == source2:
                return {"hive": service2}
            return {}

        with patch(
            "tdp.core.variables.planner.ServiceDirectoryScanner.scan",
            side_effect=scan_side_effect,
        ):
            plans = planner.plan_updates(sources, merge_inputs=True)

        assert len(plans) == 1
        hive_plan = next((p for p in plans if p.service_name == "hive"), None)
        assert hive_plan is not None
        assert hive_plan.input_paths == [service1, service2]

    def test_plan_updates_does_not_merge_when_merge_false(self, tmp_path):
        source1 = tmp_path / "source1"
        source2 = tmp_path / "source2"
        service1 = source1 / "hive"
        service2 = source2 / "hive"
        service1.mkdir(parents=True)
        service2.mkdir(parents=True)

        collections = MagicMock()
        builder = Mock(spec=ValidationMessageBuilder)
        builder.for_override.return_value = "OVERRIDE_MSG"
        builder.for_service.return_value = None

        planner = ServiceUpdatePlanner(
            collections=collections, validation_builder=builder
        )

        sources = [("source1", source1), ("source2", source2)]

        def scan_side_effect(path):
            if path == source1:
                return {"hive": service1}
            elif path == source2:
                return {"hive": service2}
            return {}

        with patch(
            "tdp.core.variables.planner.ServiceDirectoryScanner.scan",
            side_effect=scan_side_effect,
        ):
            plans = planner.plan_updates(sources, merge_inputs=False)

        assert len(plans) == 2
        assert plans[0].service_name == "hive"
        assert plans[0].input_paths == [service1]
        assert plans[1].service_name == "hive"
        assert plans[1].input_paths == [service2]

    def test_plan_adds_custom_validation_message(self, tmp_path):
        source = tmp_path / "override"
        service = source / "hive"
        service.mkdir(parents=True)

        collections = MagicMock()
        builder = MagicMock(spec=ValidationMessageBuilder)
        builder.for_override.return_value = "OVERRIDE_MSG"
        builder.for_service.return_value = "user note"

        planner = ServiceUpdatePlanner(
            collections=collections, validation_builder=builder
        )

        sources = [("override", tmp_path / "override")]

        with patch(
            "tdp.core.variables.planner.ServiceDirectoryScanner.scan",
            return_value={"hive": service},
        ):
            plans = planner.plan_updates(sources, merge_inputs=True)

        assert len(plans) == 1
        assert plans[0].validation_message == "OVERRIDE_MSG\nUser message: user note"

    def test_empty_sources_results_in_empty_plan(self):
        planner = ServiceUpdatePlanner(
            collections=MagicMock(), validation_builder=MagicMock()
        )
        result = planner.plan_updates([], merge_inputs=True)
        assert result == []
