# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.core.constants import YML_EXTENSION
from tdp.core.variables.scanner import ServiceDirectoryScanner


class TestServiceDirectoryScanner:
    def test_identifies_service_directories_with_yml(self, tmp_path):
        service_dir = tmp_path / "service1"
        service_dir.mkdir()
        (service_dir / f"file{YML_EXTENSION}").write_text("key: value")

        scanner = ServiceDirectoryScanner()
        services = scanner.scan(tmp_path)

        assert len(services) == 1
        assert "service1" in services
        assert services["service1"] == service_dir

    def test_skips_non_directories(self, tmp_path):
        (tmp_path / f"not_a_dir{YML_EXTENSION}").write_text("I'm a file")

        scanner = ServiceDirectoryScanner()
        services = scanner.scan(tmp_path)

        assert len(services) == 0

    def test_skips_directories_without_yml(self, tmp_path):
        service_dir = tmp_path / "service2"
        service_dir.mkdir()
        (service_dir / "config.txt").write_text("not yaml")

        scanner = ServiceDirectoryScanner()
        services = scanner.scan(tmp_path)

        assert len(services) == 0

    def test_multiple_services_detected(self, tmp_path):
        service1 = tmp_path / "service1"
        service1.mkdir()
        (service1 / f"file1{YML_EXTENSION}").write_text("key: value")

        service2 = tmp_path / "service2"
        service2.mkdir()
        (service2 / f"file2{YML_EXTENSION}").write_text("key: value")

        scanner = ServiceDirectoryScanner()
        services = scanner.scan(tmp_path)

        assert len(services) == 2
        assert "service1" in services
        assert "service2" in services

    def test_nested_service_directories_not_included(self, tmp_path):
        outer = tmp_path / "outer"
        inner = outer / "inner"
        inner.mkdir(parents=True)
        (inner / f"config{YML_EXTENSION}").write_text("key: value")

        scanner = ServiceDirectoryScanner()
        services = scanner.scan(tmp_path)

        # Only top-level directories are scanned
        assert len(services) == 0
