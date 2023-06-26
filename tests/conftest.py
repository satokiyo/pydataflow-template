import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        if item.path.parent.stem == "unit":
            item.add_marker(pytest.mark.unit)
        elif item.path.parent.stem == "integration":
            item.add_marker(pytest.mark.integration)
