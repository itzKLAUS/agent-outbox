import pytest

from agent_outbox import Outbox


@pytest.fixture
def clock():
    return [1000]


@pytest.fixture
def box(tmp_path, clock):
    return Outbox(tmp_path / "outbox.sqlite", clock=lambda: clock[0])
