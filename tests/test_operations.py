import json

import pytest
from test_outbox import put


def test_counts_and_pending_pagination(box):
    ids = sorted(put(box, key=str(i)) for i in range(5))
    assert box.counts() == dict(
        pending=5, leased=0, succeeded=0, failed=0, uncertain=0, cancelled=0
    )
    first = box.find("pending", limit=2)
    second = box.find("pending", after=first[-1]["id"], limit=3)
    assert [entry["id"] for entry in first + second] == ids
    assert box.find("pending", after=ids[-1]) == []


def test_find_uncertain_and_metadata_boundary(box):
    intent = put(box)
    lease = box.claim("worker")
    box.retry(lease)
    entries = box.find("uncertain")
    assert [entry["id"] for entry in entries] == [intent]
    assert box.counts()["uncertain"] == 1
    assert box.counts()["pending"] == 0
    text = json.dumps(entries)
    assert lease.token not in text
    assert '"payload"' not in text
    assert '"idem"' not in text


@pytest.mark.parametrize(
    "arguments",
    [
        {"state": "invalid"},
        {"state": "pending", "limit": 0},
        {"state": "pending", "limit": 1001},
        {"state": "pending", "limit": True},
    ],
)
def test_find_rejects_invalid_queries(box, arguments):
    with pytest.raises(ValueError):
        box.find(**arguments)
