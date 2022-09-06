from aleph.web.controllers.utils import prune_mongo_id


def test_prune_mongo_id():
    data = {
        "_id": {
            "$oid": "63174837a56357d39fcb2830",
        },
        "something": "cool",
        "other_key": "other_value",
    }

    result = prune_mongo_id(data)
    assert "_id" not in result
    assert result == {
        "something": "cool",
        "other_key": "other_value",
    }
