from ampel.test.utils import run
from ampel.core.AmpelDB import AmpelDB
from ampel.config.AmpelConfig import AmpelConfig

import pytest


@pytest.mark.usefixtures("_patch_mongo")
@pytest.mark.parametrize("one_db", [True, False])
def test_delete(testing_config, one_db):
    ampeldb = AmpelDB.new(
        AmpelConfig.load(testing_config), require_exists=False, one_db=one_db
    )
    ampeldb.init_db()
    assert all(
        ampeldb._get_pymongo_db(db.name, role=db.role.w).list_collection_names()
        for db in ampeldb.databases
    ), "all databases should be created"
    delete_args = [
        "ampel",
        "db",
        "delete",
        "--config",
        str(testing_config),
        *([] if one_db else ["--one-db"]),
    ]
    assert run(delete_args) is None, "delete should run without error"
    ampeldb.close()
    assert not any(
        ampeldb._get_pymongo_db(db.name, role=db.role.w).list_collection_names()
        for db in ampeldb.databases
    ), "all databases should be deleted"
    assert run(delete_args) is None, "delete is idempotent"


@pytest.mark.usefixtures("_patch_mongo")
@pytest.mark.parametrize("one_db", [True, False])
def test_index(testing_config, one_db):
    ampeldb = AmpelDB.new(
        AmpelConfig.load(testing_config), require_exists=False, one_db=one_db
    )
    ampeldb.init_db()
    assert all(
        ampeldb._get_pymongo_db(db.name, role=db.role.w).list_collection_names()
        for db in ampeldb.databases
    ), "all databases should be created"
    index_args = [
        "ampel",
        "db",
        "index",
        "--config",
        str(testing_config),
        *([] if one_db else ["--one-db"]),
    ]
    assert run(index_args) is None, "index should run without error"
    ampeldb.close()
    assert not any(
        ampeldb._get_pymongo_db(db.name, role=db.role.w).list_collection_names()
        for db in ampeldb.databases
    ), "all databases should be deleted"
    assert run(index_args) is None, "index is idempotent"
