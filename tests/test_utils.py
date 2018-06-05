import json
import types
from datetime import datetime

import pytest
from sqlalchemy import select

from savage import utils
from tests.models import UnarchivedTable


@pytest.fixture
def saved_model(session):
    model = UnarchivedTable(name='foo')
    session.add(model)
    session.commit()
    session.refresh(model)
    return model


def test_result_to_dict(session, saved_model):
    res = session.execute(select([UnarchivedTable]))
    dicts = utils.result_to_dict(res)
    expected_dict = {
        'id': saved_model.id,
        'name': 'foo',
        'private_attr': None,
        'created_at': saved_model.created_at,
        'jsonb_col': {},
    }
    assert dicts == [expected_dict]


def test_get_column_attribute(saved_model, dialect):
    assert utils.get_column_attribute(saved_model, 'name', dialect=dialect) == 'foo'


def test_get_column_attribute_dirty(saved_model, dialect):
    saved_model.name = 'bar'
    assert utils.get_column_attribute(saved_model, 'name', use_dirty=False, dialect=dialect) == 'foo'


def test_get_column_attribute_json(saved_model, dialect):
    json_dict = {'foo': 'bar'}
    saved_model.jsonb_col = json_dict.copy()
    assert utils.get_column_attribute(saved_model, 'jsonb_col', dialect=dialect) == json_dict


def test_get_column_keys():
    col_keys_gen = utils.get_column_keys(UnarchivedTable)
    assert isinstance(col_keys_gen, types.GeneratorType)
    assert sorted(col_keys_gen) == ['_private_attr', 'created_at', 'id', 'jsonb_col', 'name']


def test_get_column_names():
    col_keys_gen = utils.get_column_names(UnarchivedTable)
    assert isinstance(col_keys_gen, types.GeneratorType)
    assert sorted(col_keys_gen) == ['created_at', 'id', 'jsonb_col', 'name', 'private_attr']


def test_get_column_keys_and_names():
    col_keys_gen = utils.get_column_keys_and_names(UnarchivedTable)
    assert isinstance(col_keys_gen, types.GeneratorType)
    assert sorted(col_keys_gen) == [
        ('_private_attr', 'private_attr'),
        ('created_at', 'created_at'),
        ('id', 'id'),
        ('jsonb_col', 'jsonb_col'),
        ('name', 'name'),
    ]


def test_get_dialect(session):
    assert utils.get_dialect(session) == session.bind.dialect


def test_is_modified(saved_model, dialect):
    assert not utils.is_modified(saved_model, dialect)


def test_is_modified_with_change(saved_model, dialect):
    saved_model.name = 'bar'
    assert utils.is_modified(saved_model, dialect)


def test_is_modified_with_no_net_change(saved_model, dialect):
    saved_model.name = 'bar'
    saved_model.name = 'foo'
    assert not utils.is_modified(saved_model, dialect)


def test_savage_json_encoder_default_with_unserializable():
    encoder = utils.SavageJSONEncoder()
    with pytest.raises(TypeError):
        encoder.default(object())


def test_savage_json_encoder_default_with_datetime():
    ts = datetime.now()
    encoder = utils.SavageJSONEncoder()
    assert encoder.default(ts) == ts.isoformat()


def test_savage_json_serializer_datetime():
    ts = datetime.now()
    to_serialize = {'created_at': ts}
    serialized = utils.savage_json_serializer(to_serialize)
    assert json.loads(serialized) == {'created_at': ts.isoformat()}
