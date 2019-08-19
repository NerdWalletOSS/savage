from __future__ import absolute_import

import savage
from tests.models import UnarchivedTable


def test_savage_init_when_initialized(mocker, session):
    mocker.spy(savage, 'event')
    assert savage.is_initialized()
    savage.init()
    savage.event.assert_not_called()


def test_saving_unarchived_model(session):
    row = UnarchivedTable(name='foo')
    session.add(row)
    session.commit()
    assert (row.id and row.created_at)
