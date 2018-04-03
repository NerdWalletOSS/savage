from tests.utils import add_and_return_version, verify_archive, verify_deleted_archive, verify_row


def test_delete(session, user_table, p1_dict, p1):
    version = add_and_return_version(p1, session)

    session.delete(p1)
    session.commit()
    assert not session.query(user_table).filter_by(product_id=p1.product_id).count()

    verify_archive(p1_dict, version, session)
    verify_deleted_archive(p1_dict, p1, version, session, user_table)


def test_insert_after_delete(session, user_table, p1_dict, p1):
    """Inserting a row that has already been deleted should version where it left off
    (not at 0).
    """
    version = add_and_return_version(p1, session)

    session.delete(p1)
    session.commit()

    p_new = dict(p1_dict, **{
        'col1': 'changed',
        'col2': 139,
    })
    q = user_table(**p_new)
    new_version = add_and_return_version(q, session)

    verify_row(p_new, new_version, session)
    verify_archive(p1_dict, version, session)
    deleted_version = verify_deleted_archive(p1_dict, p1, version, session, user_table)
    verify_archive(p_new, new_version, session)
    assert new_version > deleted_version


def test_delete_with_user(session, user_table, p1_dict, p1):
    p1.updated_by('test_user')
    version = add_and_return_version(p1, session)

    session.delete(p1)
    session.commit()
    assert not session.query(user_table).filter_by(product_id=p1.product_id).count()

    verify_archive(p1_dict, version, session)
    verify_deleted_archive(p1_dict, p1, version, session, user_table, user='test_user')
