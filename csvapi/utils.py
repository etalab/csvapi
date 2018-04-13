def get_db_info(db_root_dir, _hash):
    dbpath = '{}/{}.db'.format(db_root_dir, _hash)
    return {
        'dsn': 'sqlite:///{}'.format(dbpath),
        'db_name': _hash,
        'table_name': _hash,
        'db_path': dbpath,
    }
