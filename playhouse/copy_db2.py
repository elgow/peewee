"""
Utility to copy between genetics_db databases, including sqlite <--> postgres
"""

from math import ceil
import inspect
from peewee import Model, sort_models_topologically, Using, PostgresqlDatabase, SqliteDatabase
import sys
import logging

logging.basicConfig()
log = logging.getLogger(__name__)
if sys.gettrace():  # see debug log output when running in debugger
    log.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.INFO)



def copy_db2(models, source, target, block_size=1000):
    '''
    Utility function to copy one Genetics DB to another using peewee. Useful for copying between different DBs
    where data type incompatibility prevents using builtin dump/load tools. Also useful for integration tests
    where data is in a sqlite file but test wants it in a different production DB, e.g. postgresql.
    Peewee models must use the database Proxy so that their DB can be initialized dynamically.
    :param module: model definition module for both source and target
    :param source: peewee database for source db
    :param target: peewee database  for target db
    :return dict of table_name:row_count for tables successfully copied
    '''

    # topo sort to avoid constraint violations
    ordered_models = sort_models_topologically(models)
    with Using(target, ordered_models):
        target.create_tables(ordered_models, safe=True)

    copied = {}
    for model in ordered_models:
        with Using(source, [model]):
            src_count = model.select().count()
        pages = int(ceil(float(src_count)/block_size))
        count = 0

        for page in range(pages):
            with Using(source, [model]):
                rows = list(model.select().paginate(page+1, block_size).dicts().execute())
            with Using (target, [model]):
                model.insert_many(rows).execute()
                target.commit()
            count = count + len(rows)

        if issubclass(target.__class__, PostgresqlDatabase):
            # ensure that next generated key won't conflict with copied records
            with Using(target, [model]):
                _update_pk_sequence(model)
        target.commit()
        copied[model._meta.name] = count

    return copied

def _get_pk_sequence(model):
    meta = model._meta
    if meta.primary_key.sequence:
        return meta.primary_key.sequence
    elif meta.auto_increment:
        return model.raw("SELECT pg_get_serial_sequence('%s', '%s')" %
            (meta.db_table, meta.primary_key.db_column)).scalar()
    else:
        return None

def _update_pk_sequence(model):
    seq = _get_pk_sequence(model)
    if seq:
        pk_col = model._meta.primary_key.db_column
        table = model._meta.db_table
        final_vals = model.raw("""
            with max_id as (select max({0}) as max from {1})
            select max, setval('{2}', max, true)
            from  max_id
            where  max >  (select last_value from {2})""".format(pk_col, table, seq)).execute()




def test():
    source = SqliteDatabase('/Users/edgow/Projects/GeneticsDB/GoldenDB/lis_data test_copy.db')
    target = PostgresqlDatabase('copy_test', user='edgow')

    from importlib import import_module

    sys.path.append('/Users/edgow/PycharmProjects/genetics_db')
    m = import_module('genetics_db.table_models')

    copied = copy_db2([m.Frequencies, m.Patients, m.Mrns, m.RunInfo, m.Results, m.Variants], source, target, block_size=10000)
    print copied

test()