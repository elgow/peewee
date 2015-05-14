import models as m
from base import *
from peewee import *
from playhouse.copy_db import copy_db


tgt_db = database_initializer.get_database()
src_db = database_initializer.get_in_memory_database()

module = 'models'
active_models = ['ChildPet', 'Child', 'Parent', 'DBBlog', 'DBUser']
count = 4

class TestCopyDb(ModelTestCase):
    requires = [m.DBUser, m.DBBlog, m.Parent, m.Child, m.ChildPet]

    def setUp(self):
        super(TestCopyDb, self).setUp()


    def tearDown(self):
        super(TestCopyDb, self).tearDown()
        self.populate_src_db()

    def populate_src_db(self, n=count):
        for i in range(n):
            p = m.Parent.create(data='p-%d' % i)
            for j in range(n):
                c = m.Child.create(parent=p, data='c-%d-%d' % (p.id, j))
                for k in range(n):
                    m.ChildPet.create(child=c, data='cp-%d-%d' % (c.id, k))
            u = m.DBUser.create(username='u-%d' % i)
            for l in range(n):
                b = m.DBBlog.create(user=u, title='c-%d-%d' % (u.id, l))

    def compute_expected(self, db, models):
        for m in models:
            Using

    def test_copy_db(self):
        copied = copy_db(module, src_db, tgt_db, drop_model_tables=True, block_size=count)

        self.assertEqual(copied, )