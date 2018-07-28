import json
import time
import unittest

import crdt_main
import crdt_redis


class TestLWWEInset(unittest.TestCase):

    def setUp(self):
        self.inset = crdt_main.LWWEInset()

    def test_payload_type_protection(self):
        with self.assertRaises(TypeError):
            wrong_type_object = [] # any mutable type works
            self.inset[wrong_type_object] = time.time()

    def test_timestamp_type_protection(self):
        with self.assertRaises(TypeError):
            self.inset['testPayload'] = 'wrong type object'

    def test_correct_types_added_and_mutated(self):
        self.setUp()
        # element can be added
        payload = 'right type object'
        timestamp  = time.time()
        self.inset[payload] = timestamp
        self.assertEqual(self.inset[payload], timestamp)
        # element can be mutated
        timestamp += 10
        self.inset[payload] = timestamp
        self.assertEqual(self.inset[payload], timestamp)

    def test_value_location_by_key(self):
        self.setUp()
        first_payload = 'first payload'
        second_payload = 'second payload'
        timestamp  = time.time()
        self.inset[first_payload] = timestamp
        self.inset[second_payload] = timestamp + 10
        self.assertEqual(self.inset[first_payload], timestamp)
        self.assertEqual(self.inset[second_payload], timestamp + 10)


class TestLWWElementSet(unittest.TestCase):

    def setUp(self):
        self.data_set = crdt_main.LWWElementSet()

    def test_add_element(self):
        self.setUp()
        timestamp  = time.time()
        payload = 'payload'
        self.data_set.add(payload, timestamp)
        self.assertEqual(timestamp, self.data_set._set_add[payload])

    def test_remove_element(self):
        self.setUp()
        timestamp  = time.time()
        payload = 'payload'
        self.data_set.remove(payload, timestamp)
        self.assertEqual(timestamp, self.data_set._set_remove[payload])

    def test_added_element_exists(self):
        self.setUp()
        element_payload = 'first'
        self.data_set.add(element_payload, time.time())
        self.assertTrue(self.data_set.exists(element_payload))

    def test_removed_element_doesnt_exist(self):
        self.setUp()
        element_payload = 'removed'
        self.data_set.add(element_payload, time.time())
        self.data_set.remove(element_payload, time.time() + 10)
        self.assertFalse(self.data_set.exists(element_payload))

    def test_non_added_element_doesnt_exist(self):
        self.setUp()
        element_payload = 'added'
        self.data_set.add(element_payload, time.time())
        self.assertFalse(self.data_set.exists('never added'))

    def test_element_readded_exists(self):
        self.setUp()
        element_payload = 'readded'
        self.data_set.add(element_payload, time.time())
        self.data_set.remove(element_payload, time.time() + 10)
        self.data_set.add(element_payload, time.time() + 20)
        self.assertTrue(self.data_set.exists(element_payload))

    def test_can_get_added_elements(self):
        self.setUp()
        elements = {'first': time.time(), 'second': time.time()}
        for payload, timestamp in elements.iteritems():
            self.data_set.add(payload, timestamp)
        self.assertEqual(len(self.data_set.get()), 2)
        self.assertIn(elements.keys()[0], self.data_set.get())

    def test_removed_element_is_removed(self):
        self.setUp()
        elements = {'first': time.time(), 'second': time.time()}
        for payload, timestamp in elements.iteritems():
            self.data_set.add(payload, timestamp)
        self.data_set.remove(elements.keys()[0], time.time() + 10)
        self.assertEqual(len(self.data_set.get()), 1)
        self.assertNotIn(elements.keys()[0], self.data_set.get())


class TestLWWElementSetRedis(TestLWWElementSet):

    def __init__(self, *args, **kwargs):
        redis_params = json.loads(open('redis.json').read())
        self.data_set = crdt_redis.LWWElementSetRedis(redis_params)
        super(TestLWWElementSetRedis, self).__init__(*args, **kwargs)

    def setUp(self):
        self.data_set._redisclient.flushdb()

if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = unittest.TestSuite([loader.loadTestsFromTestCase(TestLWWEInset), 
                                loader.loadTestsFromTestCase(TestLWWElementSet),
                                loader.loadTestsFromTestCase(TestLWWElementSetRedis)])
    unittest.TextTestRunner(verbosity=3).run(suite)
