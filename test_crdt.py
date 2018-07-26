import time
import unittest

import crdt_main

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

if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = unittest.TestSuite([loader.loadTestsFromTestCase(TestLWWEInset), 
                                loader.loadTestsFromTestCase(TestLWWElementSet)])
    unittest.TextTestRunner(verbosity=2).run(suite)