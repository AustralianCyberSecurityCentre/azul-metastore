from unittest import mock

from azul_metastore.common import search_data, wrapper
from tests.support import unit_test


class TestUtil(unit_test.BaseUnitTestCase):
    def test_partition_key_to_indices(self):
        self.assertEqual(4, len(wrapper.partition_key_to_indices("partition", "key")))
        self.assertEqual("azul.x.partition.key.*", wrapper.partition_key_to_indices("partition", "key")[0])
        self.assertEqual("azul.x.partition2.key2.*", wrapper.partition_key_to_indices("partition2", "key2")[0])

    @mock.patch("azul_metastore.common.search_data.SearchData")
    def test_set_index_properties(self, _pt):
        wrapper.set_index_properties("partition.", "key", {})


class TestWrapper(unit_test.BaseUnitTestCase):
    def test_limit_search(self):
        # test that user defined limits are correctly used to construct filters
        w = wrapper.Wrapper("", "encoded", {}, [], {}, 1)

        # check exclusive filter works
        sd = search_data.SearchData(
            credentials={}, security_exclude=["HIGH"], security_include=[], security_filter="OR"
        )
        query = {}
        query = w._limit_search(sd, query)
        self.assertEqual(
            {
                "query": {
                    "bool": {
                        "must_not": [
                            {"terms": {"encoded_security.inclusive": ["s-high"]}},
                            {"terms": {"encoded_security.exclusive": ["s-high"]}},
                            {"terms": {"encoded_security.markings": ["s-high"]}},
                        ],
                        "must": [],
                        "filter": [],
                    }
                }
            },
            query,
        )

        query = {"query": {"bool": {"filter": [{"terms": {"genuine.rolodex": "true"}}]}}}
        query = w._limit_search(sd, query)
        self.assertEqual(
            {
                "query": {
                    "bool": {
                        "must_not": [
                            {"terms": {"encoded_security.inclusive": ["s-high"]}},
                            {"terms": {"encoded_security.exclusive": ["s-high"]}},
                            {"terms": {"encoded_security.markings": ["s-high"]}},
                        ],
                        "must": [],
                        "filter": [{"terms": {"genuine.rolodex": "true"}}],
                    }
                }
            },
            query,
        )

    def test_limit_search_include(self):
        # test that user defined limits are correctly used to construct filters
        w = wrapper.Wrapper("", "encoded", {}, [], {}, 1)

        # check exclusive filter works
        sd = search_data.SearchData(
            credentials={}, security_exclude=["HIGH"], security_include=["REL:APPLE"], security_filter="AND"
        )
        query = {}
        query = w._limit_search(sd, query)
        self.assertEqual(
            {
                "query": {
                    "bool": {
                        "must_not": [
                            {"terms": {"encoded_security.exclusive": ["s-high"]}},
                            {"terms": {"encoded_security.markings": ["s-high"]}},
                        ],
                        "must": [{"term": {"encoded_security.inclusive": "s-rel-apple"}}],
                        "filter": [],
                    }
                }
            },
            query,
        )

        query = {"query": {"bool": {"filter": [{"terms": {"genuine.rolodex": "true"}}]}}}
        query = w._limit_search(sd, query)
        self.assertEqual(
            {
                "query": {
                    "bool": {
                        "filter": [{"terms": {"genuine.rolodex": "true"}}],
                        "must_not": [
                            {"terms": {"encoded_security.exclusive": ["s-high"]}},
                            {"terms": {"encoded_security.markings": ["s-high"]}},
                        ],
                        "must": [{"term": {"encoded_security.inclusive": "s-rel-apple"}}],
                    }
                }
            },
            query,
        )

    def test_limit_search_complex(self):
        # test that user defined limits are correctly used to construct filters
        w = wrapper.Wrapper("", "encoded", {}, [], {}, 1)

        # check exclusive filter works
        sd = search_data.SearchData(
            credentials={}, security_exclude=["HIGH"], security_include=[], security_filter="OR"
        )
        query = {
            "query": {
                "bool": {
                    "filter": [{"has_child": {"type": "metadata", "query": {"exists": {"field": "source.name"}}}}],
                    "should": [],
                }
            },
        }
        query = w._limit_search_complex(sd, query)
        self.assertEqual(
            {
                "query": {
                    "bool": {
                        "filter": [{"has_child": {"type": "metadata", "query": {"exists": {"field": "source.name"}}}}],
                        "should": [],
                        "must_not": [
                            {"terms": {"encoded_security.inclusive": ["s-high"]}},
                            {"terms": {"encoded_security.exclusive": ["s-high"]}},
                            {"terms": {"encoded_security.markings": ["s-high"]}},
                        ],
                    }
                }
            },
            query,
        )
        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"terms": {"genuine.rolodex": "true"}},
                        {
                            "has_child": {
                                "type": "metadata",
                                "query": {"bool": {"must": [{"exists": {"field": "source.name"}}], "must_not": []}},
                            }
                        },
                    ],
                    "should": [],
                }
            },
        }
        query = w._limit_search_complex(sd, query)
        print(query)
        self.assertEqual(
            {
                "query": {
                    "bool": {
                        "filter": [
                            {"terms": {"genuine.rolodex": "true"}},
                            {
                                "has_child": {
                                    "type": "metadata",
                                    "query": {
                                        "bool": {"must": [{"exists": {"field": "source.name"}}], "must_not": []}
                                    },
                                }
                            },
                        ],
                        "should": [],
                        "must_not": [
                            {"terms": {"encoded_security.inclusive": ["s-high"]}},
                            {"terms": {"encoded_security.exclusive": ["s-high"]}},
                            {"terms": {"encoded_security.markings": ["s-high"]}},
                        ],
                    }
                }
            },
            query,
        )

    def test_limit_search_complex_inclusive(self):
        # test that user defined limits are correctly used to construct filters
        w = wrapper.Wrapper("", "encoded", {}, [], {}, 1)

        # check exclusive filter works
        sd = search_data.SearchData(
            credentials={}, security_exclude=["REL:CAR", "HIGH"], security_include=["REL:APPLE"], security_filter="AND"
        )
        query = {
            "query": {
                "bool": {
                    "filter": [{"has_child": {"type": "metadata", "query": {"exists": {"field": "source.name"}}}}],
                    "should": [],
                }
            },
        }
        query = w._limit_search_complex(sd, query)
        self.assertEqual(
            {
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "has_child": {
                                    "type": "metadata",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {"exists": {"field": "source.name"}},
                                                {"term": {"encoded_security.inclusive": "s-rel-apple"}},
                                            ],
                                            "must_not": [{"term": {"encoded_security.inclusive": "s-rel-car"}}],
                                        }
                                    },
                                }
                            }
                        ],
                        "should": [],
                        "must_not": [
                            {"terms": {"encoded_security.exclusive": ["s-rel-car", "s-high"]}},
                            {"terms": {"encoded_security.markings": ["s-rel-car", "s-high"]}},
                        ],
                    }
                }
            },
            query,
        )
        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"terms": {"genuine.rolodex": "true"}},
                        {"has_child": {"type": "metadata", "query": {"exists": {"field": "source.name"}}}},
                    ],
                    "should": [],
                }
            },
        }
        query = w._limit_search_complex(sd, query)
        self.assertEqual(
            {
                "query": {
                    "bool": {
                        "filter": [
                            {"terms": {"genuine.rolodex": "true"}},
                            {
                                "has_child": {
                                    "type": "metadata",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {"exists": {"field": "source.name"}},
                                                {"term": {"encoded_security.inclusive": "s-rel-apple"}},
                                            ],
                                            "must_not": [{"term": {"encoded_security.inclusive": "s-rel-car"}}],
                                        }
                                    },
                                }
                            },
                        ],
                        "should": [],
                        "must_not": [
                            {"terms": {"encoded_security.exclusive": ["s-rel-car", "s-high"]}},
                            {"terms": {"encoded_security.markings": ["s-rel-car", "s-high"]}},
                        ],
                    }
                }
            },
            query,
        )

    def test_prep_docs(self):
        # test open security processing
        open_security = ["LOW", "MED", "ATTIC"]
        w = wrapper.Wrapper("doge", "encoded", {}, open_security, {}, 1)

        docs = [
            {"_id": "a", "encoded_security": {"exclusive": ["LOW"], "unique": ""}},
            {"_id": "a", "encoded_security": {"exclusive": ["MED"], "unique": ""}},
            {"_id": "a", "encoded_security": {"exclusive": ["MED"], "inclusive": ["ATTIC"], "unique": ""}},
            {"_id": "a", "encoded_security": {"exclusive": ["HIGH"], "unique": ""}},
            {"_id": "a", "encoded_security": {"exclusive": ["HIGH"], "inclusive": ["ATTIC"], "unique": ""}},
        ]

        ret = w.wrap_docs(docs)
        # note since _id is popped from the input dicts, we expect an exact match still
        self.assertEqual(
            [
                {"_op_type": "index", "_index": "azul.o.doge.encoded", "_id": "a", "_source": docs[0]},
                {"_op_type": "index", "_index": "azul.o.doge.encoded", "_id": "a", "_source": docs[1]},
                {"_op_type": "index", "_index": "azul.o.doge.encoded", "_id": "a", "_source": docs[2]},
                {"_op_type": "index", "_index": "azul.x.doge.encoded", "_id": "a", "_source": docs[3]},
                {"_op_type": "index", "_index": "azul.x.doge.encoded", "_id": "a", "_source": docs[4]},
            ],
            ret,
        )

        # test that presence of markings dont affect target index
        docs = [
            {"_id": "a", "encoded_security": {"markings": ["TLP: RED"], "unique": ""}},
            {"_id": "a", "encoded_security": {"exclusive": ["LOW"], "markings": ["TLP: RED"], "unique": ""}},
            {"_id": "a", "encoded_security": {"exclusive": ["HIGH"], "markings": ["TLP: RED"], "unique": ""}},
        ]

        ret = w.wrap_docs(docs)
        # note since _id is popped from the input dicts, we expect an exact match still
        self.assertEqual(
            [
                {"_op_type": "index", "_index": "azul.o.doge.encoded", "_id": "a", "_source": docs[0]},
                {"_op_type": "index", "_index": "azul.o.doge.encoded", "_id": "a", "_source": docs[1]},
                {"_op_type": "index", "_index": "azul.x.doge.encoded", "_id": "a", "_source": docs[2]},
            ],
            ret,
        )
