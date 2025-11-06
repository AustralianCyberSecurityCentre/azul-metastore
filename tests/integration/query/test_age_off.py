import json
import os
import time
from unittest import mock

import pendulum

from azul_metastore.common import wrapper
from azul_metastore.query import age_off
from azul_metastore.query.annotation import read_all_binary_tags
from tests.support import gen, integration_test

now = "2023-10-10T10:10:10Z"
nowp = pendulum.parse(now)
longago = pendulum.parse("1995-10-10T10:10:10Z")


class TestAgeOff(integration_test.DynamicTestCase):

    @classmethod
    def setUpClass(cls):
        cls.origin_max_delete_for_ageoff = wrapper.MAX_DOCS_DELETED_PER_QUERY
        cls.origin_min_delete_for_ageoff = wrapper.MIN_DOCS_DELETED_PER_QUERY
        wrapper.MAX_DOCS_DELETED_PER_QUERY = 3
        wrapper.MIN_DOCS_DELETED_PER_QUERY = 3
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        wrapper.MAX_DOCS_DELETED_PER_QUERY = cls.origin_max_delete_for_ageoff
        wrapper.MIN_DOCS_DELETED_PER_QUERY = cls.origin_min_delete_for_ageoff
        return super().tearDownClass()

    @classmethod
    def alter_environment(cls):
        super().alter_environment()
        # partition unit doesn't do anything for experimental ingestor
        os.environ["metastore_sources"] = json.dumps(
            {
                "day1": {"partition_unit": "day", "expire_events_after": "1 days"},
                "day10": {"partition_unit": "day", "expire_events_after": "10 days"},
                "week1": {"partition_unit": "week", "expire_events_after": "1 weeks"},
                "week10": {"partition_unit": "week", "expire_events_after": "10 weeks"},
                "month1": {"partition_unit": "month", "expire_events_after": "1 months"},
                "month10": {"partition_unit": "month", "expire_events_after": "10 months"},
                "year1": {"partition_unit": "year", "expire_events_after": "1 years"},
                "year10": {"partition_unit": "year", "expire_events_after": "10 years"},
                "keep1": {},
                "keep2": {"partition_unit": "month"},
                "keep3": {"partition_unit": "month", "expire_events_after": "0"},
                "keep4": {"expire_events_after": "0"},
                "all1": {"partition_unit": "all"},
                # Shouldn't be enforced for "all", as keeping last doesn't make sense
                "all2": {"partition_unit": "all", "expire_events_after": "0"},
            }
        )

    def _opensearch_seed(self, in_now: pendulum.DateTime):
        std_patch = {"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "day1"}}
        self.write_binary_events(
            [
                # discard eventually
                gen.binary_event({"source": {"timestamp": now, "name": "day1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "day10"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "week1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "week10"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "month1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "month10"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "year10"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "keep1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "keep2"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "keep3"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": now, "name": "keep4"}}, eid="e1"),
                # keep
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "keep1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "keep2"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "keep3"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "keep4"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "all1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "all2"}}, eid="e1"),
                # discard soon
                gen.binary_event(std_patch, eid="e2"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "day10"}}, eid="e2"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "week1"}}, eid="e2"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "week10"}}, eid="e2"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "month1"}}, eid="e2"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "month10"}}, eid="e2"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "year1"}}, eid="e2"),
                gen.binary_event({"source": {"timestamp": "2000-01-01T00:00:00Z", "name": "year10"}}, eid="e2"),
                # results
                gen.binary_event(std_patch, eid="e2", action="enriched", fvl=[("f1", "v1")], authornv=("p1", "1")),
                gen.binary_event(std_patch, eid="e2", action="enriched", fvl=[("f2", "v1")], authornv=("p2", "1")),
                # links
                gen.binary_event(
                    std_patch, eid="e3", spathl=[("e2", ("me", "1"))], action="extracted", authornv=("plugin", "2")
                ),
                gen.binary_event(
                    std_patch, eid="e4", spathl=[("e2", ("me", "1"))], action="extracted", authornv=("plugin", "2")
                ),
            ],
            now=in_now,
        )

    @mock.patch("pendulum.now", lambda: nowp)
    def test_no_write_old_docs(self):
        self._opensearch_seed(nowp)

        # should be no candidates for age off
        self.assertEqual(([], {}, 0), age_off.do_age_off())

    @mock.patch("pendulum.now", lambda: nowp)
    def test_do_age_off(self):
        self._opensearch_seed(longago)

        with mock.patch("pendulum.now", lambda: pendulum.parse("1995-10-10T10:10:10Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(deleted_docs, {})

        with mock.patch("pendulum.now", lambda: pendulum.parse("2000-01-01T10:10:10Z")):

            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(deleted_docs, {})

        with mock.patch("pendulum.now", lambda: pendulum.parse("2000-01-08T10:10:10Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(
                deleted_docs,
                {
                    f"azul.{self.partition}.binary2-submission-day1": 3,
                    f"azul.{self.partition}.binary2-submission-week1": 1,
                    # delete e3 & e4
                    f"azul.{self.partition}.binary2-other": 2,
                },
            )

        with mock.patch("pendulum.now", lambda: pendulum.parse("2000-10-10T10:10:10Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(
                deleted_docs,
                {
                    f"azul.{self.partition}.binary2-submission-day10": 1,
                    f"azul.{self.partition}.binary2-submission-week10": 1,
                    f"azul.{self.partition}.binary2-submission-month1": 1,
                },
            )

        with mock.patch("pendulum.now", lambda: pendulum.parse("2030-10-10T10:10:10Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(
                deleted_docs,
                {
                    f"azul.{self.partition}.binary2-submission-day1": 1,
                    f"azul.{self.partition}.binary2-submission-day10": 1,
                    f"azul.{self.partition}.binary2-submission-week1": 1,
                    f"azul.{self.partition}.binary2-submission-week10": 1,
                    f"azul.{self.partition}.binary2-submission-month1": 1,
                    f"azul.{self.partition}.binary2-submission-month10": 2,
                    f"azul.{self.partition}.binary2-submission-year1": 2,
                    f"azul.{self.partition}.binary2-submission-year10": 1,
                    # delete the parent doc and results for 'e2'
                    f"azul.{self.partition}.binary2-other": 3,
                },
            )

        with mock.patch("pendulum.now", lambda: pendulum.parse("2035-10-10T10:10:10Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(deleted_docs, {f"azul.{self.partition}.binary2-submission-year10": 1})

        with mock.patch("pendulum.now", lambda: pendulum.parse("2080-10-10T10:10:10Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(deleted_docs, {})

    @mock.patch("pendulum.now", lambda: nowp)
    def test_age_off_just_documents(self):
        self.write_binary_events(
            [
                gen.binary_event({"source": {"timestamp": "2001-01-01T00:00:00Z", "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2001-03-01T00:00:00Z", "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2001-05-01T00:00:00Z", "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2001-08-01T00:00:00Z", "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2002-01-01T00:00:00Z", "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2002-02-01T00:00:00Z", "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2002-03-01T00:00:00Z", "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2002-04-01T00:00:00Z", "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2002-05-01T00:00:00Z", "name": "year1"}}, eid="e1"),
                gen.binary_event({"source": {"timestamp": "2003-01-01T00:00:00Z", "name": "year1"}}, eid="e1"),
            ],
            now=longago,
        )
        # ensure status event exists to age off as well
        self.write_status_events([gen.status(eid="e1")])

        # Age off old 2001 documents but not the index, remove the status index
        with mock.patch("pendulum.now", lambda: pendulum.parse("2002-05-01T00:00:00Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(deleted_docs, {f"azul.{self.partition}.binary2-submission-year1": 3})

        # Age off the 2001 index and some of the 2002 documents.
        with mock.patch("pendulum.now", lambda: pendulum.parse("2003-04-01T05:00:00Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(deleted_docs, {f"azul.{self.partition}.binary2-submission-year1": 5})

    @mock.patch("pendulum.now", lambda: nowp)
    def test_age_off_just_annotations(self):
        self.write_binary_events(
            [
                # Old documents
                gen.binary_event({"source": {"timestamp": "2001-01-01T00:00:00Z", "name": "year1"}}, eid="a1"),
                gen.binary_event({"source": {"timestamp": "2001-03-01T00:00:00Z", "name": "year1"}}, eid="a2"),
                gen.binary_event({"source": {"timestamp": "2001-05-01T00:00:00Z", "name": "year1"}}, eid="a3"),
                # Mid range docs
                gen.binary_event({"source": {"timestamp": "2001-08-01T00:00:00Z", "name": "year1"}}, eid="a4"),
                gen.binary_event({"source": {"timestamp": "2002-01-01T00:00:00Z", "name": "year1"}}, eid="a5"),
                gen.binary_event({"source": {"timestamp": "2002-02-01T00:00:00Z", "name": "year1"}}, eid="a6"),
                gen.binary_event({"source": {"timestamp": "2002-03-01T00:00:00Z", "name": "year1"}}, eid="a7"),
                gen.binary_event({"source": {"timestamp": "2002-04-01T00:00:00Z", "name": "year1"}}, eid="a8"),
                # Newest docs
                gen.binary_event({"source": {"timestamp": "2002-05-01T00:00:00Z", "name": "year1"}}, eid="a9"),
                gen.binary_event({"source": {"timestamp": "2003-01-01T00:00:00Z", "name": "year1"}}, eid="a10"),
            ],
            now=longago,
        )
        self.write_entity_tags(
            [
                gen.entity_tag({"sha256": "a1", "tag": "a1-tag"}),
                gen.entity_tag({"sha256": "a2", "tag": "a2-tag"}),
                gen.entity_tag({"sha256": "a3", "tag": "a3-tag"}),
                gen.entity_tag({"sha256": "a4", "tag": "a4-tag"}),
                gen.entity_tag({"sha256": "a5", "tag": "a5-tag"}),
                gen.entity_tag({"sha256": "a6", "tag": "a6-tag"}),
                gen.entity_tag({"sha256": "a7", "tag": "a7-tag"}),
                gen.entity_tag({"sha256": "a8", "tag": "a8-tag"}),
                gen.entity_tag({"sha256": "a9", "tag": "a9-tag"}),
                gen.entity_tag({"sha256": "a10", "tag": "a10-tag"}),
            ]
        )

        all_tags = [all_bin_tag.tag for all_bin_tag in read_all_binary_tags(self.writer).tags]
        self.assertGreaterEqual(len(all_tags), 10)

        # Age off 3 of the 2001 documents
        with mock.patch("pendulum.now", lambda: pendulum.parse("2002-05-01T00:00:00Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(
                deleted_docs,
                {f"azul.{self.partition}.binary2-submission-year1": 3, f"azul.{self.partition}.binary2-other": 3},
            )
            self.assertEqual(deleted_annotation_count, 3)

        # Age off the 2001 index and some of the 2002 documents.
        with mock.patch("pendulum.now", lambda: pendulum.parse("2003-04-01T05:00:00Z")):
            _, deleted_docs, deleted_annotation_count = age_off.do_age_off()
            self.assertFormatted(
                deleted_docs,
                {f"azul.{self.partition}.binary2-submission-year1": 5, f"azul.{self.partition}.binary2-other": 5},
            )
            self.assertEqual(deleted_annotation_count, 5)

        # Refresh indicies to ensure all the bulk annotations updates have been applied.
        self.writer.refresh()
        all_tags = [all_bin_tag.tag for all_bin_tag in read_all_binary_tags(self.writer).tags]
        # Verify of all the added tags only 9 and 10 remain and the rest have been removed.
        self.assertNotIn("a1-tag", all_tags)
        self.assertNotIn("a2-tag", all_tags)
        self.assertNotIn("a3-tag", all_tags)
        self.assertNotIn("a4-tag", all_tags)
        self.assertNotIn("a5-tag", all_tags)
        self.assertNotIn("a6-tag", all_tags)
        self.assertNotIn("a7-tag", all_tags)
        self.assertNotIn("a8-tag", all_tags)
        self.assertIn("a9-tag", all_tags)
        self.assertIn("a10-tag", all_tags)
