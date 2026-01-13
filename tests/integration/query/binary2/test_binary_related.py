from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi

from azul_metastore.query.binary2 import binary_related, binary_summary
from tests.support import gen
from tests.support import integration_test as etb


def get_eid_children(ctx, eid):
    """Get set of child ids from a list of links."""
    data = binary_related.read_children(ctx, eid)
    return {x.sha256 for x in data}


def get_eid_read(ctx, eid):
    data = binary_summary.read(ctx, eid).parents
    return {x.sha256 for x in data}


class TestRelationGroups(etb.DynamicTestCase):

    def test_basic(self):
        a = ("p1", "1")
        self.write_binary_events(
            [
                gen.binary_event(eid="e1", spathl=[]),
                gen.binary_event(eid="e10", spathl=[("e1", a)], authorsec=gen.g2_1),
                gen.binary_event(eid="e100", spathl=[("e1", a), ("e10", a)], authorsec=gen.g3_1),
            ]
        )
        self.assertEqual(set(), get_eid_children(self.es1, "e1"))
        self.assertEqual(set(), get_eid_children(self.es1, "e10"))
        self.assertEqual(set(), get_eid_children(self.es1, "e100"))
        self.assertEqual(set(), get_eid_read(self.es1, "e1"))
        self.assertEqual(set(), get_eid_read(self.es1, "e10"))
        self.assertEqual(set(), get_eid_read(self.es1, "e100"))

        self.assertEqual({"e10"}, get_eid_children(self.es2, "e1"))
        self.assertEqual(set(), get_eid_children(self.es2, "e10"))
        self.assertEqual(set(), get_eid_children(self.es2, "e100"))
        self.assertEqual(set(), get_eid_read(self.es2, "e1"))
        self.assertEqual({"e1"}, get_eid_read(self.es2, "e10"))
        self.assertEqual(set(), get_eid_read(self.es2, "e100"))

        self.assertEqual({"e10"}, get_eid_children(self.es3, "e1"))
        self.assertEqual({"e100"}, get_eid_children(self.es3, "e10"))
        self.assertEqual(set(), get_eid_children(self.es3, "e100"))
        self.assertEqual(set(), get_eid_read(self.es3, "e1"))
        self.assertEqual({"e1"}, get_eid_read(self.es3, "e10"))
        self.assertEqual({"e10"}, get_eid_read(self.es3, "e100"))

        self.assertEqual({"e10"}, get_eid_children(self.es2, "E1"))
        self.assertEqual({"e10"}, get_eid_read(self.es3, "E100"))

        self.assertFormatted(
            binary_related.read_children(self.writer, "e1"),
            [
                models_restapi.PathNode(
                    author=azm.Author(
                        security="MEDIUM REL:APPLE",
                        category="plugin",
                        name="generic_plugin",
                        version="2021-01-01T12:00:00+00:00",
                    ),
                    action="extracted",
                    timestamp="2021-01-01T12:00:00Z",
                    relationship={"random": "data", "action": "extracted", "label": "within"},
                    sha256="e10",
                    size=1024,
                    file_format="text/plain",
                    track_link="e1.e10.plugin.generic_plugin.2021-01-01T12:00:00+00:00",
                )
            ],
        )

    def test_read_nearby(self):
        """ "Test relationship with the following binary relationship structure.

        Paths are as follows: arrow works like this Parent -> Child
        generic_source_1 -> e1 -> e10 -> e100 -> e1000
        generic_source_2 -> e2 -> e20 -> e200 -> e1000
        also e10 -> e300
        """
        a = ("p1", "1")
        # Create a binary event with no size or file format information
        # This is to ensure the event with size and format information is prioritised.
        mapped_action_no_size_info: azm.BinaryEvent = gen.binary_event(
            eid="e1", spathl=[], action=azm.BinaryAction.Mapped
        )
        mapped_action_no_size_info.entity.size = None
        mapped_action_no_size_info.entity.file_format = None
        mapped_action_no_size_info.source.path[0].file_format = None
        mapped_action_no_size_info.source.path[0].size = None

        # Create all other events in the structure.
        self.write_binary_events(
            [
                mapped_action_no_size_info,
                gen.binary_event(
                    eid="e1",
                    sourceit=("s2", "2024-01-22T01:00:00+00:00"),
                    sourcesettings={"custom_password": "passwordValue!"},
                ),
                gen.binary_event(eid="e1", sourceit=("s3", "2024-01-22T01:00:00+00:00")),
                gen.binary_event(eid="e1", sourceit=("s4", "2024-01-22T01:00:00+00:00")),
                gen.binary_event(
                    eid="e10", spathl=[("e1", a)], timestamp="2024-01-22T01:01:00+00:00"
                ),  # Older and should be dropped.
                gen.binary_event(
                    eid="e10", spathl=[("e1", a)], authornv=("p12", "1"), timestamp="2024-01-22T01:02:00+00:00"
                ),  # Newer so should be taken in preference
                gen.binary_event(eid="e10", spathl=[("e1", a), ("e10", a)], action=azm.BinaryAction.Mapped),
                gen.binary_event(
                    eid="e10", spathl=[("e1", a)], action=azm.BinaryAction.Enriched, authornv=("p10", "1")
                ),
                gen.binary_event(eid="e100", spathl=[("e1", a), ("e10", a)]),
                gen.binary_event(eid="e2", spathl=[]),
                gen.binary_event(eid="e20", spathl=[("e2", a)]),
                gen.binary_event(eid="e200", spathl=[("e2", a), ("e20", a)]),
                gen.binary_event(eid="e1000", authornv=("a2", "1"), spathl=[("e2", a), ("e20", a), ("e200", a)]),
                gen.binary_event(eid="e1000", authornv=("a1", "1"), spathl=[("e1", a), ("e10", a), ("e100", a)]),
                gen.binary_event(eid="e300", spathl=[("e1", a), ("e10", a)]),
            ]
        )
        # Write mapped event second to ensure it overrides in the event an override occurs.
        self.write_binary_events([gen.binary_event(eid="e1", spathl=[])])

        self.assertEqual(0, len(binary_related.read_nearby(self.writer, "gtrdyh6trd").links))

        # links are parent: generic_source,  child: e1, e10, e100, e1000, e300
        res = binary_related.read_nearby(self.writer, "e1")
        self.assertFormatted(
            res.links,
            [
                models_restapi.ReadNearbyLink(
                    id="e1.plugin.generic_plugin.sourcedgeneric_source",
                    child="e1",
                    child_node=models_restapi.PathNode(
                        sha256="e1",
                        action=azm.BinaryAction.Sourced,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(
                            category="plugin",
                            name="generic_plugin",
                            version="2021-01-01T12:00:00+00:00",
                            security="LOW TLP:CLEAR",
                        ),
                        file_format="text/plain",
                        size=1024,
                    ),
                    source=models_restapi.EventSource(
                        security="LOW TLP:CLEAR",
                        name="generic_source",
                        timestamp="2021-01-01T11:00:00Z",
                        references={"ref2": "val2", "ref1": "val1"},
                        track_source_references="generic_source.dd6e233ae7a843de99f9b43c349069e4",
                    ),
                ),
                models_restapi.ReadNearbyLink(
                    id="e1.plugin.generic_plugin.sourceds2",
                    child="e1",
                    child_node=models_restapi.PathNode(
                        sha256="e1",
                        action=azm.BinaryAction.Sourced,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(
                            category="plugin",
                            name="generic_plugin",
                            version="2021-01-01T12:00:00+00:00",
                            security="LOW TLP:CLEAR",
                        ),
                        file_format="text/plain",
                        size=1024,
                    ),
                    source=models_restapi.EventSource(
                        security="LOW TLP:CLEAR",
                        name="s2",
                        timestamp="2024-01-22T01:00:00Z",
                        references={"ref2": "val2", "ref1": "val1"},
                        settings={"custom_password": "passwordValue!"},
                        track_source_references="s2.dd6e233ae7a843de99f9b43c349069e4",
                    ),
                ),
                models_restapi.ReadNearbyLink(
                    id="e1.plugin.generic_plugin.sourceds3",
                    child="e1",
                    child_node=models_restapi.PathNode(
                        sha256="e1",
                        action=azm.BinaryAction.Sourced,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(
                            category="plugin",
                            name="generic_plugin",
                            version="2021-01-01T12:00:00+00:00",
                            security="LOW TLP:CLEAR",
                        ),
                        file_format="text/plain",
                        size=1024,
                    ),
                    source=models_restapi.EventSource(
                        security="LOW TLP:CLEAR",
                        name="s3",
                        timestamp="2024-01-22T01:00:00Z",
                        references={"ref2": "val2", "ref1": "val1"},
                        track_source_references="s3.dd6e233ae7a843de99f9b43c349069e4",
                    ),
                ),
                models_restapi.ReadNearbyLink(
                    id="e1.plugin.generic_plugin.sourceds4",
                    child="e1",
                    child_node=models_restapi.PathNode(
                        sha256="e1",
                        action=azm.BinaryAction.Sourced,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(
                            category="plugin",
                            name="generic_plugin",
                            version="2021-01-01T12:00:00+00:00",
                            security="LOW TLP:CLEAR",
                        ),
                        file_format="text/plain",
                        size=1024,
                    ),
                    source=models_restapi.EventSource(
                        security="LOW TLP:CLEAR",
                        name="s4",
                        timestamp="2024-01-22T01:00:00Z",
                        references={"ref2": "val2", "ref1": "val1"},
                        track_source_references="s4.dd6e233ae7a843de99f9b43c349069e4",
                    ),
                ),
                models_restapi.ReadNearbyLink(
                    id="e10.plugin.p12.extracted.e1.plugin.p1.sourced",
                    child="e10",
                    parent="e1",
                    child_node=models_restapi.PathNode(
                        sha256="e10",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2024-01-22T01:02:00Z",
                        author=azm.Author(category="plugin", name="p12", version="1", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format="text/plain",
                        size=1024,
                    ),
                ),
                models_restapi.ReadNearbyLink(
                    id="e100.plugin.generic_plugin.extracted.e10.plugin.p1.sourced",
                    child="e100",
                    parent="e10",
                    child_node=models_restapi.PathNode(
                        sha256="e100",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(
                            category="plugin",
                            name="generic_plugin",
                            version="2021-01-01T12:00:00+00:00",
                            security="LOW TLP:CLEAR",
                        ),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format="text/plain",
                        size=1024,
                    ),
                ),
                models_restapi.ReadNearbyLink(
                    id="e1000.plugin.a1.extracted.e100.plugin.p1.sourced",
                    child="e1000",
                    parent="e100",
                    child_node=models_restapi.PathNode(
                        sha256="e1000",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(category="plugin", name="a1", version="1", security="LOW TLP:CLEAR"),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format="text/plain",
                        size=1024,
                    ),
                ),
                models_restapi.ReadNearbyLink(
                    id="e300.plugin.generic_plugin.extracted.e10.plugin.p1.sourced",
                    child="e300",
                    parent="e10",
                    child_node=models_restapi.PathNode(
                        sha256="e300",
                        action=azm.BinaryAction.Extracted,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(
                            category="plugin",
                            name="generic_plugin",
                            version="2021-01-01T12:00:00+00:00",
                            security="LOW TLP:CLEAR",
                        ),
                        relationship={"random": "data", "action": "extracted", "label": "within"},
                        file_format="text/plain",
                        size=1024,
                    ),
                ),
            ],
        )
        self.assertEqual(8, len(res.links))
        is_checked = False
        for link in res.links:
            if link.child_node.sha256 == "e1":
                self.assertIsNotNone(link.child_node.file_format, "mapped event was selected instead of sourced event")
                self.assertIsNotNone(link.child_node.size, "mapped event was selected instead of sourced event")
                is_checked = True
        self.assertTrue(is_checked, "Checking for the size of the child event never happened.")

        # nearly same as e1
        res = binary_related.read_nearby(self.writer, "e10")
        self.assertEqual(8, len(res.links))
        # same as e1 but missing link to e300
        res = binary_related.read_nearby(self.writer, "e100")
        self.assertEqual(7, len(res.links))
        # parents: e1,e10,e100,generic_1,e2,e20,e200,generic_2
        res = binary_related.read_nearby(self.writer, "e1000")
        self.assertEqual(11, len(res.links))

        res = binary_related.read_nearby(self.writer, "E10")
        self.assertEqual(8, len(res.links))

        # Find cousins or cousin links.
        res = binary_related.read_nearby(self.writer, "e1", True)
        self.assertEqual(8, len(res.links))
        # e10 gains additional link to e200 through e1000
        res = binary_related.read_nearby(self.writer, "e10", True)
        self.assertEqual(9, len(res.links))
        self.assertFormatted(
            res,
            models_restapi.ReadNearby(
                id_focus="e10",
                links=[
                    models_restapi.ReadNearbyLink(
                        id="e1.plugin.generic_plugin.sourcedgeneric_source",
                        child="e1",
                        child_node=models_restapi.PathNode(
                            sha256="e1",
                            action=azm.BinaryAction.Sourced,
                            timestamp="2021-01-01T12:00:00Z",
                            author=azm.Author(
                                category="plugin",
                                name="generic_plugin",
                                version="2021-01-01T12:00:00+00:00",
                                security="LOW TLP:CLEAR",
                            ),
                            file_format="text/plain",
                            size=1024,
                        ),
                        source=models_restapi.EventSource(
                            security="LOW TLP:CLEAR",
                            name="generic_source",
                            timestamp="2021-01-01T11:00:00Z",
                            references={"ref2": "val2", "ref1": "val1"},
                            track_source_references="generic_source.dd6e233ae7a843de99f9b43c349069e4",
                        ),
                    ),
                    models_restapi.ReadNearbyLink(
                        id="e1.plugin.generic_plugin.sourceds2",
                        child="e1",
                        child_node=models_restapi.PathNode(
                            sha256="e1",
                            action=azm.BinaryAction.Sourced,
                            timestamp="2021-01-01T12:00:00Z",
                            author=azm.Author(
                                category="plugin",
                                name="generic_plugin",
                                version="2021-01-01T12:00:00+00:00",
                                security="LOW TLP:CLEAR",
                            ),
                            file_format="text/plain",
                            size=1024,
                        ),
                        source=models_restapi.EventSource(
                            security="LOW TLP:CLEAR",
                            name="s2",
                            timestamp="2024-01-22T01:00:00Z",
                            references={"ref2": "val2", "ref1": "val1"},
                            settings={"custom_password": "passwordValue!"},
                            track_source_references="s2.dd6e233ae7a843de99f9b43c349069e4",
                        ),
                    ),
                    models_restapi.ReadNearbyLink(
                        id="e1.plugin.generic_plugin.sourceds3",
                        child="e1",
                        child_node=models_restapi.PathNode(
                            sha256="e1",
                            action=azm.BinaryAction.Sourced,
                            timestamp="2021-01-01T12:00:00Z",
                            author=azm.Author(
                                category="plugin",
                                name="generic_plugin",
                                version="2021-01-01T12:00:00+00:00",
                                security="LOW TLP:CLEAR",
                            ),
                            file_format="text/plain",
                            size=1024,
                        ),
                        source=models_restapi.EventSource(
                            security="LOW TLP:CLEAR",
                            name="s3",
                            timestamp="2024-01-22T01:00:00Z",
                            references={"ref2": "val2", "ref1": "val1"},
                            track_source_references="s3.dd6e233ae7a843de99f9b43c349069e4",
                        ),
                    ),
                    models_restapi.ReadNearbyLink(
                        id="e1.plugin.generic_plugin.sourceds4",
                        child="e1",
                        child_node=models_restapi.PathNode(
                            sha256="e1",
                            action=azm.BinaryAction.Sourced,
                            timestamp="2021-01-01T12:00:00Z",
                            author=azm.Author(
                                category="plugin",
                                name="generic_plugin",
                                version="2021-01-01T12:00:00+00:00",
                                security="LOW TLP:CLEAR",
                            ),
                            file_format="text/plain",
                            size=1024,
                        ),
                        source=models_restapi.EventSource(
                            security="LOW TLP:CLEAR",
                            name="s4",
                            timestamp="2024-01-22T01:00:00Z",
                            references={"ref2": "val2", "ref1": "val1"},
                            track_source_references="s4.dd6e233ae7a843de99f9b43c349069e4",
                        ),
                    ),
                    models_restapi.ReadNearbyLink(
                        id="e10.plugin.p12.extracted.e1.plugin.p1.sourced",
                        child="e10",
                        parent="e1",
                        child_node=models_restapi.PathNode(
                            sha256="e10",
                            action=azm.BinaryAction.Extracted,
                            timestamp="2024-01-22T01:02:00Z",
                            author=azm.Author(category="plugin", name="p12", version="1", security="LOW TLP:CLEAR"),
                            relationship={"random": "data", "action": "extracted", "label": "within"},
                            file_format="text/plain",
                            size=1024,
                        ),
                    ),
                    models_restapi.ReadNearbyLink(
                        id="e100.plugin.generic_plugin.extracted.e10.plugin.p1.sourced",
                        child="e100",
                        parent="e10",
                        child_node=models_restapi.PathNode(
                            sha256="e100",
                            action=azm.BinaryAction.Extracted,
                            timestamp="2021-01-01T12:00:00Z",
                            author=azm.Author(
                                category="plugin",
                                name="generic_plugin",
                                version="2021-01-01T12:00:00+00:00",
                                security="LOW TLP:CLEAR",
                            ),
                            relationship={"random": "data", "action": "extracted", "label": "within"},
                            file_format="text/plain",
                            size=1024,
                        ),
                    ),
                    models_restapi.ReadNearbyLink(
                        id="e1000.plugin.a1.extracted.e100.plugin.p1.sourced",
                        child="e1000",
                        parent="e100",
                        child_node=models_restapi.PathNode(
                            sha256="e1000",
                            action=azm.BinaryAction.Extracted,
                            timestamp="2021-01-01T12:00:00Z",
                            author=azm.Author(category="plugin", name="a1", version="1", security="LOW TLP:CLEAR"),
                            relationship={"random": "data", "action": "extracted", "label": "within"},
                            file_format="text/plain",
                            size=1024,
                        ),
                    ),
                    models_restapi.ReadNearbyLink(
                        id="e1000.plugin.a2.extracted.e200.plugin.p1.sourced",
                        child="e1000",
                        parent="e200",
                        child_node=models_restapi.PathNode(
                            sha256="e1000",
                            action=azm.BinaryAction.Extracted,
                            timestamp="2021-01-01T12:00:00Z",
                            author=azm.Author(category="plugin", name="a2", version="1", security="LOW TLP:CLEAR"),
                            relationship={"random": "data", "action": "extracted", "label": "within"},
                            file_format="text/plain",
                            size=1024,
                        ),
                    ),
                    models_restapi.ReadNearbyLink(
                        id="e300.plugin.generic_plugin.extracted.e10.plugin.p1.sourced",
                        child="e300",
                        parent="e10",
                        child_node=models_restapi.PathNode(
                            sha256="e300",
                            action=azm.BinaryAction.Extracted,
                            timestamp="2021-01-01T12:00:00Z",
                            author=azm.Author(
                                category="plugin",
                                name="generic_plugin",
                                version="2021-01-01T12:00:00+00:00",
                                security="LOW TLP:CLEAR",
                            ),
                            relationship={"random": "data", "action": "extracted", "label": "within"},
                            file_format="text/plain",
                            size=1024,
                        ),
                    ),
                ],
            ),
        )

        # e100, gains additional links to e300, e200, e20
        res = binary_related.read_nearby(self.writer, "e100", True)
        self.assertEqual(10, len(res.links))
        # e1000 gains additional link to e300
        res = binary_related.read_nearby(self.writer, "e1000", True)
        self.assertEqual(12, len(res.links))

        # Find cousins with large depth (keep searching for cousins regardless of how far away)
        # Finds all available links like e1000 does.
        res = binary_related.read_nearby(self.writer, "e1", True, max_cousin_distance=50)
        self.assertEqual(12, len(res.links))

    def test_read_nearby_file_info(self):
        """ "Test relationship with the following binary relationship structure.

        Paths are as follows: arrow works like this Parent -> Child
        generic_source_1 -> e1 -> e10 -> e100 -> e1000
        generic_source_2 -> e2 -> e20 -> e200 -> e1000
        also e10 -> e300
        """
        a = ("p1", "1")
        # Create a binary event with no size or file format information
        # This is to ensure the event with size and format information is prioritised.
        mapped_action_no_size_info: azm.BinaryEvent = gen.binary_event(
            eid="e1", spathl=[], action=azm.BinaryAction.Mapped
        )
        mapped_action_no_size_info.entity.size = None
        mapped_action_no_size_info.entity.file_format = None
        mapped_action_no_size_info.source.path[0].file_format = None
        mapped_action_no_size_info.source.path[0].size = None

        # Create binary with no file info
        self.write_binary_events(
            [
                mapped_action_no_size_info,
            ]
        )
        # Same binary with file info
        self.write_binary_events([gen.binary_event(eid="e1", spathl=[], sourceit=("s2", "2024-01-22T01:00:00+00:00"))])

        self.assertEqual(0, len(binary_related.read_nearby(self.writer, "gtrdyh6trd").links))

        res = binary_related.read_nearby(self.writer, "e1")
        self.assertFormatted(
            res.links,
            [
                models_restapi.ReadNearbyLink(
                    id="e1.plugin.generic_plugin.mappedgeneric_source",
                    child="e1",
                    child_node=models_restapi.PathNode(
                        sha256="e1",
                        action=azm.BinaryAction.Mapped,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(
                            category="plugin",
                            name="generic_plugin",
                            version="2021-01-01T12:00:00+00:00",
                            security="LOW TLP:CLEAR",
                        ),
                    ),
                    source=models_restapi.EventSource(
                        security="LOW TLP:CLEAR",
                        name="generic_source",
                        timestamp="2021-01-01T11:00:00Z",
                        references={"ref2": "val2", "ref1": "val1"},
                        track_source_references="generic_source.dd6e233ae7a843de99f9b43c349069e4",
                    ),
                ),
                models_restapi.ReadNearbyLink(
                    id="e1.plugin.generic_plugin.sourceds2",
                    child="e1",
                    child_node=models_restapi.PathNode(
                        sha256="e1",
                        action=azm.BinaryAction.Sourced,
                        timestamp="2021-01-01T12:00:00Z",
                        author=azm.Author(
                            category="plugin",
                            name="generic_plugin",
                            version="2021-01-01T12:00:00+00:00",
                            security="LOW TLP:CLEAR",
                        ),
                        file_format="text/plain",
                        size=1024,
                    ),
                    source=models_restapi.EventSource(
                        security="LOW TLP:CLEAR",
                        name="s2",
                        timestamp="2024-01-22T01:00:00Z",
                        references={"ref2": "val2", "ref1": "val1"},
                        track_source_references="s2.dd6e233ae7a843de99f9b43c349069e4",
                    ),
                ),
            ],
        )
