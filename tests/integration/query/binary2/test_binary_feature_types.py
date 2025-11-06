from tests.support import gen, integration_test


class TestSearchEntity(integration_test.DynamicTestCase):

    def test_offset(self):
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                    sourceit=("s1", "2000-01-01T00:00:00Z"),
                    features=[gen.feature(fv=("f1", "v1"), patch={"offset": 5, "size": 250})],
                )
            ]
        )
        # check result
        resp = list(self.read_binary_events("e1", raw=True))
        result = [x for x in resp if "author" in x and x["author"]["name"] == "a1" and x["source"]["name"] == "s1"][0]

        features = result["features"]

        fv = [x for x in features if x["name"] == "f1"][0]
        self.assertEqual({"gte": 5, "lte": 255}, fv["encoded"]["location"])
        self.assertEqual(5, fv["offset"])
        self.assertEqual(250, fv["size"])

        # not really having an offset, check that lazy normalisation works
        tmp = gen.binary_event(
            eid="e1",
            authornv=("a1", "2"),
            sourceit=("s1", "2000-01-01T00:00:00Z"),
            features=[gen.feature(fv=("f1", "v1"), patch={"offset": None, "size": None, "label": None})],
        )
        tmp.entity.features[0].offset = None
        tmp.entity.features[0].size = None
        tmp.entity.features[0].label = None
        self.write_binary_events([tmp])
        # check result
        resp = list(self.read_binary_events("e1", raw=True))
        result = [x for x in resp if "author" in x and x["author"]["name"] == "a1" and x["source"]["name"] == "s1"][0]
        features = result["features"]

        fv = [x for x in features if x["name"] == "f1"][0]
        print(result)
        self.assertEqual({"name": "f1", "type": "string", "value": "v1", "enriched": {}, "encoded": {}}, fv)

    def test_uris(self):
        self.write_plugin_events(
            [
                gen.plugin(
                    authornv=("a1", "1"),
                    features=[
                        "url1",
                        "url2",
                        "url3",
                        "url4",
                        "url5",
                        "url6",
                        "url7",
                        "url8",
                        "url9",
                        "url10",
                    ],
                )
            ]
        )
        self.write_binary_events(
            [
                gen.binary_event(
                    eid="e1",
                    authornv=("a1", "1"),
                    sourceit=("s1", "2000-01-01T00:00:00Z"),
                    fvtl=[
                        ("url1", "http://myuser@blah.com:443/this/is/the/path.html?qry&morequer#fragmetnts", "uri"),
                        (
                            "url2",
                            "http://myuser:password@blah.com:443/this/is/the/path.html?qry&morequer#fragmetnts",
                            "uri",
                        ),
                        ("url3", "https://blah.com/this/is/the/path.html", "uri"),
                        ("url4", "http://blah.com/this/is/the/path.html", "uri"),
                        ("url5", "http://blah.com", "uri"),
                        ("url6", "http://201.111.20.5", "uri"),
                        ("url7", "http://201.111.20.5/blah/file.txt", "uri"),
                        ("url8", "http://myuser@blah.com:443/this/is/the/path.html?qry&morequer#fragmetnts", "string"),
                        ("url9", "file:///blah/systemctl/file.txt", "uri"),
                        ("url10", "mailto:blah@tomatowine.com", "uri"),
                    ],
                )
            ]
        )

        # check result
        resp = list(self.read_binary_events("e1", raw=True))
        result = [x for x in resp if "author" in x and x["author"]["name"] == "a1" and x["source"]["name"] == "s1"][0]
        features = result["features"]

        fv = [x for x in features if x["name"] == "url1"][0]
        self.assertEqual("http", fv["enriched"]["scheme"])
        self.assertEqual("myuser@blah.com:443", fv["enriched"]["netloc"])
        self.assertEqual("/this/is/the/path.html", fv["enriched"]["filepath"])
        self.assertEqual("qry&morequer", fv["enriched"]["query"])
        self.assertEqual("fragmetnts", fv["enriched"]["fragment"])
        self.assertEqual("myuser", fv["enriched"]["username"])
        self.assertEqual("blah.com", fv["enriched"]["hostname"])
        self.assertEqual(443, fv["enriched"]["port"])
        self.assertEqual(None, fv["enriched"].get("ip"))

        fv = [x for x in features if x["name"] == "url2"][0]
        self.assertEqual("http", fv["enriched"]["scheme"])
        self.assertEqual("myuser:password@blah.com:443", fv["enriched"]["netloc"])
        self.assertEqual("/this/is/the/path.html", fv["enriched"]["filepath"])
        self.assertEqual("qry&morequer", fv["enriched"]["query"])
        self.assertEqual("fragmetnts", fv["enriched"]["fragment"])
        self.assertEqual("myuser", fv["enriched"]["username"])
        self.assertEqual("password", fv["enriched"]["password"])
        self.assertEqual("blah.com", fv["enriched"]["hostname"])
        self.assertEqual(443, fv["enriched"]["port"])
        self.assertEqual(None, fv["enriched"].get("ip"))

        fv = [x for x in features if x["name"] == "url3"][0]
        self.assertEqual("https", fv["enriched"]["scheme"])
        self.assertEqual("blah.com", fv["enriched"]["netloc"])
        self.assertEqual("/this/is/the/path.html", fv["enriched"]["filepath"])
        self.assertEqual(None, fv["enriched"].get("query"))
        self.assertEqual(None, fv["enriched"].get("fragment"))
        self.assertEqual(None, fv["enriched"].get("username"))
        self.assertEqual("blah.com", fv["enriched"]["hostname"])
        self.assertEqual(None, fv["enriched"].get("port"))
        self.assertEqual(None, fv["enriched"].get("ip"))

        fv = [x for x in features if x["name"] == "url4"][0]
        self.assertEqual("http", fv["enriched"]["scheme"])
        self.assertEqual("blah.com", fv["enriched"]["netloc"])
        self.assertEqual("/this/is/the/path.html", fv["enriched"]["filepath"])
        self.assertEqual(None, fv["enriched"].get("query"))
        self.assertEqual(None, fv["enriched"].get("fragment"))
        self.assertEqual(None, fv["enriched"].get("username"))
        self.assertEqual("blah.com", fv["enriched"]["hostname"])
        self.assertEqual(None, fv["enriched"].get("port"))
        self.assertEqual(None, fv["enriched"].get("ip"))

        fv = [x for x in features if x["name"] == "url5"][0]
        self.assertEqual("http", fv["enriched"]["scheme"])
        self.assertEqual("blah.com", fv["enriched"]["netloc"])
        self.assertEqual(None, fv["enriched"].get("path"))
        self.assertEqual(None, fv["enriched"].get("query"))
        self.assertEqual(None, fv["enriched"].get("fragment"))
        self.assertEqual(None, fv["enriched"].get("username"))
        self.assertEqual("blah.com", fv["enriched"]["hostname"])
        self.assertEqual(None, fv["enriched"].get("port"))
        self.assertEqual(None, fv["enriched"].get("ip"))

        fv = [x for x in features if x["name"] == "url6"][0]
        self.assertEqual("http", fv["enriched"]["scheme"])
        self.assertEqual("201.111.20.5", fv["enriched"]["netloc"])
        self.assertEqual(None, fv["enriched"].get("path"))
        self.assertEqual(None, fv["enriched"].get("query"))
        self.assertEqual(None, fv["enriched"].get("fragment"))
        self.assertEqual(None, fv["enriched"].get("username"))
        self.assertEqual("201.111.20.5", fv["enriched"]["hostname"])
        self.assertEqual(None, fv["enriched"].get("port"))
        self.assertEqual("201.111.20.5", fv["enriched"]["ip"])

        fv = [x for x in features if x["name"] == "url7"][0]
        self.assertEqual("http", fv["enriched"]["scheme"])
        self.assertEqual("201.111.20.5", fv["enriched"]["netloc"])
        self.assertEqual("/blah/file.txt", fv["enriched"]["filepath"])
        self.assertEqual(None, fv["enriched"].get("query"))
        self.assertEqual(None, fv["enriched"].get("fragment"))
        self.assertEqual(None, fv["enriched"].get("username"))
        self.assertEqual("201.111.20.5", fv["enriched"]["hostname"])
        self.assertEqual(None, fv["enriched"].get("port"))
        self.assertEqual("201.111.20.5", fv["enriched"]["ip"])

        fv = [x for x in features if x["name"] == "url8"][0]
        self.assertEqual(None, fv["enriched"].get("scheme"))
        self.assertEqual(None, fv["enriched"].get("netloc"))
        self.assertEqual(None, fv["enriched"].get("filepath"))
        self.assertEqual(None, fv["enriched"].get("query"))
        self.assertEqual(None, fv["enriched"].get("fragment"))
        self.assertEqual(None, fv["enriched"].get("username"))
        self.assertEqual(None, fv["enriched"].get("hostname"))
        self.assertEqual(None, fv["enriched"].get("port"))
        self.assertEqual(None, fv["enriched"].get("ip"))

        fv = [x for x in features if x["name"] == "url9"][0]
        self.assertEqual("file", fv["enriched"]["scheme"])
        self.assertEqual(None, fv["enriched"].get("netloc"))
        self.assertEqual("/blah/systemctl/file.txt", fv["enriched"]["filepath"])
        self.assertEqual(None, fv["enriched"].get("query"))
        self.assertEqual(None, fv["enriched"].get("fragment"))
        self.assertEqual(None, fv["enriched"].get("username"))
        self.assertEqual(None, fv["enriched"].get("hostname"))
        self.assertEqual(None, fv["enriched"].get("port"))
        self.assertEqual(None, fv["enriched"].get("ip"))

        fv = [x for x in features if x["name"] == "url10"][0]
        self.assertEqual("mailto", fv["enriched"]["scheme"])
        self.assertEqual(None, fv["enriched"].get("netloc"))
        self.assertEqual("blah@tomatowine.com", fv["enriched"]["filepath"])
        self.assertEqual(None, fv["enriched"].get("query"))
        self.assertEqual(None, fv["enriched"].get("fragment"))
        self.assertEqual(None, fv["enriched"].get("username"))
        self.assertEqual(None, fv["enriched"].get("hostname"))
        self.assertEqual(None, fv["enriched"].get("port"))
        self.assertEqual(None, fv["enriched"].get("ip"))
