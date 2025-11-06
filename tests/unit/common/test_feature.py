from azul_metastore.common import feature
from tests.support import unit_test


class TestTypes(unit_test.BaseUnitTestCase):
    def test_falsy(self):
        r = feature._parse_feature_value("0", "integer")
        self.assertEqual(0, r["integer"])

        r = feature._parse_feature_value("0", "float")
        self.assertEqual(0, r["float"])

    def test_uri(self):
        url = "http://myuser:pw@11.212.31.78:443/this/is/the/path.html;parm?qry&morequer#fragmetnts"
        r = feature._parse_feature_value(url, "uri")
        self.assertEqual("http", r["scheme"])
        self.assertEqual("myuser:pw@11.212.31.78:443", r["netloc"])
        self.assertEqual("/this/is/the/path.html", r["filepath"])
        self.assertEqual("parm", r["params"])
        self.assertEqual("qry&morequer", r["query"])
        self.assertEqual("fragmetnts", r["fragment"])
        self.assertEqual("myuser", r["username"])
        self.assertEqual("pw", r["password"])
        self.assertEqual("11.212.31.78", r["hostname"])
        self.assertEqual(443, r["port"])
        self.assertEqual("11.212.31.78", r["ip"])

        url = "http://website.com"
        r = feature._parse_feature_value(url, "uri")
        self.assertEqual("http", r["scheme"])
        self.assertEqual("website.com", r["netloc"])
        self.assertTrue("filepath" not in r)
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)
        self.assertEqual("website.com", r["hostname"])
        self.assertTrue("port" not in r)
        self.assertTrue("ip" not in r)

    def test_uri_noscheme(self):
        # not strictly url's but we also transport host info via this type
        url = "10.20.111.5"
        r = feature._parse_feature_value(url, "uri")
        self.assertTrue("scheme" not in r)
        self.assertEqual("10.20.111.5", r["netloc"])
        self.assertTrue("filepath" not in r)
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)
        self.assertEqual("10.20.111.5", r["hostname"])
        self.assertTrue("port" not in r)
        self.assertEqual("10.20.111.5", r["ip"])

        url = "10.20.111.5:32000"
        r = feature._parse_feature_value(url, "uri")
        self.assertTrue("scheme" not in r)
        self.assertEqual("10.20.111.5:32000", r["netloc"])
        self.assertTrue("filepath" not in r)
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)
        self.assertEqual("10.20.111.5", r["hostname"])
        self.assertEqual(32000, r["port"])
        self.assertEqual("10.20.111.5", r["ip"])

        url = "foo.bar.info"
        r = feature._parse_feature_value(url, "uri")
        self.assertTrue("scheme" not in r)
        self.assertEqual("foo.bar.info", r["netloc"])
        self.assertTrue("filepath" not in r)
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)
        self.assertEqual("foo.bar.info", r["hostname"])
        self.assertTrue("port" not in r)
        self.assertTrue("ip" not in r)

        url = "foo.bar.info:123"
        r = feature._parse_feature_value(url, "uri")
        self.assertTrue("scheme" not in r)
        self.assertEqual("foo.bar.info:123", r["netloc"])
        self.assertTrue("filepath" not in r)
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)
        self.assertEqual("foo.bar.info", r["hostname"])
        self.assertEqual(123, r["port"])
        self.assertTrue("ip" not in r)

    def test_uri_ipv6(self):
        url = "[::1]"
        r = feature._parse_feature_value(url, "uri")
        self.assertTrue("scheme" not in r)
        self.assertEqual("[::1]", r["netloc"])
        self.assertTrue("filepath" not in r)
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)
        self.assertEqual("::1", r["hostname"])
        self.assertTrue("port" not in r)
        self.assertEqual("::1", r["ip"])

        url = "[::1]:11"
        r = feature._parse_feature_value(url, "uri")
        self.assertTrue("scheme" not in r)
        self.assertEqual("[::1]:11", r["netloc"])
        self.assertTrue("filepath" not in r)
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)
        self.assertEqual("::1", r["hostname"])
        self.assertEqual(11, r["port"])
        self.assertEqual("::1", r["ip"])

        url = "https://myuser@[2a03:2880:f003:c07:face:b00c::2]/login"
        r = feature._parse_feature_value(url, "uri")
        self.assertEqual("https", r["scheme"])
        self.assertEqual("myuser@[2a03:2880:f003:c07:face:b00c::2]", r["netloc"])
        self.assertEqual("/login", r["filepath"])
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertEqual("myuser", r["username"])
        self.assertTrue("password" not in r)
        self.assertEqual("2a03:2880:f003:c07:face:b00c::2", r["hostname"])
        self.assertTrue("port" not in r)
        self.assertEqual("2a03:2880:f003:c07:face:b00c::2", r["ip"])

        url = "https://myuser@[2a03:2880:f003:c07:face:b00c::2]:443/login"
        r = feature._parse_feature_value(url, "uri")
        self.assertEqual("https", r["scheme"])
        self.assertEqual("myuser@[2a03:2880:f003:c07:face:b00c::2]:443", r["netloc"])
        self.assertEqual("/login", r["filepath"])
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertEqual("myuser", r["username"])
        self.assertTrue("password" not in r)
        self.assertEqual("2a03:2880:f003:c07:face:b00c::2", r["hostname"])
        self.assertTrue(443, r["port"])
        self.assertEqual("2a03:2880:f003:c07:face:b00c::2", r["ip"])

    def test_uri_no_slashes(self):
        """Add cases for uris which lead with http(s): to ensure they work correctly."""
        # Http equivalent
        r = feature._parse_feature_value("http:onvinced.lf", "uri")
        self.assertEqual("http", r["scheme"])
        self.assertEqual("onvinced.lf", r["filepath"])
        self.assertCountEqual(["scheme", "filepath"], list(r.keys()))

        # Https equivalent
        r = feature._parse_feature_value("https:onvinced.lf", "uri")
        self.assertEqual("https", r["scheme"])
        self.assertEqual("onvinced.lf", r["filepath"])
        self.assertCountEqual(["scheme", "filepath"], list(r.keys()))

    def test_path(self):
        r = feature._parse_feature_value("C:\\windows\\system43\\evil.exe", "filepath")
        self.assertEqual("C:/windows/system43/evil.exe", r["filepath"])
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)

        r = feature._parse_feature_value("evil.exe", "filepath")
        self.assertEqual("evil.exe", r["filepath"])

        r = feature._parse_feature_value("/bin/some/path/to/file", "filepath")
        self.assertEqual("/bin/some/path/to/file", r["filepath"])
        r = feature._parse_feature_value("/bin/some/path/to/file", "filepath")
        self.assertEqual("/bin/some/path/to/file", r["filepath"])
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)

        r = feature._parse_feature_value("a_single_file", "filepath")
        self.assertEqual("a_single_file", r["filepath"])
        self.assertTrue("params" not in r)
        self.assertTrue("query" not in r)
        self.assertTrue("fragment" not in r)
        self.assertTrue("username" not in r)
        self.assertTrue("password" not in r)

        r = feature._parse_feature_value("/opt/evil\\file\\from\\linux", "filepath")
        self.assertEqual("/opt/evil\\file\\from\\linux", r["filepath"])

        # consequence of combining win + unix
        r = feature._parse_feature_value("evil\\file\\from\\linux", "filepath")
        self.assertEqual("evil/file/from/linux", r["filepath"])
