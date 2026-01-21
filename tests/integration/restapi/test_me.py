import json
import os

from tests.support import integration_test


class TestOpensearchAccess(integration_test.BaseRestapi):
    @classmethod
    def alter_environment(cls):
        super().alter_environment()
        os.environ["security_presets"] = json.dumps(
            ["LOW", "MEDIUM", "HIGH", "MEDIUM REL:APPLE REL:BEE", "MEDIUM REL:APPLE"]
        )

    def test_with_writer(self):
        user = "writer"
        response = self.client.get("/v0/users/me/opensearch", headers={"x-test-user": user})
        self.assertEqual(200, response.status_code)
        resp = response.json()
        resp["account_info"] = {}  # changes depending on cluster you run tests on
        print(resp)
        self.assertEqual(
            resp,
            {
                "account_info": {},
                "security_enabled": True,
                "privileged": True,
                "roles": ["azul_write"],
                "security": {
                    "labels": [
                        "HANOVERLAP",
                        "HIGH",
                        "LOW",
                        "LOW: LY",
                        "MEDIUM",
                        "MOD1",
                        "MOD2",
                        "MOD3",
                        "OVER",
                        "REL:APPLE",
                        "REL:BEE",
                        "REL:CAR",
                        "TLP:AMBER",
                        "TLP:AMBER+STRICT",
                        "TLP:CLEAR",
                        "TLP:GREEN",
                        "TOP HIGH",
                    ],
                    "labels_inclusive": ["REL:APPLE", "REL:BEE", "REL:CAR"],
                    "labels_exclusive": [
                        "HANOVERLAP",
                        "HIGH",
                        "LOW",
                        "LOW: LY",
                        "MEDIUM",
                        "MOD1",
                        "MOD2",
                        "MOD3",
                        "OVER",
                        "TOP HIGH",
                    ],
                    "labels_markings": ["TLP:AMBER", "TLP:AMBER+STRICT", "TLP:CLEAR", "TLP:GREEN"],
                    "unique": "69066aead6c0eb13bcb9ca4323cd10a1",
                    "max_access": "TOP HIGH MOD1 MOD2 MOD3 HANOVERLAP OVER REL:APPLE,BEE,CAR",
                    "allowed_presets": ["LOW", "MEDIUM", "HIGH", "MEDIUM REL:APPLE,BEE", "MEDIUM REL:APPLE"],
                },
            },
        )

    def test_with_low(self):
        user = "low"
        response = self.client.get("/v0/users/me/opensearch", headers={"x-test-user": user})
        self.assertEqual(200, response.status_code)
        resp = response.json()
        resp["account_info"] = {}  # changes depending on cluster you run tests on
        print(resp)
        self.assertEqual(
            resp,
            {
                "account_info": {},
                "security_enabled": True,
                "privileged": False,
                "roles": [
                    "azul-fill1",
                    "azul-fill2",
                    "azul-fill3",
                    "azul-fill4",
                    "azul-fill5",
                    "azul_read",
                    "s-any",
                    "s-low",
                    "s-official",
                ],
                "security": {
                    "labels": ["LOW", "TLP:AMBER", "TLP:AMBER+STRICT", "TLP:CLEAR", "TLP:GREEN"],
                    "labels_inclusive": [],
                    "labels_exclusive": ["LOW"],
                    "labels_markings": ["TLP:AMBER", "TLP:AMBER+STRICT", "TLP:CLEAR", "TLP:GREEN"],
                    "unique": "1b150459d481d5c197570266cf16abef",
                    "max_access": "LOW TLP:AMBER+STRICT",
                    "allowed_presets": ["LOW"],
                },
            },
        )

    def test_with_medium(self):
        user = "med"
        response = self.client.get("/v0/users/me/opensearch", headers={"x-test-user": user})
        self.assertEqual(200, response.status_code)
        resp = response.json()
        resp["account_info"] = {}  # changes depending on cluster you run tests on
        print(resp)
        self.assertEqual(
            resp,
            {
                'account_info': {}, 
                'security_enabled': True, 
                'privileged': False, 
                'roles': [
                    'azul-fill1', 
                    'azul-fill2', 
                    'azul-fill3', 
                    'azul-fill4', 
                    'azul-fill5', 
                    'azul_read', 
                    's-any', 
                    's-low', 
                    's-medium', 
                    's-official', 
                    's-rel-apple'
                ], 
                'security': {
                    'labels': [
                        'LOW', 
                        'MEDIUM', 
                        'REL:APPLE', 
                        'REL:BEE', 
                        'REL:CAR', 
                        'TLP:AMBER', 
                        'TLP:AMBER+STRICT', 
                        'TLP:CLEAR', 
                        'TLP:GREEN'
                    ], 
                    'labels_inclusive': ['REL:APPLE', 'REL:BEE', 'REL:CAR'],
                    'labels_exclusive': ['LOW', 'MEDIUM'], 
                    'labels_markings': ['TLP:AMBER', 'TLP:AMBER+STRICT', 'TLP:CLEAR', 'TLP:GREEN'], 
                    'unique': 'c1d06f8fabe5c6382a56e8c695e74c41', 
                    'max_access': 'MEDIUM REL:APPLE,BEE,CAR', 
                    'allowed_presets': ['LOW', 'MEDIUM', 'MEDIUM REL:APPLE,BEE', 'MEDIUM REL:APPLE']
                }
            },
        )

    def test_with_high_org2(self):
        user = "high_org2"
        response = self.client.get("/v0/users/me/opensearch", headers={"x-test-user": user})
        self.assertEqual(200, response.status_code)
        resp = response.json()
        resp["account_info"] = {}  # changes depending on cluster you run tests on
        print(resp)
        self.assertEqual(
            resp,
            {
                'account_info': {},
                'security_enabled': True,
                'privileged': False,
                'roles': [
                    'azul-fill1',
                    'azul-fill2',
                    'azul-fill3',
                    'azul-fill4',
                    'azul-fill5', 
                    'azul_read', 
                    's-any', 
                    's-high', 
                    's-low', 
                    's-medium', 
                    's-mod1', 
                    's-official', 
                    's-rel-bee'], 
                'security': {
                    'labels': [
                        'HIGH', 
                        'LOW', 
                        'MEDIUM', 
                        'MOD1', 
                        'REL:APPLE', 
                        'REL:BEE', 
                        'TLP:AMBER', 
                        'TLP:AMBER+STRICT', 
                        'TLP:CLEAR', 
                        'TLP:GREEN'
                    ],     
                    'labels_inclusive': ['REL:APPLE', 'REL:BEE'], 
                    'labels_exclusive': ['HIGH', 'LOW', 'MEDIUM', 'MOD1'], 
                    'labels_markings': ['TLP:AMBER', 'TLP:AMBER+STRICT', 'TLP:CLEAR', 'TLP:GREEN'], 
                    'unique': '8eecffba7893ca75415838bbee82dd92', 
                    'max_access': 'HIGH MOD1 REL:APPLE,BEE', 
                    'allowed_presets': ['LOW', 'MEDIUM', 'HIGH', 'MEDIUM REL:APPLE,BEE', 'MEDIUM REL:APPLE']
                },
            },
        )

    def test_with_bad_user(self):
        user = "anonymoose"
        response = self.client.get("/v0/users/me/opensearch", headers={"x-test-user": user})
        self.assertEqual(401, response.status_code)
        resp = response.json()
        print(resp)
        self.assertEqual(
            resp,
            {"detail": "user does not meet minimum_required_access, missing security labels ['LOW']"},
        )
