import unittest
from unittest import mock

import pendulum

from azul_metastore.query import age_off


class TestTypes(unittest.TestCase):
    @mock.patch("pendulum.now", lambda: pendulum.parse("2023-10-10T10:10:10Z"))
    def test_should_delete(self):
        one_day_ms = pendulum.Duration(days=1).in_seconds() * 1000
        # days
        self.assertEqual(False, age_off._should_delete(one_day_ms, "2023-10-09T10:10:11.000Z"))
        self.assertEqual(False, age_off._should_delete(one_day_ms, "2023-10-09T10:10:10.000Z"))
        self.assertEqual(True, age_off._should_delete(one_day_ms, "2023-10-09T10:10:09.000Z"))
        self.assertEqual(True, age_off._should_delete(one_day_ms, "2023-10-09T00:00:00.000Z"))

        ten_days_ms = pendulum.Duration(days=10).in_seconds() * 1000
        self.assertEqual(False, age_off._should_delete(ten_days_ms, "2023-09-30T10:10:11.000Z"))
        self.assertEqual(True, age_off._should_delete(ten_days_ms, "2023-09-30T10:10:09.000Z"))

        # weeks
        one_week_ms = pendulum.Duration(weeks=1).in_seconds() * 1000
        self.assertEqual(False, age_off._should_delete(one_week_ms, "2023-10-03T10:10:11.000Z"))
        self.assertEqual(True, age_off._should_delete(one_week_ms, "2023-10-03T10:10:09.000Z"))

        ten_week_ms = pendulum.Duration(weeks=10).in_seconds() * 1000
        self.assertEqual(False, age_off._should_delete(ten_week_ms, "2023-08-01T10:10:11.000Z"))
        self.assertEqual(True, age_off._should_delete(ten_week_ms, "2023-08-01T10:10:09.000Z"))

        # months
        one_month_ms = pendulum.Duration(months=1).in_seconds() * 1000
        self.assertEqual(False, age_off._should_delete(one_month_ms, "2023-09-10T10:10:11.000Z"))
        self.assertEqual(True, age_off._should_delete(one_month_ms, "2023-09-10T10:10:09.000Z"))

        ten_month_ms = pendulum.Duration(months=10).in_seconds() * 1000
        self.assertEqual(False, age_off._should_delete(ten_month_ms, "2022-12-14T10:10:11.000Z"))
        self.assertEqual(True, age_off._should_delete(ten_month_ms, "2022-12-14T10:10:09.000Z"))

        # years
        one_year_ms = pendulum.Duration(years=1).in_seconds() * 1000
        self.assertEqual(False, age_off._should_delete(one_year_ms, "2022-10-10T10:10:11.000Z"))
        self.assertEqual(True, age_off._should_delete(one_year_ms, "2022-10-10T10:10:09.000Z"))

        ten_years_ms = pendulum.Duration(years=10).in_seconds() * 1000
        self.assertEqual(False, age_off._should_delete(ten_years_ms, "2013-10-12T10:10:11.000Z"))
        self.assertEqual(True, age_off._should_delete(ten_years_ms, "2013-10-12T10:10:09.000Z"))
