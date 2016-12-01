from GaugeStationIDGrabber import check_for_id
from unittest import TestCase


class TestCheck_for_id(TestCase):
    def test_check_for_id(self):
        self.assertTrue(check_for_id('1029TH'))
        self.assertFalse(check_for_id('invalid'))
        self.assertFalse(check_for_id('03293523'))
        self.assertFalse(check_for_id(''))
        self.assertTrue(check_for_id('E2043'))
