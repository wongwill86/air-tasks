import unittest
from airflow import settings
from airflow.models import DagBag


class TestCompileDags(unittest.TestCase):
    def test_dags_should_compile(self):
        assert DagBag(settings.DAGS_FOLDER)
