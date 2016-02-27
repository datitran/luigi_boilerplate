import unittest
import pipeline

class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.task1 = pipeline.PostgresTask()
        self.task2 = pipeline.SparkTask()

    def test_task1_has_output(self):
        """Check if the method output exists for task1."""
        self.assertTrue(hasattr(self.task1, "output"))

    def test_task1_has_run(self):
        """Check if the method run exists for task1."""
        self.assertTrue(hasattr(self.task1, "run"))

    def test_task2_has_output(self):
        """Check if the method output exists for task2."""
        self.assertTrue(hasattr(self.task2, "output"))

    def test_task2_has_requires(self):
        """Check if the method requires exists for task2."""
        self.assertTrue(hasattr(self.task2, "requires"))
