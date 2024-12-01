import unittest
from llm_service import LLMService
import os
from unittest.mock import Mock, patch

class TestLLMService(unittest.TestCase):
    def setUp(self):
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise EnvironmentError("Please set GEMINI_API_KEY environment variable")
        self.service = LLMService(api_key)

    def test_create_context(self):
        self.assertTrue(self.service.create_context("test1"))
        self.assertFalse(self.service.create_context("test1"))

    def test_add_query(self):
        self.service.create_context("test2")
        
        response = self.service.add_query("test2", "What is 2+2?")
        self.assertIsNotNone(response)

        response = self.service.add_query("nonexistent", "Hello")
        self.assertIsNone(response)

    def test_save_answer(self):
        self.service.create_context("test3")

        self.assertTrue(self.service.save_answer("test3", "This is an answer"))

        self.assertFalse(self.service.save_answer("nonexistent", "Answer"))

    def test_get_context(self):
        self.service.create_context("test4")
        self.service.add_query("test4", "Test question")
        self.service.save_answer("test4", "Test answer")

        context = self.service.get_context("test4")
        self.assertIsNotNone(context)
        self.assertIn("Test question", context)
        self.assertIn("Test answer", context)

        self.assertIsNone(self.service.get_context("nonexistent"))

    def test_get_all_contexts(self):
        self.service.create_context("test5a")
        self.service.create_context("test5b")
        
        contexts = self.service.get_all_contexts()
        self.assertIsInstance(contexts, dict)
        self.assertIn("test5a", contexts)
        self.assertIn("test5b", contexts)

if __name__ == '__main__':
    unittest.main()