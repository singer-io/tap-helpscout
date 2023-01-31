import unittest
from tap_helpscout import transform


class TransformMethods(unittest.TestCase):
    def test_camel_case_converter_method(self):
        """Tests transform fn to convert camel_case to snake_case"""
        self.assertEqual(transform.convert("TestMethod"), "test_method")
        self.assertEqual(transform.convert("threadid"), "threadid")
        self.assertEqual(transform.convert("Conversation_Id"), "conversation__id")

    def test_convert_array(self):
        """Tests the function which converts all the nested dict type `KEYS` in
        list of items from camel to snake case."""
        input_array = ["TestCase", {"TestCaseNumber": 22}, [{"TestSuite": [{"UnitTests": 23}]}]]
        expected_output = ["TestCase", {"test_case_number": 22}, [{"test_suite":
                                                                       [{"unit_tests": 23}]}]]
        self.assertEqual(transform.convert_array(input_array), expected_output)

    def test_convert_json(self):
        """Tests the function which converts all the dict type `Keys` in a dict
        from camel to snake case."""
        input_json = {"CamelCaseKey": "UnitTest",
                      "SnakeCaseKeys": [{"first_name": "tester", "second_name": "dev"}]}
        expected_output = {
            "camel_case_key": "UnitTest",
            "snake_case_keys": [{"first_name": "tester", "second_name": "dev"}],
        }
        self.assertEquals(transform.convert_json(input_json), expected_output)

    def test_denest_embedded_nodes(self):
        """Tests transform_json"""
        mock_input = {"conversations": [{"id": 12345,
                                         "user_updated_at": "2023-01-22T12:00:00Z",
                                         "photo_url": "test_account.jpg",
                                         "_embedded": {"attachments": {"file_name": "input.txt"},
                                                       "emails": ["adbc_test@google.com",
                                                                  "test_acc@google.com"],
                                                       "social_profiles": {
                                                           "linkedin": "https://www.linkedin.com/in"
                                                                       "/test-user-11122330b",
                                                           "facebook": "https://www.fb.com/12345"}}}
                                        ]}
        expected_output = {"conversations": [{"id": 12345,
                                              "user_updated_at": "2023-01-22T12:00:00Z",
                                              "updated_at": "2023-01-22T12:00:00Z",
                                              "photo_url": "test_account.jpg",
                                              "attachments": {"file_name": "input.txt"},
                                              "emails": ["adbc_test@google.com",
                                                         "test_acc@google.com"],
                                              "social_profiles": {
                                                  "linkedin": "https://www.linkedin.com/in"
                                                              "/test-user-11122330b",
                                                  "facebook": "https://www.fb.com/12345"}}]}

        self.assertEquals(transform.transform_json(mock_input, "conversations", "conversations"),
                          expected_output)
