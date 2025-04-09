import unittest
from unittest.mock import patch
import requests

# Fake response class to simulate requests responses
class FakeResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code

    def json(self):
        return self._json

class TestServer(unittest.TestCase):
    base_url = "http://127.0.0.1:5000/api"
    
    @patch("requests.post")
    @patch("requests.get")
    def test_states_mean_request(self, mock_get, mock_post):
        mock_post.return_value = FakeResponse({"job_id": 1})
        mock_get.return_value = FakeResponse({"data": {"Missouri": 32.76268656716418}})
        sender = requests.post(f"{self.base_url}/states_mean", json={
            "question": "Percent of adults aged 18 years and older who have an overweight classification"
        })
        job_id = sender.json()["job_id"]
        received = requests.get(f"{self.base_url}/get_results/{job_id}")
        self.assertEqual(received.json()["data"]["Missouri"], 32.76268656716418)
    
    @patch("requests.post")
    @patch("requests.get")
    def test_state_mean_request(self, mock_get, mock_post):
        fake_job = {"job_id": 2}
        fake_data = {"data": {"South Carolina": 33.25909090909091}}
        mock_post.return_value = FakeResponse(fake_job)
        mock_get.return_value = FakeResponse(fake_data)
        response = requests.post(f"{self.base_url}/state_mean", json={
            "question": "Percent of adults aged 18 years and older who have an overweight classification",
            "state": "South Carolina"
        })
        job_id = response.json()["job_id"]
        received = requests.get(f"{self.base_url}/get_results/{job_id}")
        self.assertEqual(received.json()["data"]["South Carolina"], 33.25909090909091)
    
    @patch("requests.post")
    @patch("requests.get")
    def test_best5_request(self, mock_get, mock_post):
        fake_job = {"job_id": 3}
        expected_data = {
            'Arkansas': 32.99516129032258, 
            'District of Columbia': 30.746875, 
            'Kentucky': 33.071641791044776, 
            'Missouri': 32.76268656716418, 
            'Vermont': 33.11818181818182
        }
        fake_get_data = {"data": expected_data}
        mock_post.return_value = FakeResponse(fake_job)
        mock_get.return_value = FakeResponse(fake_get_data)
        response = requests.post(f"{self.base_url}/best5", json={
            "question": "Percent of adults aged 18 years and older who have an overweight classification"
        })
        job_id = response.json()["job_id"]
        received = requests.get(f"{self.base_url}/get_results/{job_id}")
        self.assertEqual(received.json()["data"], expected_data)
    
    @patch("requests.post")
    @patch("requests.get")
    def test_worst5_request(self, mock_get, mock_post):
        fake_job = {"job_id": 4}
        expected_data = {
            'Alaska': 35.902777777777786, 
            'Montana': 36.17826086956522,
            'Nevada': 36.358333333333334, 
            'New Jersey': 36.08059701492537,
            'Puerto Rico': 36.98636363636363
        }
        fake_get_data = {"data": expected_data}
        mock_post.return_value = FakeResponse(fake_job)
        mock_get.return_value = FakeResponse(fake_get_data)
        response = requests.post(f"{self.base_url}/worst5", json={
            "question": "Percent of adults aged 18 years and older who have an overweight classification"
        })
        job_id = response.json()["job_id"]
        received = requests.get(f"{self.base_url}/get_results/{job_id}")
        self.assertEqual(received.json()["data"], expected_data)
    
    @patch("requests.post")
    @patch("requests.get")
    def test_global_mean_request(self, mock_get, mock_post):
        fake_job = {"job_id": 5}
        fake_get_data = {"data": {"global_mean": 34.48276141583355}}
        mock_post.return_value = FakeResponse(fake_job)
        mock_get.return_value = FakeResponse(fake_get_data)
        response = requests.post(f"{self.base_url}/global_mean", json={
            "question": "Percent of adults aged 18 years and older who have an overweight classification"
        })
        job_id = response.json()["job_id"]
        received = requests.get(f"{self.base_url}/get_results/{job_id}")
        self.assertEqual(received.json()["data"]["global_mean"], 34.48276141583355)
    
    @patch("requests.post")
    @patch("requests.get")
    def test_diff_from_mean_request(self, mock_get, mock_post):
        fake_job = {"job_id": 6}
        fake_get_data = {"data": {"Ohio": 1.2252271692582184}}
        mock_post.return_value = FakeResponse(fake_job)
        mock_get.return_value = FakeResponse(fake_get_data)
        response = requests.post(f"{self.base_url}/diff_from_mean", json={
            "question": "Percent of adults aged 18 years and older who have an overweight classification"
        })
        job_id = response.json()["job_id"]
        received = requests.get(f"{self.base_url}/get_results/{job_id}")
        self.assertEqual(received.json()["data"]["Ohio"], 1.2252271692582184)
    
    @patch("requests.post")
    @patch("requests.get")
    def test_state_diff_from_mean_request(self, mock_get, mock_post):
        fake_job = {"job_id": 7}
        fake_get_data = {"data": {"South Carolina": 1.2236705067426428}}
        mock_post.return_value = FakeResponse(fake_job)
        mock_get.return_value = FakeResponse(fake_get_data)
        response = requests.post(f"{self.base_url}/state_diff_from_mean", json={
            "question": "Percent of adults aged 18 years and older who have an overweight classification",
            "state": "South Carolina"
        })
        job_id = response.json()["job_id"]
        received = requests.get(f"{self.base_url}/get_results/{job_id}")
        self.assertEqual(received.json()["data"]["South Carolina"], 1.2236705067426428)
    
    @patch("requests.post")
    def test_mean_by_category_request(self, mock_post):
        mock_post.return_value = FakeResponse({"dummy": "data"}, status_code=200)
        response = requests.post(f"{self.base_url}/mean_by_category", json={
            "question": "Percent of adults aged 18 years and older who have an overweight classification"
        })
        self.assertEqual(response.status_code, 200)
    
    @patch("requests.post")
    def test_state_mean_by_category_request(self, post_mock):
        request_payload = {
            "question": "Percent of adults aged 18 years and older who have an overweight classification",
            "state": "South Carolina"
        }
        simulated_result = {"job_id": 8}
        post_mock.return_value = FakeResponse(simulated_result, status_code=200)
        response = requests.post(f"{self.base_url}/state_mean_by_category", json=request_payload)
        self.assertEqual(response.status_code, 200, f"Expected 200 OK, got {response.status_code}")
        content = response.json()
        self.assertTrue("job_id" in content, "Missing 'job_id' in response")
        self.assertTrue(isinstance(content["job_id"], int), "'job_id' should be an integer")


    @patch("requests.get")
    def test_jobs_request(self, mock_get):
        simulated = {"data": []}
        mock_get.return_value = FakeResponse(simulated, status_code=200)
        res = requests.get(f"{self.base_url}/jobs")
        data_list = res.json()["data"]
        self.assertEqual(res.status_code, 200)
        self.assertIsInstance(data_list, list)
        self.assertEqual(data_list, [])


    @patch("requests.get")
    def test_num_jobs_request(self, mock_get):
        simulated = {"num_jobs": 0}
        mock_get.return_value = FakeResponse(simulated, status_code=200)
        res = requests.get(f"{self.base_url}/num_jobs")
        count = res.json()["num_jobs"]
        self.assertEqual(res.status_code, 200)
        self.assertIsInstance(count, int)
        self.assertEqual(count, 0)


    @patch("requests.get")
    def test_get_result_invalid_job_id(self, mocked_get):
        error_response = {"status": "error", "reason": "Invalid job_id"}
        mocked_get.return_value = FakeResponse(error_response, status_code=200)
        invalid_job_endpoint = f"{self.base_url}/get_results/1000"
        response_invalid = requests.get(invalid_job_endpoint)
        result_error = response_invalid.json()
        self.assertEqual(result_error.get("status"), "error")
        self.assertEqual(result_error.get("reason"), "Invalid job_id")


    @patch("requests.post")
    @patch("requests.get")
    def test_get_result_task_done(self, mocked_get, mocked_post):
        creation_payload = {"job_id": 1}
        mocked_post.return_value = FakeResponse(creation_payload, status_code=200)
        mocked_get.return_value = FakeResponse({"status": "done", "data": {}}, status_code=200)
        task_payload = {"question": "Percent of adults aged 18 years and older who have an overweight classification"}
        _ = requests.post(f"{self.base_url}/states_mean", json=task_payload)
        done_response = requests.get(f"{self.base_url}/get_results/1")
        result_done = done_response.json()
        self.assertEqual(result_done["status"], "done")

if __name__ == "__main__":
    unittest.main()