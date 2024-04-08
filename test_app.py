import unittest
from app import app


class CreateDagTestCase(unittest.TestCase):
    def setUp(self):
        app.testing = True
        self.client = app.test_client()

    def test_create_dag_success(self):
        data = {
            "dag_name": "my_dag",
            "schedule_interval": "0 0 * * *",
            "default_args": {
                "owner": "airflow",
                "depends_on_past": False,
                "start_date": "2024-01-01"
            },
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": "BashOperator",
                    "bash_command": "echo 'Hello World!'"
                },
                {
                    "task_id": "task_2",
                    "operator": "PythonOperator",
                    "python_callable": "print_hello"
                }
            ]
        }

        response = self.client.post('/api/v1/create_dag', json=data)
        json_data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json_data['status'], 'success')
        self.assertEqual(json_data['message'], 'DAG with my_dag created successfully!')

    def test_create_dag_missing_fields(self):
        data = {
            "dag_name": "my_dag",
            # Missing schedule_interval
            "default_args": {
                "owner": "airflow",
                "depends_on_past": False,
                "start_date": "2024-01-01"
            },
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": "BashOperator",
                    "bash_command": "echo 'Hello World!'"
                },
                {
                    "task_id": "task_2",
                    "operator": "PythonOperator",
                    "python_callable": "print_hello"
                }
            ]
        }

        response = self.client.post('/api/v1/create_dag', json=data)
        json_data = response.get_json()

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json_data['message'], 'Schedule interval is required!')

    def test_create_dag_existing_dag(self):
        data = {
            "dag_name": "existing_dag",
            "schedule_interval": "0 0 * * *",
            "default_args": {
                "owner": "airflow",
                "depends_on_past": False,
                "start_date": "2024-01-01"
            },
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": "BashOperator",
                    "bash_command": "echo 'Hello World!'"
                },
                {
                    "task_id": "task_2",
                    "operator": "PythonOperator",
                    "python_callable": "print_hello"
                }
            ]
        }

        response = self.client.post('/api/v1/create_dag', json=data)
        json_data = response.get_json()

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json_data['status'], 'error')
        self.assertEqual(json_data['message'], 'DAG already exists!')


if __name__ == '__main__':
    unittest.main()
