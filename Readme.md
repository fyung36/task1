# Documentaion Of Task
## Implementation Guide
### Tools Used
- Python
- Flask
- Restful API

### Steps to run the project
1. Clone the repository
2. Install the requirements
```bash
pip install -r requirements.txt
```
3. Run app.py
```bash
python app.py
```
4. The server will start running on http://localhost:5000
5. Use Postman or any other API testing tool to test the API endpoints as mentioned below

### Running the tests
1. Run the following command to run the tests
```bash
python -m unittest test_app.py

```

### Project Structure
- app.py - The main file which contains the API endpoints

### API Endpoints
- /api/v1/create_dag - POST
Request Body:
```json
{
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
```

Response:
Success:
```json
{
    "status": "success",
    "message": "DAG with {dag_name} created successfully!"
}
```

# Assumptions
- The task_id is unique for each task in the DAG
- The operator is a valid operator in Airflow
- The python_callable is a valid function in the python file
- The bash_command is a valid bash command
- The schedule_interval is a valid cron expression