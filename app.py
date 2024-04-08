from flask import Flask, request, jsonify

app = Flask(__name__)


def dag_exists(dag_name):
    """
    Check if DAG already exists in Airflow
    """
    return False  # Mock Implementation for now


def create_dag_in_airflow(data):
    """
    Create DAG in Airflow
    """
    return True  # Mock Implementation for now


# Endpoint to Create DAG in Airflow
@app.route('/api/v1/create_dag', methods=['POST'])
def create_dag():
    """
    Create DAG in Airflow

    Request Body:
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

    Response:
    {
        "status": "success",
        "message": "DAG with {dag_name} created successfully!"
    }

    {
        "status": "error",
        "message": "Error creating DAG!"
    }

    {
        "status": "error",
        "message": "DAG already exists!"
    }
    """
    data = request.json

    # Data Validation
    if not data.get('dag_name'):
        return jsonify({'message': 'DAG name is required!'}), 400
    if not data.get('schedule_interval'):
        return jsonify({'message': 'Schedule interval is required!'}), 400
    if not data.get('default_args'):
        return jsonify({'message': 'Default args is required!'}), 400
    if not data.get('tasks'):
        return jsonify({'message': 'Tasks is required!'}), 400

    # Check if DAG already exists
    # If exists, return error
    if dag_exists(data.get('dag_name')):
        return jsonify({"status": "error", 'message': 'DAG already exists!'}), 400

    # Simulate creating DAG in Airflow
    try:
        dag = create_dag_in_airflow(data)
    except Exception as e:
        return jsonify({"status": "error", 'message': 'Error creating DAG!'}), 500

    if not dag:
        return jsonify({"status": "error", 'message': 'Error creating DAG!'}), 500

    response_message = f"DAG with {data.get('dag_name')} created successfully!"
    return jsonify({"status": "success", "message": response_message})


if __name__ == '__main__':
    app.run(debug=True)
