from airflow import DAG
from airflow.models import DagModel
from flask import Flask, request, jsonify
from error_handler import AirflowDAGCreationError

app = Flask(__name__)


def dag_exists(dag_name):
    """
    Check if DAG already exists in Airflow
    """
    # Check if there's a DAG model with the given name in the database
    if DagModel.get_dagmodel(dag_id=dag_name) is not None:
        return True
    return False  # Mock Implementation for now


def create_dag_in_airflow(data):
    """
    Create DAG in Airflow
    """
    dag_name = data.get('dag_name')
    schedule_interval = data.get('schedule_interval')
    default_args = data.get('default_args')
    tasks = data.get('tasks')

    # Create a new DAG object
    dag = DAG(
        dag_name,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False  # Optional
    )

    # Add tasks to the DAG
    for task_data in tasks:
        task_id = task_data.get('task_id')
        operator = task_data.get('operator')

        if operator == "BashOperator":
            from airflow.operators.bash_operator import BashOperator
            bash_command = task_data.get('bash_command')
            task = BashOperator(
                task_id=task_id,
                bash_command=bash_command,
                dag=dag
            )
        elif operator == "PythonOperator":
            from airflow.operators.python_operator import PythonOperator
            python_callable = globals().get(task_data.get('python_callable'))
            task = PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                dag=dag
            )
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    # Optionally, persist the DAG to the database
    dag.sync_to_db()
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
        if dag:
            response_message = f"DAG with {data.get('dag_name')} created successfully!"
            return jsonify({"status": "success", "message": response_message})
    except AirflowDAGCreationError as e:
        return jsonify({"status": "error", 'message': 'Error creating DAG: {}'.format(str(e))}), 500
    except Exception as e:
        return jsonify({"status": "error", 'message': 'Unknown error occurred: {}'.format(str(e))}), 500


if __name__ == '__main__':
    app.run(debug=True)
