class AirflowDAGCreationError(Exception):
    """
    Exception raised when there is an error creating a DAG in Airflow.
    """

    def __init__(self, message="Error creating DAG in Airflow"):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message}"