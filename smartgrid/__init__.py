"""Shared, framework-free logic for the Smart-Grid Energy Monitoring and Billing pipeline.

Everything in this package is plain Python (plus psycopg2 in ``db``/``billing_job``/``queries``)
so it can be imported by the simulator, the Spark driver, the Airflow DAG, the dashboard,
the API and the tests without pulling in any heavy framework.
"""
