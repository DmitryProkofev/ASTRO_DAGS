# dags/branch_without_trigger.py
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="branch_without_trigger",
    schedule="@once",
    start_date=pendulum.datetime(2019, 2, 28, tz="UTC"),
    catchup=False,
) as dag:

    run_this_first = EmptyOperator(task_id="run_this_first")

    @task.branch
    def do_branching():
        return "branch_a"  # или верни список task_id, если несколько

    branching = do_branching()

    branch_a = EmptyOperator(task_id="branch_a")
    follow_branch_a = EmptyOperator(task_id="follow_branch_a")
    branch_false = EmptyOperator(task_id="branch_false")

    join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # Чтобы join не скипался
    )

    run_this_first >> branching
    branching >> branch_a >> follow_branch_a >> join
    branching >> branch_false >> join
