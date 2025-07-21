import pandas as pd
import requests
import sqlalchemy as sa
from config import api_auth
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator 
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
from sql.analogues.query import query