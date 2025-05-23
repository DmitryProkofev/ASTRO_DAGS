from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import logging



# Определяем параметры DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
}




def check_data_exists(**context):

    query = """SELECT e.fio, e.date_dissmission, now() AS updated_ad
FROM (
  SELECT 
    fio,
    date_dissmission,
    ROW_NUMBER() OVER (PARTITION BY fio ORDER BY date_dissmission DESC) AS rn
  FROM stage.employes_zup
) e
LEFT JOIN stage.dismissed_employees d ON e.fio = d.fio
WHERE e.rn = 1
  AND e.date_dissmission <> '2099-01-01 00:00:00.000'
  AND d.fio IS NULL"""
    
    try:

        hook = PostgresHook(postgres_conn_id='postgres_prod')

        records = hook.get_records(query)
        if records:
            fio_list = [row[0] for row in records]
            context['ti'].xcom_push(key='dismissed_employees', value=fio_list)
            logging.info("Сотрудники {fio_list} переданы в xcom")
            return True
        return False
    
    except Exception as err:

        logging.error(f"ERROR: {err}")
        raise


# Создание DAG
with DAG(
    'test_email_dag',
    default_args=default_args,
    description='DAG оповещения об уволенных сотрудниках',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Задача для отправки email

    check_data = ShortCircuitOperator(
            task_id='check_data_exists',
            python_callable=check_data_exists,
        )
    

    @task
    def e_mail(**context):

        records = context['ti'].xcom_pull(task_ids='check_data_exists', key='dismissed_employees')
        fio_string = '<br>'.join(records) 

        recepient = ['d.prokofev@pegas-agro.ru']

        LOG_MAIL = 'd.prokofev@pegas-agro.ru'
        PASS_MAIL = 'cwjGdvwkb1mzUTPnMEHN'

        host = 'smtp.mail.ru'
        msg = MIMEMultipart('mixed')
        msg['From'] = formataddr(['Оповещение об уволенных сотрудниках', LOG_MAIL])
        # msg['To'] = formataddr([header_mes, ','.join(recepient)])
        # msg['To'] = ','.join(receivers)
        msg['Subject'] = 'Оповещение об уволенных сотрудниках'  # Тема письма
        #at_ex = MIMEText(open('/home/pegas/report.xlsx', 'rb').read(), 'base64', 'utf-8')
        # at_ex["Content-Type"] = 'text/html'
        # at_ex["Content-Disposition"] = f'attachment; filename=report.xlsx'
        txt_msg = '''
        <html>
        <body>
            <p style="font-size: 16px;">TEXT_HTML<br>
        </body>
        </html>
        '''
        msgText = MIMEText(txt_msg.replace("TEXT_HTML", fio_string), 'html')
        msg.attach(msgText)
        # msg.attach(at_ex)

        try:
            with smtplib.SMTP_SSL(host, 465) as server:
                server.login(LOG_MAIL, PASS_MAIL)
                for el in recepient:
                    server.sendmail(LOG_MAIL, el, msg.as_string())
            logging.info(f"Пользователи {records} успешно отправлены!")
            return True
        except smtplib.SMTPException as err:
            return err, False
    

    @task
    def save_employees():

        sql = """insert into stage.dismissed_employees 
SELECT e.fio, e.date_dissmission, now() AS updated_ad
FROM (
  SELECT 
    fio,
    date_dissmission,
    ROW_NUMBER() OVER (PARTITION BY fio ORDER BY date_dissmission DESC) AS rn
  FROM stage.employes_zup
) e
LEFT JOIN stage.dismissed_employees d ON e.fio = d.fio
WHERE e.rn = 1
  AND e.date_dissmission <> '2099-01-01 00:00:00.000'
  AND d.fio IS NULL"""
        
        try:

            hook = PostgresHook(postgres_conn_id='postgres_prod')
            result = hook.run(sql)

            logging.info(f"Successfully inserted {result} rows into stage.dismissed_employees.")
            
        except Exception as e:

            logging.error(f"Error occurred while saving employees: {e}")
            raise


    check_data >> e_mail() >> save_employees()