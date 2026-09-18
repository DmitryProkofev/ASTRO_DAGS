from flask import abort
from os import environ
import time
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
import pandas as pd


def tryInt(value, name):
    try:
        value = int(value)
    except ValueError:
        abort(400, 'NOK ' + name + ' must be integer, you have sent: ' + str(value))


class NeedEnviron(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        print('calling str')
        if self.message:
            return 'NeedEnviron, {0} '.format(self.message)
        else:
            return 'NeedEnviron has been raised'


def dtC():
    return datetime.today().strftime('%Y-%m-%d')


def getEnvVar(variable: str, if_not_setted=None):
    """
    variable = имя переменной, if_not_setted = что вернуть в случае, если не установлена.
    Функция получения данных из виртуального окружения, проверяет установлена ли переменная.
    """
    if variable in environ:
        return environ[variable]
    elif if_not_setted is not None:
        return if_not_setted
    else:
        raise NeedEnviron

def time_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        ex_time = end_time - start_time
        print(f"Время выполнения функции {func.__name__}: {ex_time}")
        return result
    return wrapper

def e_mail(recepient: list, txt):
    """
    Делает рассылку excel отчетов по почте
    :param recepient: Список почовых адресов
    :param txt: Текст для сообщения
    :return: bool
    """
    LOG_MAIL = 'd.prokofev@pegas-agro.ru'
    PASS_MAIL = 'cwjGdvwkb1mzUTPnMEHN'

    host = 'smtp.mail.ru'
    msg = MIMEMultipart('mixed')
    msg['From'] = formataddr(['Бот качества', LOG_MAIL])
    # msg['To'] = formataddr([header_mes, ','.join(recepient)])
    # msg['To'] = ','.join(receivers)
    msg['Subject'] = 'Отчет по обращениям'  # Тема письма
    at_ex = MIMEText(open('/home/pegas/report.xlsx', 'rb').read(), 'base64', 'utf-8')
    at_ex["Content-Type"] = 'text/html'
    at_ex["Content-Disposition"] = f'attachment; filename=report.xlsx'
    txt_msg = '''
    <html>
    <body>
        <p style="font-size: 16px;">TEXT_HTML<br>
           Успешной работы и хорошего дня!</p>
    </body>
    </html>
    '''
    msgText = MIMEText(txt_msg.replace("TEXT_HTML", txt), 'html')
    msg.attach(msgText)
    msg.attach(at_ex)

    try:
        with smtplib.SMTP_SSL(host, 465) as server:
            server.login(LOG_MAIL, PASS_MAIL)
            for el in recepient:
                server.sendmail(LOG_MAIL, el, msg.as_string())
        return True
    except smtplib.SMTPException as err:
        return err, False
