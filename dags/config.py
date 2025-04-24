LOG_MAIL = 'd.prokofev@pegas-agro.ru'
PASS_MAIL = 'dLz-D9x-8X4-Qvb'

mail_auth = {'pass': 'dLz-D9x-8X4-Qvb',
             "login": "d.prokofev@pegas-agro.ru",
             "serv": "imap.yandex.ru"}

api_auth = ('api', 'GjkmpjdfntkmFGB')

ORA_AUTH_PROD = {'user': 'data_ex',
                 'password': 'Q',
                 'hostname': 'PA-SRV6',
                 'service_name': 'ORCL',
                 'port': '1521'}

ORA_AUTH_TEST = {'user': 'data_ex',
                 'password': 'Q',
                 'hostname': 'PA-SRV9',
                 'service_name': 'ORACLE',
                 'port': '1521'}

token_test_bot = "5559202586:AAFRV3lVANNtUY5MFNfOHJbboHAdplG8zZU"
token_prod= '5670524214:AAH--yb-F-nKBfNdmI8RP17M5AxeKssgo4s'
token_airflow = '5996442270:AAELMicnB_KiFZLAYStG5D1ovkSlnmCzk1A'

trancate = 'TRUNCATE TABLE "DATA_EX"."EMPLOYES_ENRICHED"'

trancate_test = 'TRUNCATE TABLE "DATA_EX"."EMPLOYES_ENRICHED_TESTICULUS"'

query = 'INSERT INTO data_ex.EMPLOYES_ENRICHED ("NAME", "SURNAME", "PATRONYMIC", "DIVISION", ' \
            '"POSITION", "PERSONNEL_NUMBER", "BIRTHDATE", "PARENT_DIVISION", "EMAIL", "LOGIN", ' \
            '"BARCODE_ERP","UID_USER", "NON_ACTIVE", "WORK_PHONE")' \
            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)'


query_test_table = 'INSERT INTO data_ex.EMPLOYES_ENRICHED_TESTICULUS ("NAME", "SURNAME", "PATRONYMIC", "DIVISION", ' \
            '"POSITION", "PERSONNEL_NUMBER", "BIRTHDATE", "PARENT_DIVISION", "EMAIL", "LOGIN", ' \
            '"BARCODE_ERP", "UID_USER", "WORK_PHONE")' \
            'values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)'

q_erp =  u'Выбрать пДопРекв.Ссылка.ФизическоеЛицо.Фамилия как Familiya, ' \
         u'пДопРекв.Ссылка.ФизическоеЛицо.Имя как Imya, ' \
         u'пДопРекв.Ссылка.ФизическоеЛицо.Отчество как Parentname, ' \
         u'пДопРекв.Ссылка как Пользователь, ' \
         u'пДопРекв.Значение как Barcode, ' \
         u'фДопСвед.Значение UID ' \
         u'Из Справочник.Пользователи.ДополнительныеРеквизиты пДопРекв ' \
         u'Левое Соединение РегистрСведений.ДополнительныеСведения фДопСвед ' \
         u'По пДопРекв.Ссылка.ФизическоеЛицо=фДопСвед.Объект И "УИД_ЗУП"=фДопСвед.Свойство.Имя ' \
         u'Левое Соединение Справочник.ФизическиеЛица.ДополнительныеРеквизиты фДопРекв ' \
         u'По пДопРекв.Ссылка.ФизическоеЛицо=фДопРекв.Ссылка И "Должность"=фДопРекв.Свойство.Имя ' \
         u'Где Не пДопРекв.Ссылка.Недействителен И пДопРекв.Свойство.Имя="ШтрихкодТСД"'

# q_erp = u'Выбрать пДопРекв.Ссылка.ФизическоеЛицо.Фамилия Familiya, ' \
#         u'пДопРекв.Ссылка.ФизическоеЛицо.Имя Imya, ' \
#         u'пДопРекв.Ссылка.ФизическоеЛицо.Отчество Parentname, ' \
#         u'пДопРекв.Ссылка Пользователь, пДопРекв.Значение Barcode, ' \
#         u'пДопРекв.Ссылка.Недействителен=Истина ' \
#         u'Или пДопРекв.Ссылка.ПометкаУдаления=Истина ' \
#         u'Или ЕстьNull(пДопРекв.Ссылка.ФизическоеЛицо.ПометкаУдаления,Ложь)=Истина ' \
#         u'Или ЕстьNull(пДопРекв.Ссылка.ФизическоеЛицо.Родитель.Наименование,"")="Уволенные сотрудники" ' \
#         u'Или фДопСвед.Значение Есть Null non_active, ' \
#         u'фДопСвед.Значение UID, ' \
#         u'пДопРекв.Ссылка.Подразделение, ' \
#         u'фДопРекв.Значение Должность ' \
#         u'Из Справочник.Пользователи.ДополнительныеРеквизиты пДопРекв ' \
#         u'Левое Соединение РегистрСведений.ДополнительныеСведения фДопСвед ' \
#         u'По пДопРекв.Ссылка.ФизическоеЛицо=фДопСвед.Объект ' \
#         u'И "УИД_ЗУП"=фДопСвед.Свойство.Имя ' \
#         u'Левое Соединение Справочник.ФизическиеЛица.ДополнительныеРеквизиты фДопРекв ' \
#         u'По пДопРекв.Ссылка.ФизическоеЛицо=фДопРекв.Ссылка ' \
#         u'И "Должность"=фДопРекв.Свойство.Имя ' \
#         u'Где пДопРекв.Ссылка.ФизическоеЛицо<>Значение(Справочник.ФизическиеЛица.ПустаяСсылка) ' \
#         u'И пДопРекв.Свойство.Имя="ШтрихкодТСД"'


select_oracle = "SELECT NAME, SURNAME FROM AGRO.WHITE_LIST_EMPLOYES"

select_mail = "SELECT *  FROM AGRO.MAILING_SCRIPTS " \
              "WHERE SCRIPT_NAME = 'difference_search_employes'" \
              "AND OTHER_1 <> 'BOSS'" \
              "ORDER BY OTHER_1"

select_boss = "SELECT *  FROM AGRO.MAILING_SCRIPTS WHERE SCRIPT_NAME = 'difference_search_employes' " \
              "AND OTHER_1 = 'BOSS'"

HTTP_AUTH_ZUP = {'user': "api",
             'password': "GjkmpjdfntkmFGB"}

HTTP_URL_PROD = {'stock': 'http://pa-srv2/1c-erp/hs/symph_exch/stock',
                 'products': 'http://pa-srv2/1c-erp/hs/symph_exch/products',
                 'barcode': 'http://it12/test-1c-erp02/hs/symph_exch/nomenclature',
                 'code1c': 'http://it12/test-1c-erp02/hs/symph_exch/nomenclature?code1c=773',
                 'zup': 'http://pa-srv2/1c-zup-pegas/hs/get/employees'
}

path = r"C:\Users\developer\Desktop\Сервисы и аппки\specification.xlsx"

select_ora = """SELECT "NAME", 
"SURNAME", 
"PATRONYMIC", 
"DIVISION", 
"POSITION", 
"PERSONNEL_NUMBER", 
"BIRTHDATE", 
"PARENT_DIVISION", 
"EMAIL", 
"LOGIN", 
"BARCODE_ERP", 
"UID_USER", 
"WORK_PHONE" 
FROM DATA_EX.EMPLOYES_ENRICHED_TESTICULUS"""

insert_atc = 'INSERT INTO data_ex.EMPLOYES_ATS ("NAME", "SURNAME", "WORK_PHONE") values(:1, :2, :3)'
insert_ad = 'INSERT INTO data_ex.EMPLOYES_AD ("SURNAME", "NAME", "MAIL", "LOGIN") values(:1, :2, :3, :4)'
insert_erp = 'INSERT INTO data_ex.EMPLOYES_ERP ("SURNAME", "NAME", "PATRONYMIC" , "FIO", "BARCODE", "UID_PHYS") values(:1, :2, :3, :4, :5, :6)'
insert_zup = 'INSERT INTO data_ex.EMPLOYES_ZUP ("FIO", "DIVISION" , "POSITION" , "PERSONNEL_NUMBER" , "DATE_DISSMISSION", "SURNAME", "NAME", "PATRONYMIC", "BIRTHADAY", "UID_EMPLOYEE", "UID_PHYS", "UID_DIVISION", "UID_POSITION", "PARENT_DIVISION", "UID_PARENT_DIVISION", "ARCHIVE", "DEL") ' \
             'VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17)'

# delete_ats = 'TRUNCATE TABLE DATA_EX."EMPLOYES_ATS"'


#######---test query---######
# test_insert_atc = 'INSERT INTO data_ex.TEST_EMPLOYES_ATS ("NAME", "SURNAME", "WORK_PHONE") values(:1, :2, :3)'
# test_insert_ad = 'INSERT INTO data_ex.TEST_EMPLOYES_AD ("SURNAME", "NAME", "MAIL", "LOGIN", "IS_ACTIVE") values(:1, :2, :3, :4, :5)'
# test_insert_erp = 'INSERT INTO data_ex.TEST_EMPLOYES_ERP ("SURNAME", "NAME", "PATRONYMIC" , "FIO", "BARCODE", "UID_PHYS") values(:1, :2, :3, :4, :5, :6)'
# test_insert_zup = 'INSERT INTO data_ex.TEST_EMPLOYES_ZUP ("FIO", "DIVISION" , "POSITION" , "PERSONNEL_NUMBER" , "DATE_DISSMISSION", "SURNAME", "NAME", "PATRONYMIC", "BIRTHADAY", "UID_EMPLOYEE", "UID_PHYS", "UID_DIVISION", "UID_POSITION", "PARENT_DIVISION", "UID_PARENT_DIVISION", "ARCHIVE", "DEL") ' \
#              'VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17)'
#
#
#
# select_zup = 'SELECT * from DATA_EX.TEST_EMPLOYES_ZUP'
# select_erp = 'SELECT * from DATA_EX.TEST_EMPLOYES_ERP'
# select_ad = 'SELECT * from DATA_EX.TEST_EMPLOYES_AD'

####---prod_query---####
delete_ats = 'TRUNCATE TABLE DATA_EX."EMPLOYES_ATS"'

test_insert_atc = 'INSERT INTO data_ex.EMPLOYES_ATS ("NAME", "SURNAME", "WORK_PHONE") values(:1, :2, :3)'
test_insert_ad = 'INSERT INTO data_ex.EMPLOYES_AD ("SURNAME", "NAME", "MAIL", "LOGIN", "IS_ACTIVE") values(:1, :2, :3, :4, :5)'
test_insert_erp = 'INSERT INTO data_ex.EMPLOYES_ERP ("SURNAME", "NAME", "PATRONYMIC" , "FIO", "BARCODE", "UID_PHYS") values(:1, :2, :3, :4, :5, :6)'
test_insert_zup = 'INSERT INTO data_ex.EMPLOYES_ZUP ("FIO", "DIVISION" , "POSITION" , "PERSONNEL_NUMBER" , "DATE_DISSMISSION", "SURNAME", "NAME", "PATRONYMIC", "BIRTHADAY", "UID_EMPLOYEE", "UID_PHYS", "UID_DIVISION", "UID_POSITION", "PARENT_DIVISION", "UID_PARENT_DIVISION", "ARCHIVE", "DEL", "DATE_ACCOUNTING", "DATE_ADMISSION") VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19)'



select_zup = 'SELECT * from DATA_EX.EMPLOYES_ZUP'
select_erp = 'SELECT * from DATA_EX.EMPLOYES_ERP'
select_ad = 'SELECT * from DATA_EX.EMPLOYES_AD'
select_atc = 'SELECT * from DATA_EX.EMPLOYES_ATS'


select_atc_query_new = '''select Name, Surname, Telephone from "atc_new"'''


mail_message_complain = '''Доброе утро!<br>В прикрепленном файле представлены все заявки по дефектам с датой заполнения за прошлый день.<br>'''


test_mod = 1
if test_mod == 0:
    test_modius = {'lib': r"C:\instantclient_21_9", 'table': 'five_tables.db'}
else:
    test_modius = {'lib': r"/opt/oracle/instantclient_21_9", 'table': '////home/pegas/airflow/airflow.db'}
    
