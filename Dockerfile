FROM quay.io/astronomer/astro-runtime:12.7.1

# 1. Переключаемся на root для установки системных пакетов и настройки ОС
USER root

# 2. Установка системных зависимостей
# (добавил очистку кэша apt, чтобы уменьшить размер образа)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libaio1 \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# 3. Распаковываем Instant Client
WORKDIR /opt/oracle
COPY instantclient-basic-linux.x64-19.32.0.0.0dbru.zip .
RUN unzip instantclient-basic-linux.x64-19.32.0.0.0dbru.zip \
    && rm instantclient-basic-linux.x64-19.32.0.0.0dbru.zip

ENV ORACLE_CLIENT_DIR=/opt/oracle/instantclient_19_32

# 4. Регистрация библиотек + проверка зависимостей
RUN echo "$ORACLE_CLIENT_DIR" > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig \
    && ls -la "$ORACLE_CLIENT_DIR" \
    && (ldd "$ORACLE_CLIENT_DIR/libclntsh.so" | grep "not found" && exit 1 || echo "deps OK")

# 5. ВАЖНО: Возвращаемся к пользователю astro для безопасности и корректной работы Airflow
USER astro

# Дальше ONBUILD-инструкции Astro отработают автоматически (копирование requirements.txt и их установка)