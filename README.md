# Scooters Data Uploader

Простой инструмент для загрузки данных о скутерах в базу данных PostgreSQL на основе DuckDB

## Клонирование репозитория

Для получения кода с GitHub выполните команду:

```bash
git clone https://github.com/Inzhenerka/scooters_data_uploader.git
```

Затем перейдите в директорию с проектом:

```bash
cd scooters_data_uploader
```

## Установка зависимостей

Требуется **Python 3.9** или новее. Для установки зависимостей выполните команду:

```bash
pip install -r requirements.txt
```

## Подготовка адреса базы данных

Нужно подготовить адрес базы данных в формате Database URI:

```
postgresql://<user>:<password>@<host>:<port>/<database>
```

Пример (стандартный адрес для локального PostgreSQL):

```
postgresql://postgres:postgres@localhost:5432/postgres
```

## Загрузка данных

Для загрузки данных из удаленного репозитория в базу данных выполните команду `upload`,
передав адрес базы данных в качестве аргумента:

```bash
python uploader.py upload <database_uri>
```

Пример:

```bash
python uploader.py upload postgresql://postgres:postgres@localhost:5432/postgres
```

## Проверка свежести данных

Для проверки свежести данных в базе выполните команду `version`, передав адрес базы данных в качестве аргумента:

```bash
python uploader.py version <database_uri>
```

Пример:

```bash
python uploader.py version postgresql://postgres:postgres@localhost:5432/postgres
```

## Помощь

Для получения справки по использованию утилиты выполните команды:

```bash
python uploader.py --help
python uploader.py upload --help
python uploader.py version --help
```
