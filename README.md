# Scooters Data Uploader

<img src="katalkin-inzhenerka.png" alt="Logo" width="300"/>

Простой инструмент для загрузки данных о скутерах в базу данных PostgreSQL на основе DuckDB
в рамках симулятора [Data Warehouse Analytics Engineer на базе dbt для инженеров и аналитиков данных](https://inzhenerka.tech/dbt)
от школы ИнженеркаТех.

Несмотря на то, что телеграм-бот [dbt Data Bot](https://t.me/inzhenerka_dbt_bot) позволяет проще загрузить данные
в базу данных через интернет, данный проект работает с локальными и приватными базами.

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

## Альтернативный способ

Если приложение по какой-то причине не работает, можно воспользоваться штатными средствами PosgtreSQL (psql, pg_restore),
чтобы создать схему со всеми таблицами из файла [scooters_raw.sql](https://inzhenerka-public.s3.eu-west-1.amazonaws.com/scooters_data_generator/scooters_raw.sql).

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

## Источник данных

Данные созданы в симуляторе поездок [scooters_data_generator](https://github.com/Inzhenerka/scooters_data_generator).
Там же можно найти ссылки на опубликованные parquet-файлы с данными, которые использует данное приложение для загрузки в базу.

## Другие ссылки

- [Чебоксарский кикшеринг покоряет столицу](https://vc.ru/u/206753-farya-roslovets/1103469)
- [Тренажеры по работе с данными от Инженерки](https://inzhenerka.tech/working-with-data)
