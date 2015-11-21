# fias2postgresql

Импорт ФИАС (http://fias.nalog.ru) в PostgreSQL.


Необходимы:
* *python3.2+* с пакетами [requests](http://docs.python-requests.org/en/latest/), [lxml](http://lxml.de)

* *unrar*

* [*pgdbf*](https://github.com/kstrauser/pgdbf), обязательно с [патчем](https://github.com/kstrauser/pgdbf/commit/baa1d9579274a979aaf2f2d880f5ee566ddeb905). Версия 0.6.2, установленная через homebrew например не годится. [Здесь](https://github.com/bacilla-ru/pgdbf/archive/master.zip) поправленая версия под autotools 1.15.

Запуск sql-скриптов производится посредством ``psql``. База данных и схема данных, в которую производится импорт, должны быть созданы предварительно.

Использование:
```sh
python3 fias.py -d your-db-name -s schema-in-db -u db-user
```

your-db-name - база данных (по умолчанию - fias)

schema-in-db - схема данных (по умолчанию - public)

db-user - пользователь базы данных

Рабочие файлы создаются в текущем каталоге. Требуется ~9Gb свободного места. Скачаный rar-файл с файлами .dbf после работы не удаляется; также, .sql файлы, созданные в процессе работы, пакуются в отдельный файл.
