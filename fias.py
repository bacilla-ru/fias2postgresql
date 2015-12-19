#!/usr/bin/env python3
# coding=utf-8

"""
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
"""

import argparse
from os import path, listdir, rename, unlink
import re
import subprocess
from textwrap import dedent
import requests
from lxml import etree


def shell_cmd(info, cmd):
    print(info)
    if subprocess.call(cmd, shell=True) != 0:
        print('error')
        exit(1)


def run(db, schema, username):
    data = dedent('''\
        <?xml version="1.0" encoding="utf-8"?>
        <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
          <soap12:Body>
            <GetLastDownloadFileInfo xmlns="http://fias.nalog.ru/WebServices/Public/DownloadService.asmx" />
          </soap12:Body>
        </soap12:Envelope>
        ''')
    headers = {'content-type': 'application/soap+xml; charset=utf-8'}
    resp = requests.post('http://fias.nalog.ru/WebServices/Public/DownloadService.asmx', data, headers=headers)
    tree = etree.fromstring(resp.content)
    version = tree.xpath('//fias:VersionId[1]/text()', namespaces={'fias': 'http://fias.nalog.ru/WebServices/Public/DownloadService.asmx'})[0]
    dbf_rar_url = tree.xpath('//fias:FiasCompleteDbfUrl[1]/text()', namespaces={'fias': 'http://fias.nalog.ru/WebServices/Public/DownloadService.asmx'})[0]
    dbf_rar_name = dbf_rar_url.rsplit('/', 1)[1]

    try:
        with open(dbf_rar_name + '.url', 'r') as url_file:
            last_dbf_url = url_file.read().strip()
    except FileNotFoundError:
        last_dbf_url = ''

    if last_dbf_url == dbf_rar_url:
        #print('using already downloaded [{}]'.format(dbf_rar_url))
        print('no updates available')
        return

    shell_cmd('downloading [{}]...'.format(dbf_rar_url), 'curl -O {}'.format(dbf_rar_url))

    shell_cmd('unpacking [{}]...'.format(dbf_rar_name), 'unrar x {}'.format(dbf_rar_name))

    multi = {}
    for dbf in listdir('.'):
        f, ext = path.splitext(dbf)
        if ext == '.DBF':
            cmd = 'pgdbf -s cp866'
            dbt = f + '.DBT'
            if path.exists(dbt):
                cmd += ' -m ' + dbt
            sql = f.lower() + '.sql'
            if not path.exists(sql):
                if schema != 'public':
                    with open(sql, 'w') as sql_file:
                        sql_file.write('SET SCHEMA \'{}\';\n'.format(schema))
                cmd += ' ' + dbf + ' >> ' + sql
                shell_cmd('executing [{}]...'.format(cmd), cmd)

            cmd = 'psql'
            if username != '-':
                cmd += ' -U ' + username
            cmd += ' {} < {}'.format(db, sql)
            shell_cmd('executing [{}]...'.format(cmd), cmd)

            m = re.match(r'^([A-Z]+)\d+', dbf)
            if m:
                multi.setdefault(m.group(1).lower(), []).append(f.lower())

    sql_postprocess = ''
    if schema != 'public':
        sql_postprocess += 'SET SCHEMA \'{}\';\n'.format(schema)
    for table in iter(multi):
        sql_postprocess += 'drop table if exists {};\ncreate table {} as \n'.format(table, table) + '\n union all '.join(
            map(lambda t: 'select * from {}'.format(t), multi[table])) + ';\n'
        sql_postprocess += ''.join(map(lambda t: 'drop table {};\n'.format(t), multi[table]))

    sql_postprocess += dedent('''\
        alter table addrobj add primary key(aoid);
        delete from addrobj where aoid in (select aoid from daddrobj);
        create index on addrobj(aoguid);
        drop table daddrobj;

        drop table if exists tmp_house_dup;
        create table tmp_house_dup as
          select distinct * from house where houseid in (
            select houseid from house group by houseid having count(houseid) > 1);
        create index on tmp_house_dup(houseid);
        create index tmp_idx_house_houseid on house(houseid);
        delete from house where houseid in (select houseid from tmp_house_dup);
        insert into house select * from tmp_house_dup;
        drop table tmp_house_dup;
        drop index tmp_idx_house_houseid;
        alter table house add primary key(houseid);
        delete from house where houseid in (select houseid from dhouse);
        create index on house(aoguid);
        drop table dhouse;

        alter table houseint add primary key(houseintid);
        delete from houseint where houseintid in (select houseintid from dhousint);
        create index on houseint(aoguid);
        drop table dhousint;

        create table if not exists dlandmrk ( landid character varying(36) );
        alter table landmark add primary key(landid);
        delete from landmark where landid in (select landid from dlandmrk);
        create index on landmark(aoguid);
        drop table dlandmrk;

        create index tmp_idx_nordoc_normdocid on nordoc(normdocid);
        create table tmp_nordoc_dup as
          select distinct * from nordoc where normdocid in (
            select normdocid from nordoc group by normdocid having count(normdocid) > 1);
        delete from nordoc where normdocid in (select normdocid from tmp_nordoc_dup);
        insert into nordoc select * from tmp_nordoc_dup;
        drop index tmp_idx_nordoc_normdocid;
        drop table tmp_nordoc_dup;
        alter table nordoc add primary key(normdocid);
        delete from nordoc where normdocid in (select normdocid from dnordoc);
        drop table dnordoc;

        alter table ndoctype alter ndtypeid type int;
        alter table ndoctype add primary key(ndtypeid);
        ''')

    for table, col in (
            ('actstat', 'actstatid'),
            ('centerst', 'centerstid'),
            ('curentst', 'curentstid'),
            ('eststat', 'eststatid'),
            ('hststat', 'housestid'),
            ('intvstat', 'intvstatid'),
            ('operstat', 'operstatid'),
            ('strstat', 'strstatid'),
        ):
        sql_postprocess += dedent('''\
            alter table {} alter {} type numeric(2,0);
            alter table {} add primary key({});
            '''.format(table, col, table, col))

    postprocess_sql_file = '_postprocess.sql'
    with open(postprocess_sql_file, 'w') as sql_file:
        sql_file.write(sql_postprocess)

    cmd = 'psql -U {} {} < {}'.format(username, db, postprocess_sql_file)
    shell_cmd('postprocessing database...', cmd)

    shell_cmd('packing sql files...', 'tar cvzf fias_sql_v{}.tar.gz *.sql'.format(version))

    f, ext = path.splitext(dbf_rar_name)
    rename(dbf_rar_name, '{}_v{}{}'.format(f, version, ext))

    print('cleanup...')
    for file_name in listdir('.'):
        f, ext = path.splitext(file_name)
        if ext in ('.DBF', '.DBT', '.sql'):
            unlink(file_name)

    with open(dbf_rar_name + '.url', 'w') as url_file:
        url_file.write(dbf_rar_url)

    print('success.')


parser = argparse.ArgumentParser(description='Import FIAS DB (http://fias.nalog.ru) into PostgreSQL database.')
parser.add_argument('-d', dest='db', type=str, default='fias', help='database (default: fias)')
parser.add_argument('-s', dest='schema', type=str, default='public', help='schema (default: public)')
parser.add_argument('-u', dest='username', type=str, default='-', help='user')
args = parser.parse_args()

run(args.db, args.schema, args.username)
