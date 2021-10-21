import json
import os
import sys
import psycopg2
import psycopg2.extensions
import codecs
import xml.etree.ElementTree as etree

with open('config.json', 'r') as f:
    config = json.load(f)

lb_script_file_paths = set()
lb_script_file = None

def save_script(db, schema, name, subpath, body, header:str=None, footer:str=None):
    path = os.path.join(db, schema, *subpath)
    os.makedirs(path, exist_ok=True)
    filename = os.path.join(path, name) + '.sql'
    with codecs.open(filename, 'w', 'utf-8-sig') as f:
        if header is not None:
            f.write(header)
        f.write(body)
        if footer is not None:
            f.write(footer)
    path = os.path.join(schema, *subpath)
    return os.path.join(path, name) + '.sql'

def add_filter(sql):
    sql = "select * from (\n" + \
            sql + \
        """
            ) x
            where
            	x.schemaname not like 'information#_schema' ESCAPE '#' and
                x.schemaname not like 'pg#_catalog' ESCAPE '#' and
                --x.schemaname not like 'public' and
                x.schemaname not like 'hdb#_%' ESCAPE '#' and
                x.schemaname not like '#_%' ESCAPE '#'
            order by x.schemaname, x.name
        """
    return sql

def save_functions(cur, db_name):
    lb_script_file.write('\n<!-- FUNCTIONS -->\n')
    sql = """
        select s.nspname as schemaname, p.proname as name, pg_get_functiondef(p.oid) as body
        from pg_proc p
        inner join pg_namespace s on s.oid=p.pronamespace
        inner join pg_language lang on lang.oid=p.prolang and lang.lanname in ('sql', 'plpgsql')
    """
    cur.execute(add_filter(sql))
    for r in cur.fetchall():
        fn = save_script(db_name, r[0], r[1], ('functions',), r[2])
        if fn not in lb_script_file_paths:
            lb_script_file.write(f'<sqlFile path="{fn}" relativeToChangelogFile="true" splitStatements="false" encoding="utf8" />\n')

def save_enums(cur, db_name):
    lb_script_file.write('\n<!-- ENUM -->\n')
    sql = """select n.nspname as schemaname,
                t.typname as name,
                concat('''',
            	    string_agg(e.enumlabel, ''', ''' order by e.enumsortorder),
                '''') as body
            from pg_type t
                join pg_enum e on t.oid = e.enumtypid
                join pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            group by n.nspname, t.typname"""
    cur.execute(add_filter(sql))
    for r in cur.fetchall():
        h = f"CREATE TYPE {r[0]}.{r[1]} AS ENUM (\n\t"
        fn = save_script(db_name, r[0], r[1], ('enums',), r[2], header=h, footer="\n);")
        if fn not in lb_script_file_paths:
            lb_script_file.write(f'<sqlFile path="{fn}" relativeToChangelogFile="true" splitStatements="true" encoding="utf8" />\n')

def save_views(cur, db_name):
    lb_script_file.write('\n<!-- VIEWS -->\n')
    sql = """
    	select schemaname, viewname as name, definition as body
    	from pg_views
    """
    cur.execute(add_filter(sql))
    for r in cur.fetchall():
        h = f"CREATE OR REPLACE VIEW {r[0]}.{r[1]} AS\n"
        fn = save_script(db_name, r[0], r[1], ('views',), r[2], header=h)
        if fn not in lb_script_file_paths:
            lb_script_file.write(f'<sqlFile path="{fn}" relativeToChangelogFile="true" splitStatements="true" encoding="utf8" />\n')

def save_triggers(cur, db_name):
    lb_script_file.write('\n<!-- TRIGGERS -->\n')
    sql = """
        select
        s.nspname as schemaname,
        tr.tgname as name,
        pg_get_triggerdef(tr.oid) as body,
        t.relname as table_name
        from pg_trigger tr
        inner join pg_class t on t.oid=tr.tgrelid
        inner join pg_namespace s on s.oid=t.relnamespace
        where not tr.tgisinternal
    """
    cur.execute(add_filter(sql))
    for r in cur.fetchall():
        h = f"DROP TRIGGER IF EXISTS {r[1]} ON {r[0]}.{r[3]};\n\n"
        fn = save_script(db_name, r[0], r[1], ('triggers',r[3]), r[2], header=h)
        if fn not in lb_script_file_paths:
            lb_script_file.write(f'<sqlFile path="{fn}" relativeToChangelogFile="true" splitStatements="true" encoding="utf8" />\n')

def save_db(cfg):
    global lb_script_file, lb_script_file_paths

    with codecs.open(os.path.join(cfg['database'], 'changelog_post.xml'), 'r', 'utf-8-sig') as f:
        xml = etree.parse(f)
        root = xml.getroot()
        for changeSet in root.findall('{http://www.liquibase.org/xml/ns/dbchangelog}changeSet'):
            for sqlFile in changeSet.findall('{http://www.liquibase.org/xml/ns/dbchangelog}sqlFile'):
                lb_script_file_paths.add(sqlFile.attrib['path'])

    with codecs.open(os.path.join(cfg['database'], 'lb_help_script.txt'), 'w', 'utf-8-sig') as lb_script_file:
        with psycopg2.connect(dbname=cfg['database'], user=cfg['user'], host=cfg['host'], password=cfg['password'], port=cfg.get('port', 5432)) as db:
            with db.cursor() as cur:
                save_enums(cur, cfg['database'])
                save_functions(cur, cfg['database'])
                save_views(cur, cfg['database'])
                save_triggers(cur, cfg['database'])

def main():
    basePath = os.path.dirname(sys.argv[0])
    rootPath = os.path.split(basePath)[0]

    #os.chdir(rootPath)
    os.chdir(basePath)

    save_db(config['db'])

if __name__ == '__main__':
    main()
