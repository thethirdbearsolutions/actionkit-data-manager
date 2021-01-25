import csv
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from django.template.defaultfilters import slugify

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('filename')
        parser.add_argument('tablename')

    def create_table(self, cur, header, tablename):
        print("Schema: %s" % header)
        columns_statement = ', '.join([
            '"%s" char(255)' % col for col in header
        ])
        sql = 'create unlogged table "%s" (%s)' % (tablename, columns_statement)
        print(sql)
        cur.execute(sql)
        
    def handle(self, *args, **options):
        delim = '\t' if options['filename'].endswith('.tsv') else ','
        
        with open(options['filename']) as fp:
            reader = csv.reader(fp, delimiter=delim)
            header = next(reader)
            num_rows = sum(1 for row in reader)
            
        print("Loading file %s with %s rows" % (options['filename'], num_rows))
        tablename = options['tablename']

        cur = connections['default'].cursor()

        try:
            cur.execute("select * from information_schema.tables where table_name=%s",
                        (tablename,))
            if cur.rowcount == 0:
                print("Table %s does not exist, creating..." % tablename)
                self.create_table(cur, header, tablename)

            with open(options['filename']) as fp:
                cur.copy_from(fp, '"%s"' % tablename, sep=delim)

        finally:
            cur.close()
