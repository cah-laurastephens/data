import datetime
import json
import os
import pandas as pd
import pandas_profiling  # sudo pip3 install pandas-profiling --upgrade --ignore-installed six
import sys
from sqlalchemy import create_engine


def profile_table(engine, db_name, table, parms):
    cnn = engine.connect()
    sql_col = "select column_name from information_schema.columns where table_name = '%s' order by ordinal_position" % table
    cols = []
    result = cnn.execute(sql_col)

    for r in result:
        cols.append('[' + r.column_name + ']')

    outfile = cfg['Output_path'] + db_name + '-' + table + '.html'
    sql_profile = 'select %s %s from %s %s' % (parms['limit'], ",".join(cols), table, parms['order_by'])
    print('DB: %s\tTable: %s\tFile: %s' % (db_name, table, outfile))
    print('\t' + sql_profile)

    df = pd.read_sql(sql_profile, engine)
    x = pandas_profiling.ProfileReport(df)
    x.to_file(outputfile=outfile)
    del df

    cnn.close()


def process(db_name, db):
    # env_var = db['connection_string']
    #
    # for x in os.environ:
    #     print('X: %s', x)
    #
    # foo = os.environ['PAYCOG_DB_URL']
    engine = create_engine('mssql+pymssql://paycog:Fuse2018@QDBSBDWV01.corp.rghent.com/MarketingDW')
    for table in db['tables']:
        profile_table(engine, db_name, table, db['tables'][table])


if __name__ == "__main__":
    start = datetime.datetime.now()

    with open(sys.argv[1], "r") as cfg_file:
        cfg = json.load(cfg_file)

    for dbs in cfg:
        if dbs == 'Databases':
            for db in cfg[dbs]:
                process(db, cfg[dbs][db])

    end = datetime.datetime.now()
    print("Time to complete: {0:.{1}f} seconds".format((end - start).total_seconds(), 3))