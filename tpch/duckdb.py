#!/usr/bin/python3
import duckdb
import timeit
import sys
con = duckdb.connect(database=':memory:', read_only=False)


if len(sys.argv) != 2:
    print("Please pass the path to the SQL queries")
    sys.exit()

query_path = sys.argv[1]


for i in range(1,23):
   text_file = open(query_path + "/" + str(i) + ".sql", "r")
   data = text_file.read()
   runtimes = []
   for j in range(0,10):
      start = timeit.default_timer()
      con.execute(query=data)
      stop = timeit.default_timer()
      runtimes.append(stop-start)
   print('Query',i, ' Min: ', min(runtimes))
   text_file.close()