#!/usr/bin/python3
import duckdb
import timeit
import os

file_path = os.path.abspath(os.path.dirname(__file__))

con = duckdb.connect(database=':memory:', read_only=False)

for i in range(1,23):
   text_file = open(file_path + "/queries/" + str(i) + ".sql", "r")
   data = text_file.read()
   runtimes = []
   for j in range(0,10):
      start = timeit.default_timer()
      con.execute(query=data)
      stop = timeit.default_timer()
      runtimes.append(stop-start)
   print('Query',i, 'Min:', min(runtimes))
   text_file.close()