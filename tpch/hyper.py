#!/usr/bin/python3

from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName
import timeit
import sys

if len(sys.argv) != 2:
    print("Please pass the path to the SQL queries")
    sys.exit()

query_path = sys.argv[1]

# Process Parameters
process_parameters = {
    "experimental_external_format_parquet": "on"
}
# Start Hyper
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, 'db.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        print("The connection to the Hyper file is open.")
        # ... use the connection object to send SQL queries to read data
        for i in range(1,23):
            text_file = open(query_path + "/" + str(i) + ".sql", "r")
            data = text_file.read()
            runtimes = []
            for j in range(0,10):
                start = timeit.default_timer()
                connection.execute_list_query(data)
                stop = timeit.default_timer()
                runtimes.append(stop-start)
            print('Query',i, ' Min: ', min(runtimes))
            text_file.close()

    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")