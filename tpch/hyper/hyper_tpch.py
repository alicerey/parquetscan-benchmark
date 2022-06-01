#!/usr/bin/python3
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName
import timeit
import os

file_path = os.path.abspath(os.path.dirname(__file__))

process_parameters = {
    "log_dir": file_path,
}


with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,parameters=process_parameters) as hyper:
    with Connection(hyper.endpoint, file_path + '/db.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        for i in range(1,23):
            text_file = open(file_path + "/queries/" + str(i) + ".sql", "r")
            data = text_file.read()
            runtimes = []
            for j in range(0,10):
                start = timeit.default_timer()
                connection.execute_list_query(data)
                stop = timeit.default_timer()
                runtimes.append(stop-start)
            print('Query',i, 'Min:', min(runtimes))
            text_file.close()
