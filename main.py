import psycopg2
import schedule
import threading

JOB_STATE = 'c'

POSTGRES_CONNECTION_STRING = "dbname='postgres' user='postgres' port='31000' host='10.20.30.165' password='admin'"

NEED_PROCEDURE_CREATION = True

EXEC_COMMAND_TEXT = 'CALL insert_data(); COMMIT'

PROCEDURE_TEXT = """CREATE OR REPLACE PROCEDURE public.insert_data()
                        LANGUAGE 'plpgsql'
                        AS $BODY$
                        declare
                        -- variable declaration
                            begin
                                insert into public."testTable"(name, speed, freq) 
                                values (substr(md5(random()::text), 0, 5), random() * 400, random() * 5000);
                            end;
                        $BODY$;"""


def createOrReplaceJobProcedure(cursor):
    cursor.execute(PROCEDURE_TEXT)


def scheduledJob(cursor):
    cursor.execute(EXEC_COMMAND_TEXT)
    print("Scheduler tick")


def initConnectionToPostgres(connectionString):
    return psycopg2.connect(connectionString)


def getCursor(connection):
    return connection.cursor()


def threadTask():
    connection = initConnectionToPostgres(POSTGRES_CONNECTION_STRING)
    cursor = getCursor(connection)
    if NEED_PROCEDURE_CREATION:
        createOrReplaceJobProcedure(cursor)
        print("Scheduled procedure was updated")
    schedule.every(10).seconds.do(scheduledJob, cursor=cursor)
    print("Scheduled evaluation of job started...")
    print("Enter 'q' for stop scheduler evaluation...")
    while JOB_STATE != 'q':
        schedule.run_pending()
    schedule.cancel_job(scheduledJob)
    connection.close()


if __name__ == "__main__":
    thread = threading.Thread(target=threadTask)
    thread.start()
    while JOB_STATE != 'q':
        JOB_STATE = input()
    thread.join()
    print("Scheduler finish work", end='')
