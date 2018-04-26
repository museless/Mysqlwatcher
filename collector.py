# -*- coding: utf-8 -*-

import pymysql
import sys
import os
import getopt
import time
import pdb


MIN_SLEEP = 5
SERVER_STATUS = (
    "Aborted_clients",
    "Aborted_connects",
    "Bytes_received",
    "Bytes_sent",
    "Connections",
    "Created_tmp_files",
    "Created_tmp_tables",
    "Created_tmp_disk_tables",
    "Max_execution_time_exceeded",
    "Threads_cached",
    "Threads_created",
    "Threads_running",
)


def collect(datapath, sleep=MIN_SLEEP):
    conn = pymysql.Connection(user="root", password="WdSr0922.")
    cursor = conn.cursor()
    pf = open("{}processlist".format(datapath), "a")
    sf = open("{}globalstatus".format(datapath), "a+")

    t = time.strftime("%Y%m%d%H%M%S")
    status_header = "Time," + ",".join(SERVER_STATUS)

    pos = sf.tell()
    sf.seek(0)
    line = sf.readline().strip("\n")
    sf.seek(pos)

    if line and line != status_header:
        sf = open("{}globalstatus-{}".format(datapath, t), "a")

    if line != status_header:
        sf.write(status_header + "\n")

    while True:
        t = time.strftime("%Y%m%d%H%M%S")

        cursor.execute("show processlist")
        data = cursor.fetchall()

        d = ["{0[1]} {0[2]} {0[3]} {0[4]} {0[5]}".format(d) for d in data]
        d.insert(0, t)

        pf.write("\n".join(d) + "\n\n")
        pf.flush()

        cursor.execute("show global status")
        data = cursor.fetchall()

        vals = {k: v for (k, v) in data}        
        status = (str(vals.get(st, "NaN")) for st in SERVER_STATUS)
        v = "{time} {status}\n".format(time=t, status=" ".join(status))
        sf.write(v)
        sf.flush()

        time.sleep(sleep)


def show_help():
    print("fuck")


def daemon():
    pid = os.fork()

    if pid != 0:
        sys.exit()        

    pid = os.fork()

    os.setsid()

    if pid != 0:
        sys.exit()

    os.chdir("/")

    f = open("/dev/null")
    sys.stdin = sys.stderr = sys.stdout = f


def main(argv=sys.argv):
    longopts = [
        "detach", "sleep=", "help", "datapath="
    ]

    opts, args = getopt.getopt(argv[1: ], "hs:", longopts)
    sleep_time, datapath, daemonable = MIN_SLEEP, "", False

    for (o, a) in opts:
        if o == "--detach":
            daemonable = True

        elif o in ("-s", "--sleep"):
            sleep_time = max(a, MIN_SLEEP)

        elif o in ("-h", "--help"):
            show_help()

        elif o == "--datapath":
            datapath = a

    if not datapath:
        print("collector: missing datapath")
        return

    if daemonable:
        daemon()

    collect(sleep=sleep_time, datapath=datapath)


if __name__ == "__main__":
    main()

