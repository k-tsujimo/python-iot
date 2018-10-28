#!/usr/bin/python3
# coding: UTF-8

from serial import *
import os, sys, time
from sys import stdout, stdin, stderr
import pymysql.cursors
import datetime
from decimal import Decimal
from urllib.parse import urlparse


def write_log(line):
    try:
        log_file = open('/var/log/python_daemon.log','a')
        log_file.write(str(line) + "\n")
        log_file.close()

    except:
        sys.exit(1)


def open_serial(port):
    # シリアルポートを開く
    try:
        ser = Serial(port)
        write_log('open serial port: {}'.format(port))
        return ser

    except:
        write_log('cannot open serial port: {}'.format(port))
        sys.exit(1)

def close_serial(ser):
    ser.close()
    write_log('close serial port: {}'.format(str(ser)))


def connect_mysql(url):
    # connect to Mysql
    u = urlparse(url)
    try:
        conn = pymysql.connect(
    		host = u.hostname or 'localhost',
		port = u.port or 3306,
		user = u.username or 'root',
		password = u.password or 'password',
		database = u.path[1:] or 'database', 
        	charset='utf8mb4',
        	cursorclass=pymysql.cursors.DictCursor,
        )
        return conn
    except:
        write_log('cannot connect to mysql server {}'.format(url))
        sys.exit(1)


def scan_serial(ser):
    # データを１行ずつ解釈する
    while True:
        try:
            line = ser.readline().rstrip() # １ライン単位で読み出し、末尾の改行コードを削除（ブロッキング読み出し）
            #if line:
            #    write_log(line.decode('utf-8'))
            #else:
            #    write_log('false')
            rd = line.decode('utf-8').split(";")
        except BaseException as e:
            #write_log('except')
            #write_log(e)
            import traceback
            write_log(traceback.print_exc())
        else: 
            if len(rd) == 14:
                new_posts = {"timestamp": int(rd[1]),
                                 "repeater": rd[2],
                                 "lqi": int(rd[3]),
                                 "seq": int(rd[4]),
                                 "child": rd[5],
                                 "voltage": int(rd[6]),
                                 "temperature": Decimal(rd[7]) / 100,
                                 "humidity": Decimal(rd[8]) / 100,
                                 "adc1": int(rd[9]),
                                 "adc2": int(rd[10]),
                                 "sensor": rd[11],
                                 "pressure": int(rd[12]), 
                                 "datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                #print(new_posts)
                try:
                    conn = connect_mysql(url)
                     
                    with conn.cursor() as cursor:
                        # Create a new record
                        sql = "INSERT INTO `myroom_bme280` " \
                            + "(`lqi`, `adc1`, `adc2`, `childsid`, " \
                            + "`humidity`, `pressure`, `repeatersid`, " \
                            + "`sensortype`, `seq`, `temperature`, " \
                            + "`timestamp`, `voltage`, `datetime` " \
                            + " ) VALUES (" \
                            + "%(lqi)s , %(adc1)s, %(adc2)s, %(child)s , " \
                            + "%(humidity)s, %(pressure)s, %(repeater)s, " \
                            + "%(sensor)s, %(seq)s, %(temperature)s, " \
                            + "%(timestamp)s, %(voltage)s, %(datetime)s)"
                        cursor.execute(sql, new_posts)
                        conn.commit()
                except:
                    import traceback
                    write_log(tracebak.print_exc())
            
                finally:
                    conn.close()
                    time.sleep(10) 


def daemonize(ser):
    pid = os.fork()
    if pid > 0:
        write_log('pid: ' + str(pid))
        pid_file = open('/var/run/python_daemon.pid','w')
        pid_file.write(str(pid)+"\n")
        pid_file.close()
        sys.exit()
    
    if pid == 0:
        scan_serial(ser)


if __name__ == '__main__':
    # パラメータの確認
    #   第一引数: シリアルポート名
    if len(sys.argv) != 3:
        write_log('serial port name and url are needed: {}'.format(sys.argv[0]))
        exit(1) 

    url = sys.argv[2]
    serialport = sys.argv[1]

    ser = open_serial(serialport)
    daemonize(ser)

