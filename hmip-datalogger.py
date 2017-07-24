# -*- coding: utf-8 -*-
import config
import logging
import sys
from logging import handlers
from operator import attrgetter
from argparse import ArgumentParser
import sqlite3
import homematicip
from builtins import str

def __create_logger():
    logger = logging.getLogger()
    logger.setLevel(config.LOGGING_LEVEL)
    handler = logging.handlers.TimedRotatingFileHandler(config.LOGGING_FILENAME, when='midnight', backupCount=5) if config.LOGGING_FILENAME else logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    return logger

def __create_database():
    conn = __open_database()
    c = conn.cursor()

    c.execute('''CREATE TABLE groups
                 (id text primary key, label text not null, lastStatusUpdate datetime not null)''')

    c.execute('''CREATE TABLE devices
                 (id text primary key, label text not null, group_id text not null, type text not null, lastStatusUpdate datetime not null,
                 FOREIGN KEY(group_id) REFERENCES groups(id))''')

    c.execute('''CREATE TABLE logs
                 (id integer primary key autoincrement, device_id text not null, lastStatusUpdate datetime not null, window_state text, valve_position real, humidity real, actual_temperature real,
                 FOREIGN KEY(device_id) REFERENCES devices(id))''')

    conn.commit()
    __close_database(conn)

def __open_database():
    if not config.DATABASE_FILENAME:
        raise ValueError('No database is configured')

    conn = sqlite3.connect(config.DATABASE_FILENAME)

    return conn

def __close_database(conn):
    if conn:
        conn.close()

def __create_database_devices(home):
    sortedGroups = sorted(home.groups, key=attrgetter('groupType', 'label'))
    db = __open_database()

    for g in sortedGroups:
        if g.groupType == 'META':
            db_cur = db.cursor()
            db_cur.execute('select id from groups where id = "' + g.id + '"')
            data = db_cur.fetchone()

            if data is None:
                db_curInsert = db.cursor()
                db_curInsert.execute('insert into groups (id, label, lastStatusUpdate) values ("' + g.id + '", "' + g.label + '", "' + str(g.lastStatusUpdate) + '")')

    sortedDevices = sorted(home.devices, key=attrgetter('deviceType', 'label'))
    for d in sortedDevices:
        db_cur = db.cursor()
        db_cur.execute('select id from devices where id = "' + d.id + '"')
        data = db_cur.fetchone()

        if data is None:
            groupId = None
            for g in sortedGroups:
                if g.groupType == 'META':
                    for gd in g.devices:
                        if gd.id == d.id:
                            groupId = g.id
                            break

                if groupId is not None:
                    break

            db_curInsert = db.cursor()
            db_curInsert.execute('insert into devices (id, label, group_id, type, lastStatusUpdate) values ("' + d.id + '", "' + d.label + '", "' + groupId + '", "' + d.deviceType + '", "' + str(d.lastStatusUpdate) + '")')

    db.commit()
    __close_database(db)

def __create_log(home):
    db = __open_database()

    sortedDevices = sorted(home.devices, key=attrgetter('deviceType', 'label'))
    for d in sortedDevices:
        db_cur = db.cursor()
        db_cur.execute('select id from logs where device_id = "' + d.id + '" and lastStatusUpdate = "' + str(d.lastStatusUpdate) + '"')
        data = db_cur.fetchone()

        if data is None:
            db_curInsert = db.cursor()

            if d.deviceType == 'HEATING_THERMOSTAT':
                db_curInsert.execute('insert into logs (device_id, lastStatusUpdate, valve_position) values ("' + d.id + '", "' + str(d.lastStatusUpdate) + '", ' + str(d.valvePosition) + ')')
            elif d.deviceType == 'SHUTTER_CONTACT':
                db_curInsert.execute('insert into logs (device_id, lastStatusUpdate, window_state) values ("' + d.id + '", "' + str(d.lastStatusUpdate) + '", "' + d.windowState + '")')
            elif d.deviceType == 'TEMPERATURE_HUMIDITY_SENSOR_DISPLAY' or d.deviceType == 'WALL_MOUNTED_THERMOSTAT_PRO':
                db_curInsert.execute('insert into logs (device_id, lastStatusUpdate, humidity, actual_temperature) values ("' + d.id + '", "' + str(d.lastStatusUpdate) + '", ' + str(d.humidity) + ', ' + str(d.actualTemperature) + ')')

    db.commit()
    __close_database(db)

def main():
    parser = ArgumentParser(description="A cli data logger for the homematicip API")
    parser.add_argument("--debug-level", dest="debug_level", type=int, default=30, help="the debug level which should get used(Critical=50, DEBUG=10)")
    parser.add_argument("--create-database", dest="create_database", action='store_true', help="Create a new database")
    parser.add_argument("--log", dest="log", action='store_true', help="Log homematic devices")

    if len(sys.argv) == 1:
        parser.print_help()
        return

    args = parser.parse_args()

    logger.setLevel(args.debug_level)

    homematicip.init(config.ACCESS_POINT)
    homematicip.set_auth_token(config.AUTH_TOKEN)
    home = homematicip.Home()

    if not home.get_current_state():
        return

    if args.create_database:
        __create_database()

    if args.log:
        __create_database_devices(home)
        __create_log(home)

logger = __create_logger()
if __name__ == "__main__":
    main()
