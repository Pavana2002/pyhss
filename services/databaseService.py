import os, sys, json, yaml
import uuid, time
import asyncio
import socket
import datetime
import traceback
sys.path.append(os.path.realpath('../lib'))
from messagingAsync import RedisMessagingAsync
from banners import Banners
from logtool import LogTool
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table

class DatabaseService:
    """
    Redis-Database Cache Service
    Functions as an asynchronous cache for a database.
    Currently read-only.
    """

    def __init__(self, redisHost: str='127.0.0.1', redisPort: int=6379):
        try:
            with open("../config.yaml", "r") as self.configFile:
                self.config = yaml.safe_load(self.configFile)
        except:
            print(f"[Database] Fatal Error - config.yaml not found, exiting.")
            quit()
        self.logTool = LogTool(self.config)
        self.banners = Banners()

        self.redisUseUnixSocket = self.config.get('redis', {}).get('useUnixSocket', False)
        self.redisUnixSocketPath = self.config.get('redis', {}).get('unixSocketPath', '/var/run/redis/redis-server.sock')
        self.redisHost = self.config.get('redis', {}).get('host', 'localhost')
        self.redisPort = self.config.get('redis', {}).get('port', 6379)
        self.redisDatabaseReadMessaging = RedisMessagingAsync(host=self.redisHost, port=self.redisPort, useUnixSocket=self.redisUseUnixSocket, unixSocketPath=self.redisUnixSocketPath)
        self.redisLogMessaging = RedisMessagingAsync(host=self.redisHost, port=self.redisPort, useUnixSocket=self.redisUseUnixSocket, unixSocketPath=self.redisUnixSocketPath)
        self.hostname = socket.gethostname()

        supportedDatabaseTypes = ["mysql", "postgresql"]
        self.databaseType = self.config.get('database', {}).get('db_type', 'mysql').lower()
        if not self.databaseType in supportedDatabaseTypes:
            print(f"[Database] Fatal Error - unsupported database type: {self.databaseType}. Supported database types are: {supportedDatabaseTypes}, exiting.")
            quit()

        self.databaseHost = self.config.get('database', {}).get('server', '')
        self.databaseUsername = self.config.get('database', {}).get('username', '')
        self.databasePassword = self.config.get('database', {}).get('password', '')
        self.database = self.config.get('database', {}).get('database', '')
        self.readCacheEnabled = self.config.get('database', {}).get('readCacheEnabled', True)
        self.cacheReadInterval = int(self.config.get('database', {}).get('cacheReadInterval', 60))
        
        # Disable TLS for MySQL
        connect_args = {}
        if self.databaseType == "mysql":
            connect_args={"ssl_mode": "DISABLED"}   

        echo = self.config['logging'].get('sqlalchemy_sql_echo', False) 
        pool_recycle=self.config['logging'].get('sqlalchemy_pool_recycle', 3600)
        pool_size=self.config['logging'].get('sqlalchemy_pool_size', 20)
        max_overflow=self.config['logging'].get('sqlalchemy_max_overflow', 10)
        pool_timeout=30

        self.sqlAlchemyEngine = create_engine(
            f"{self.databaseType}://{self.databaseUsername}:{self.databasePassword}@{self.databaseHost}/{self.database}",
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
            pool_timeout=pool_timeout,
            connect_args=connect_args,
        )
        self.sqlAlchemySession = sessionmaker(bind=self.sqlAlchemyEngine)

    def sanitizeJson(self, obj):
        """
        Handles general JSON sanitizaion.
        """

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        raise TypeError(f'Object of type {type(obj).__name__} is not JSON serializable')

    def safeClose(self, databaseSession):
        try:
            if databaseSession.is_active:
                databaseSession.close()
        except Exception as E:
            self.logTool.log(service='Database', level='error', message=f"[Database] [safeClose] Failed to safely close session: {traceback.format_exc()}", redisClient=self.redisLogMessaging)

import json
import traceback
from sqlalchemy import MetaData, select
import asyncio

async def readDatabase(self):
    """
    Efficient DB -> Redis reader:
    - Reflects metadata once at startup
    - Streams rows via fetchmany and builds batches
    - Sends batches using RedisMessagingAsync.sendBulkMessage
    """

    # safety: check expected attributes (adapt if your attribute names differ)
    engine = getattr(self, "sqlAlchemyEngine", None)
    redis_messaging = getattr(self, "redisDatabaseReadMessaging", None)
    session_factory = getattr(self, "sqlAlchemySession", None)
    cache_interval = getattr(self, "cacheReadInterval", 5)
    hostname = getattr(self, "hostname", "unknown-host")
    batch_size = getattr(self, "db_read_batch_size", 200)  # tunable

    if engine is None or redis_messaging is None:
        # Nothing to do; log and exit the coroutine
        try:
            self.logTool.log(service='Database', level='error',
                              message="[readDatabase] Missing engine or redisDatabaseReadMessaging. Exiting readDatabase.",
                              redisClient=getattr(self, 'redisLogMessaging', None))
        except Exception:
            pass
        return

    # Reflect once at startup
    try:
        metadata = MetaData()
        with engine.connect() as conn:
            metadata.reflect(bind=conn)
    except Exception:
        self.logTool.log(service='Database', level='error',
                         message=f"[readDatabase] Failed to reflect metadata: {traceback.format_exc()}",
                         redisClient=getattr(self, 'redisLogMessaging', None))
        return

    # Build a stable map: table_name -> (TableObj, primary_key_name)
    table_map = {}
    for tname, table_obj in metadata.tables.items():
        pk_cols = [c.name for c in table_obj.primary_key.columns]
        if not pk_cols:
            # skip tables without PK
            continue
        table_map[tname] = (table_obj, pk_cols[0])

    # Main loop: iterate tables and stream rows in chunks
    while True:
        try:
            with engine.connect() as conn:
                # Use streaming results option to avoid grabbing everything at once
                for table_name, (table_obj, pkname) in table_map.items():
                    try:
                        stmt = select(table_obj)
                        result = conn.execution_options(stream_results=True).execute(stmt)

                        batch = []
                        while True:
                            rows = result.fetchmany(batch_size)
                            if not rows:
                                break
                            for row in rows:
                                # row is a Row/RowMapping; convert to plain dict safely
                                try:
                                    row_dict = dict(row._mapping)  # SQLAlchemy 1.4 RowMapping
                                except Exception:
                                    # fallback for older row types
                                    row_dict = dict(row)

                                # primary key value (fixes the earlier bug)
                                record_id = row_dict.get(pkname, None)

                                # sanitize/serialize datetimes if you have such helper
                                try:
                                    record_json = json.dumps(row_dict, default=getattr(self, 'sanitizeJson', str))
                                except Exception:
                                    # last-resort str conversion
                                    record_json = json.dumps({k: (str(v) if v is not None else None) for k, v in row_dict.items()})

                                # build message format - adapt to your consumers if needed
                                # using "<id>:<json>" as before; you can change to any schema
                                if record_id is None:
                                    # if no PK value for some reason, use JSON only and warn
                                    message = record_json
                                else:
                                    message = f"{record_id}:{record_json}"

                                batch.append(message)

                            # send batch to redis via your sendBulkMessage (async)
                            if batch:
                                try:
                                    # sendBulkMessage(queue, messageList, queueExpiry=None, usePrefix=False, prefixHostname='unknown', prefixServiceName='common')
                                    await redis_messaging.sendBulkMessage(
                                        queue=table_name,
                                        messageList=batch,
                                        queueExpiry=None,
                                        usePrefix=True,
                                        prefixHostname=hostname,
                                        prefixServiceName='database'
                                    )
                                except Exception as e_send:
                                    # log but continue processing next rows/tables
                                    self.logTool.log(service='Database', level='error',
                                                     message=f"[readDatabase] sendBulkMessage failed for {table_name}: {e_send}",
                                                     redisClient=getattr(self, 'redisLogMessaging', None))
                                batch = []

                        # close result set
                        try:
                            result.close()
                        except Exception:
                            pass

                    except Exception as e_table:
                        # log and continue with next table
                        self.logTool.log(service='Database', level='error',
                                         message=f"[readDatabase] Error reading table {table_name}: {traceback.format_exc()}",
                                         redisClient=getattr(self, 'redisLogMessaging', None))
                        continue

            # sleep until next pass
            await asyncio.sleep(cache_interval)

        except asyncio.CancelledError:
            # be cooperative to cancellations
            break
        except Exception:
            self.logTool.log(service='Database', level='error',
                             message=f"[readDatabase] Unexpected error: {traceback.format_exc()}",
                             redisClient=getattr(self, 'redisLogMessaging', None))
            await asyncio.sleep(cache_interval)
            continue

    async def startService(self):
        """
        Performs sanity checks on configuration and starts the database service.
        """
        await(self.logTool.logAsync(service='Database', level='info', message=f"{self.banners.databaseService()}"))
        while True:

            if not self.readCacheEnabled:
                await(self.logTool.logAsync(service='Database', level='info', message=f"[Database] [startService] Database read cache enabled, exiting."))
                sys.exit()

            activeTasks = []

            readCacheTask = asyncio.create_task(self.readDatabase())
            activeTasks.append(readCacheTask)

            completeTasks, pendingTasks = await(asyncio.wait(activeTasks, return_when=asyncio.FIRST_COMPLETED))

            if len(pendingTasks) > 0:
                for pendingTask in pendingTasks:
                    try:
                        pendingTask.cancel()
                        await(asyncio.sleep(0.001))
                    except asyncio.CancelledError:
                        pass


if __name__ == '__main__':
    databaseService = DatabaseService()
    asyncio.run(databaseService.startService())
