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

# replace readDatabase() body with this approach
async def readDatabase(self):
    # reflect once
    databaseMetadata = MetaData()
    # connect and reflect ONCE at startup
    with self.sqlAlchemyEngine.connect() as databaseConnection:
        databaseMetadata.reflect(bind=databaseConnection)
    # build a stable map: tableName -> (table, primaryKeyName)
    table_map = {}
    for tableName, tableObj in databaseMetadata.tables.items():
        pk_cols = [c.name for c in tableObj.primary_key.columns]
        if not pk_cols:
            continue
        table_map[tableName] = (tableObj, pk_cols[0])

    while True:
        try:
            # use single session for full pass
            session = self.sqlAlchemySession()
            # for each table, stream rows in chunks
            for tableName, (tableObj, pkname) in table_map.items():
                # use select() and chunking (yield_per)
                q = session.query(tableObj).yield_per(500)
                batch = []
                # use pipeline if your RedisMessagingAsync supports it, otherwise collect messages
                async_sends = []
                for row in q:
                    recordDict = dict(row._mapping)
                    recordJson = json.dumps(recordDict, default=self.sanitizeJson)
                    # get real id value
                    recordId = recordDict.get(pkname)
                    # build message
                    msg = f"{recordId}:{recordJson}"
                    batch.append(msg)
                    # when batch size reaches threshold, send as group
                    if len(batch) >= 200:
                        # perform bulk send (pseudo)
                        await self.redisDatabaseReadMessaging.send_bulk(queue=tableName, messages=batch, usePrefix=True, prefixHostname=self.hostname, prefixServiceName='database')
                        batch = []
                if batch:
                    await self.redisDatabaseReadMessaging.send_bulk(queue=tableName, messages=batch, usePrefix=True, prefixHostname=self.hostname, prefixServiceName='database')
            session.close()
            await asyncio.sleep(self.cacheReadInterval)
        except Exception as e:
            self.logTool.log(service='Database', level='error', message=f"[Database] [readDatabase] Error: {traceback.format_exc()}", redisClient=self.redisLogMessaging)
            try: session.close()
            except: pass
            await asyncio.sleep(self.cacheReadInterval)

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
