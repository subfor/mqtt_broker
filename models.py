import os
import datetime
import asyncio
import aiomysql

from pytz import timezone
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, DateTime
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base

TIMEZONE = timezone('Europe/Kiev')
# DB_USER = os.getenv('DB_USER')
# DB_PASS = os.getenv('DB_PASS')
# DB_HOST = os.getenv('DB_HOST')
# DB = os.getenv('DB')

DB_USER = "root"
DB_PASS = "pass"
DB_HOST = "192.168.3.5"
DB = "mqtt_data"
DeclarativeBase = declarative_base()


def time_now():
    return datetime.datetime.now(TIMEZONE)


class Telemetry(DeclarativeBase):
    __tablename__ = 'telemetry'

    id = Column(Integer, primary_key=True)
    client_name = Column('client_name', String(50))
    data = Column('data', Text)
    creation_time = Column('creation_time', DateTime(timezone=True), default=time_now)

    def __repr__(self):
        return "".format(self.code)


class Event(DeclarativeBase):
    __tablename__ = 'event'

    id = Column(Integer, primary_key=True)
    client_name = Column('client_name', String(50))
    data = Column('data', Text)
    creation_time = Column('creation_time', DateTime(timezone=True), default=time_now)

    def __repr__(self):
        return "".format(self.code)


class Receipt(DeclarativeBase):
    __tablename__ = 'receipt'

    id = Column(Integer, primary_key=True)
    client_name = Column('client_name', String(50))
    data = Column('data', Text)
    creation_time = Column('creation_time', DateTime(timezone=True), default=time_now)

    def __repr__(self):
        return "".format(self.code)


async def create_tables():
    engine = create_async_engine(
        f"mysql+aiomysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB}", echo=False,
    )
    async with engine.begin() as conn:
        await conn.run_sync(DeclarativeBase.metadata.drop_all)
        await conn.run_sync(DeclarativeBase.metadata.create_all)
    await engine.dispose()


async def write_to_db(data: dict, table_name: str):
    tables_insert = {
        'Event': Event.__table__.insert(),
        'Receipt': Receipt.__table__.insert(),
        'Telemetry': Telemetry.__table__.insert()
    }
    engine = create_async_engine(
        f"mysql+aiomysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB}", echo=False,
    )
    async with engine.begin() as conn:
        await conn.execute(tables_insert[table_name], data)
    await engine.dispose()


if __name__ == '__main__':
    asyncio.run(create_tables())
