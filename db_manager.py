import abc
import asyncio
import contextlib

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


class DBManager(metaclass=abc.ABCMeta):
    """DataBase Abstract
    responsibility: interface, DB connection tools
    """

    @abc.abstractmethod
    def _initialize(self):
        """db init, connect db"""
        return NotImplemented

    @abc.abstractmethod
    def clean_up(self):
        """empty db's connection"""
        return NotImplemented

    @abc.abstractmethod
    def get_db(self):
        """get db connection for using."""
        return NotImplemented


class AsyncSqlalchemy(DBManager):

    def __init__(self, db_dialect: str, db_driver,
                 db_user: str, db_password: str, db_host: str,
                 db_port: int, db_name: str, echo: bool):
        """
"postgresql+asyncpg://scott:tiger@localhost/test", echo=True,

        Args:
            db_dialect: database name such as mysql, oracle, postgresql.
            db_driver: driver the name of a DBAPI,
                such as psycopg2, pyodbc, cx_oracle, aiomysql etc.
            db_user: user
            db_password: password
            db_host: host
            db_port: port
            db_name: database's name
            echo: if True, the Engine will log all statements as well as a repr.
        """
        self.db_url = f"{db_dialect}+{db_driver}://{db_user}:{db_password}" \
                      f"@{db_host}:{db_port}/{db_name}"

        self._engine = create_async_engine(
            url=self.db_url,
            echo=echo,
        )
        self._initialize()

    def _initialize(self):
        """
        Configure class for creating scoped session.
        :return:
        """
        async_session_factory = sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            class_=AsyncSession
        )
        self._session = async_session_factory()

    @contextlib.asynccontextmanager
    async def get_db(self) -> AsyncSession:
        """contextmanager will create and teardown a session"""
        try:
            yield self._session
            await self._session.commit()

        except Exception:
            await self._session.rollback()
            raise

        finally:
            await self._session.close()

    def clean_up(self):
        """
        Cleans up the database connection pool.
        :return:
        """
        if not self._engine:
            self._engine.dispose()


if __name__ == "__main__":
    async def example():
        db_service = AsyncSqlalchemy(
            db_dialect='mysql', db_driver='aiomysql', db_user='test_user',
            db_password='test-pws', db_host='0.0.0.0', db_port=3306,
            db_name='user_profile', echo=True
        )

        async with db_service.get_db() as db:
            async with db.begin():
                statement = 'select * from user_profile.label_metadata'
                result = await db.execute(statement)
                print(result.fetchall())


    loop = asyncio.get_event_loop()
    loop.run_until_complete(example())


