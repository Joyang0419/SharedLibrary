import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from src.SharedLibrary.db_manager.abstract import DBManager


class ToolSqlalchemy(DBManager):

    def __init__(self, db_dialect: str, db_driver,
                 db_user: str, db_password: str, db_host: str,
                 db_port: int, db_name: str, echo: bool):
        """

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

        self._engine = create_engine(
            url=self.db_url,
            echo=echo,
        )
        self._initialize()

    def _initialize(self):
        """
        Configure class for creating scoped session.
        :return:
        """
        session_factory = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False
        )
        self._session = session_factory()

    @contextlib.contextmanager
    def get_db(self) -> Session:
        """contextmanager will create and teardown a session"""
        try:
            yield self._session
            self._session.commit()

        except Exception:
            self._session.rollback()
            raise

        finally:
            self._session.close()

    def clean_up(self):
        """
        Cleans up the database connection pool.
        :return:
        """
        if not self._engine:
            self._engine.dispose()
