import abc


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
