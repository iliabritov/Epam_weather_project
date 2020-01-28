import sqlalchemy
from configparser import ConfigParser
from sqlalchemy import Column, Float, Integer, Date, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class DbDriver:
    def __init__(self, config_path, config_name):
        self._config_path = config_path
        self._config_name = config_name

        self._config = self.get_config()
        self.make_session()

    def make_session():
        username = self._config["username"]
        password = self._config["password"]
        host = self._config["host"]
        db_name = self._config["db_name"]

        connect_path = f"postgresql://{username}:{password}@{host}/{db_name}"
        engine = sqlalchemy.create_engine(connect_path)
        session_maker = sqlalchemy.orm.session_maker(bind=engine)
        self._session = session_maker()

    def get_config():
        parser = Parser()
        with self.config_path.open() as conf:
            parser.read_file(conf)
        return parser[self.config_name]

    def add(self, data_obj):
        self._session.add(data_obj)
        self._session.commit()

    def update(self):
        self._session.commit()

    def delete(self, data_obj):
        self._session.delete(data_obj)
        self._session.commit()


class Weather(Base):
    __tablename__ = "weather"
    id = Column(Integer, primary_key=True)
    data_base = Column(String)
    city = Column(String)
    date_day = Column(Date)
    temp = Column(Float)
    prec_desc = Column(String)
    prec_mm = Column(Float)
    wind_speed = Column(Float)
    wind_direc = Column(String)

    def __init__(
        self,
        data_base,
        city,
        date_day,
        temp,
        prec_desc,
        prec_mm,
        wind_speed,
        wind_direc,
    ):
        self.data_base = data_base
        self.city = city
        self.date_day = date_day
        self.temp = temp
        self.prec_desc = prec_desc
        self.prec_mm = prec_mm
        self.wind_speed = wind_speed
        self.wind_direc = wind_direc
