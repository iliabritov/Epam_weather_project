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

    def make_session(self):
        username = self._config["user"]
        password = self._config["password"]
        host = self._config["host"]
        db_name = self._config["database"]

        connect_path = f"postgresql://{username}:{password}@{host}/{db_name}"
        engine = sqlalchemy.create_engine(connect_path)
        session_maker = sqlalchemy.orm.sessionmaker(bind=engine)
        self._session = session_maker()

    def get_config(self):
        parser = ConfigParser()
        with self._config_path.open() as conf:
            parser.read_file(conf)
        return parser[self._config_name]

    def add(self, data_obj):
        self._session.add(data_obj)
        self._session.commit()

    def update(self):
        self._session.commit()

    def delete(self, data_obj):
        self._session.delete(data_obj)
        self._session.commit()

    def make_query(self, data_obj):
        self.query = self._session.query(data_obj)

    def __del__(self):
        self._session.close()


class WeatherDate(Base):
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
