from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker 

engine = create_engine('sqlite:///newspaper.db')#create an engine for dialect and pool for actual database

Session =sessionmaker(bind=engine)#create the session to manage  the database

Base = declarative_base()

