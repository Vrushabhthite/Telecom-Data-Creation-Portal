
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker, declarative_base


# DATABASE_URL = "mysql+pymysql://root:Coder%40123@localhost/smart_disk_automation"

# engine = create_engine(DATABASE_URL)

# SessionLocal = sessionmaker(bind=engine)
# Base = declarative_base()


# import os
# from sqlalchemy import create_engine

# DATABASE_URL = "postgresql://ioms_user:6xntcph7oZ2cpFWztGsqiyRD0EW7LNgR@dpg-d71si1i4d50c73bvc1k0-a.singapore-postgres.render.com/ioms_data"

# engine = create_engine(
#     DATABASE_URL,
#     connect_args={"sslmode": "require"}  # VERY IMPORTANT
    
import os
from sqlalchemy import create_engine

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    connect_args={"sslmode": "require"}
)
