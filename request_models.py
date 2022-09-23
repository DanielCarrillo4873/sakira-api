"""" All models for requests body

"""
from pydantic import BaseModel


class Actor(BaseModel):
    last_name: str
    first_name: str
