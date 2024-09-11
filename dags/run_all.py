from get_sensors import _get_sensors
from api import _start_api
from create_relational_database import _start_create_relational_database
from postgres import _start_post

_get_sensors()
_start_api()
_start_create_relational_database()
_start_post()