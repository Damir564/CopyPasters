from blacksheep import Application, Response, Content, json
from consumer import DataReader

app = Application()
get = app.router.get

#
dr = DataReader()


@get("/")
def home() -> Response:
    return json(dr.read_data())

