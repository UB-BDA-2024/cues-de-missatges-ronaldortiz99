import fastapi
from .sensors.controller import router as sensorsRouter

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

app.include_router(sensorsRouter)

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
