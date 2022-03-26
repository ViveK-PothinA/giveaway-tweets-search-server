from os.path import dirname, abspath
from os import getenv
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from fastapi import FastAPI, Request
from routers import test_router, search_router
import logging
from utils.custom_logger import CustomizeLogger
from utils.custom_exception import CustomException
from fastapi.responses import JSONResponse, HTMLResponse

# ENV variables
HOST = getenv('HOST', "localhost")
PORT = int(getenv('PORT', '7210'))
DEBUG = bool(getenv('DEBUG', 'false'))


# INIT app
app = FastAPI(
    title="Twitter Data Giveaway Search Engine",
    description="Twitter Data Giveaway Search Engine",
    version="1.0",
)


# LOGGER settings
logger = logging.getLogger(__name__)
logger = CustomizeLogger.make_logger(DEBUG, f"{dirname(abspath(__file__))}/app.log")
app.logger = logger


# CORS config
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(CustomException)
async def custom_exception_handler(request: Request, execption: CustomException):
    return JSONResponse(
        status_code=400,
        content={"message": execption.message},
    )

# API Router inclusion
app.include_router(test_router.test_router, tags=["test"])
app.include_router(search_router.search_router, tags=["search"])


@app.get("/")
async def index():
    html_content = f'<p>Engine is up and running. Checkout  <a href="http://{HOST}:{PORT}/docs">Swagger docs</a> or <a href= "http://{HOST}:{PORT}/redoc">Redoc docs</a><p>'
    return HTMLResponse(content=html_content, status_code=200)


if __name__ == "__main__":
    print(HOST, PORT, DEBUG)
    uvicorn.run(app, host=HOST, port=PORT, debug=DEBUG)
