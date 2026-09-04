import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .logging_config import setup_logging
from .config import get_settings
from .db import init_db
from .security import get_current_user
from .api.routes import router as api_router
from .poller import poll_loop

settings = get_settings()
setup_logging(settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

poll_task: asyncio.Task | None = None


async def _warm_cookies():
    try:
        from .cookie_provider import get_cookies

        cookies = await get_cookies()
        logger.info(f"cookie pre-warm: {sorted(cookies.keys()) or 'none (fallback modes active)'}")
    except Exception as e:
        logger.debug(f"cookie pre-warm failed err={type(e).__name__}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global poll_task
    await init_db()
    asyncio.create_task(_warm_cookies())
    poll_task = asyncio.create_task(poll_loop())
    logger.info("app started, poller running")
    yield
    if poll_task:
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
    logger.info("app shutdown")


app = FastAPI(title="PlaytopiaLiveNotifier", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=settings.APP_SECRET_KEY, max_age=86400 * 7)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app.include_router(api_router, prefix="/api")


@app.get("/health")
async def health():
    ready = poll_task is not None and not poll_task.done()
    return {"status": "ok", "poller_running": ready}


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if await get_current_user(request):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request})


async def _require_page_auth(request: Request):
    if not await get_current_user(request):
        return RedirectResponse(url="/login", status_code=302)
    return None


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not await get_current_user(request):
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard2(request: Request):
    if not await get_current_user(request):
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("dashboard.html", {"request": request})


if (BASE_DIR / "static").exists():
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.exception_handler(Exception)
async def generic_handler(request: Request, exc: Exception):
    logger.exception(f"unhandled {type(exc).__name__}")
    return JSONResponse(status_code=500, content={"detail": "Internal error"})
