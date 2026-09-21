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
from .infrastructure.persistence.database import init_db
from .security import get_current_user
from .presentation.api.auth import router as auth_router
from .presentation.api.admins import router as admins_router
from .presentation.api.subscriptions import router as subscriptions_router
from .presentation.api.logs import router as logs_router
from .presentation.api.schedule import router as schedule_router
from .presentation.api.settings import router as settings_router
from .presentation.api.media import router as media_router
from .presentation.api.analytics import router as analytics_router
from .presentation.api.misc import router as misc_router
from .poller import poll_loop

settings = get_settings()
setup_logging(settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

poll_task: asyncio.Task | None = None


async def _warm_cookies():
    try:
        from .infrastructure.checkers.cookies import get_cookies

        cookies = await get_cookies()
        logger.info(f"cookie pre-warm: {sorted(cookies.keys()) or 'none (fallback modes active)'}")
    except Exception as e:
        logger.debug(f"cookie pre-warm failed err={type(e).__name__}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global poll_task
    await init_db()
    if (getattr(settings, "TT_COOKIE_PROVIDER", "off") or "off").lower() != "off":
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

for _r in (auth_router, admins_router, subscriptions_router, logs_router,
           schedule_router, settings_router, media_router, analytics_router,
           misc_router):
    app.include_router(_r, prefix="/api")


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


@app.get("/dashboard")
async def dashboard2():
    return RedirectResponse(url="/", status_code=302)


if (BASE_DIR / "static").exists():
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.exception_handler(Exception)
async def generic_handler(request: Request, exc: Exception):
    logger.exception(f"unhandled {type(exc).__name__}")
    return JSONResponse(status_code=500, content={"detail": "Internal error"})
