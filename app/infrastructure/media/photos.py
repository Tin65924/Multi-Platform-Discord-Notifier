"""Creator photo uploads: validated, normalized, DB-backed.

Security model:
  - Never trust client filename / content-type / extension.
  - Identity comes from magic bytes via Pillow (open + full load).
  - Decompression bombs blocked by Pillow's MAX_IMAGE_PIXELS + our byte cap.
  - Always re-encoded (strips EXIF, normalizes orientation, kills
    polyglot payloads hiding after image data).
  - Stored in Postgres (survives Render free restarts), never on disk.
"""
import io
import logging

logger = logging.getLogger(__name__)

MAX_UPLOAD_BYTES = 8 * 1024 * 1024  # reject anything bigger up front
MAX_FINAL_BYTES = 2 * 1024 * 1024  # re-encoded output must fit this
MAX_DIM = 1024  # longest side after resize
JPEG_QUALITY = 85

ALLOWED_FORMATS = {"JPEG", "PNG", "WEBP", "GIF"}


def process_upload(raw: bytes) -> tuple[bytes, str]:
    """Validate + normalize an uploaded image. Returns (bytes, mime).

    Raises ValueError with a user-facing message on any rejection.
    """
    if not raw:
        raise ValueError("Empty file")
    if len(raw) > MAX_UPLOAD_BYTES:
        raise ValueError(f"File too large (max {MAX_UPLOAD_BYTES // 1024 // 1024}MB)")

    try:
        from PIL import Image, ImageOps
    except ImportError:
        raise ValueError("Image support unavailable on server")

    # Identity + integrity: magic bytes first, then full decompress.
    try:
        img = Image.open(io.BytesIO(raw))
        fmt = img.format
        img.load()
    except Exception:
        raise ValueError("Not a valid image (JPEG/PNG/WEBP/GIF only)")
    if fmt not in ALLOWED_FORMATS:
        raise ValueError("Only JPEG, PNG, WEBP or GIF allowed")

    # Animated GIF: keep first frame only (embed shows one image anyway).
    try:
        img.seek(0)
    except Exception:
        pass
    img = ImageOps.exif_transpose(img)  # fix phone rotation, drop orientation tag

    has_alpha = (
        img.mode in ("RGBA", "LA")
        or (img.mode == "P" and "transparency" in img.info)
    )
    if max(img.size) > MAX_DIM:
        img.thumbnail((MAX_DIM, MAX_DIM), Image.LANCZOS)

    out = io.BytesIO()
    if has_alpha:
        img.convert("RGBA").save(out, format="PNG", optimize=True)
        mime = "image/png"
    else:
        img.convert("RGB").save(out, format="JPEG", quality=JPEG_QUALITY, optimize=True)
        mime = "image/jpeg"
    data = out.getvalue()
    if len(data) > MAX_FINAL_BYTES:
        raise ValueError("Image still too large after optimization — use a smaller one")
    logger.info(f"photo processed format={fmt} size={img.size} bytes={len(data)} mime={mime}")
    return data, mime
