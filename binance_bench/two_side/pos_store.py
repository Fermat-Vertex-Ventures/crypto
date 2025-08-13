# pos_store.py
from __future__ import annotations
import json, os
from datetime import datetime
from typing import Dict, Any

def read_pos(path: str, symbol: str) -> dict[str, Any] | None:
    """
    برگرداندن اطلاعات پوزیشن برای یک symbol خاص.
    اگر موجود نبود → None
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(symbol.upper())
    except FileNotFoundError:
        return None
    except Exception:
        return None

def write_pos(path: str, symbol: str, qty: float) -> None:
    """
    ذخیره‌سازی پوزیشن به همراه side و زمان آخرین تغییر.
    qty به واحد COIN است.
    """
    sym = symbol.upper()
    side = "flat"
    if qty > 0:
        side = "buy"
    elif qty < 0:
        side = "sell"

    # اگر فایل وجود دارد، محتوا را بخوانیم
    data: Dict[str, Any] = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}

    data[sym] = {
        "q": qty,
        "side": side,
        "time": datetime.utcnow().isoformat(timespec="seconds")
    }

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)
