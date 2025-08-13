# coinex_hedger.py
from __future__ import annotations
import os, time, math, json
from datetime import datetime
from typing import Tuple
from nobitex_utils import get_nobitex_orderbook, read_balance, log
import coinex_module as cx
from pos_store import read_pos, write_pos

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
def load_cfg(coin: str) -> dict:
    path = os.environ.get("CONFIG_JSON", "config_edt.json")
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    try:
        return cfg["dual_arb"][coin.upper()]
    except KeyError:
        raise SystemExit(f"[CONFIG] dual_arb → {coin} missing in {path}")

# ─────────────────────────────────────────────────────────────────────────────
# Helpers: CoinEx orderbook & rounding
# ─────────────────────────────────────────────────────────────────────────────
def coinex_best(symbol: str) -> Tuple[float, float]:
    """Return (best_bid, best_ask) for CoinEx FUTURES."""
    ob = cx.future_order_book(symbol, limit=5)
    bids = ob["bids"]
    asks = ob["asks"]
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    return best_bid, best_ask

def round_qty(q: float, prec: int) -> float:
    if prec <= 0:
        return math.floor(q)
    f = 10.0 ** prec
    return math.floor(q * f) / f

def round_price(p: float, prec: int) -> float:
    f = 10.0 ** prec
    return math.floor(p * f) / f

def get_coinex_pos(symbol: str, store_path: str) -> float:
    """
    Try API for current position size (in COIN units). If API not available,
    fallback to pos_store.
    """
    try:
        # Some CoinEx v2 deployments expose /futures/position?market=...
        data = cx._signed_request("GET", "/futures/position", {"market": symbol, "market_type": "FUTURES"})
        # Expect like: {"positions":[{"market":"BTCUSDT","size":"0.0123","side":"long"}]}
        if isinstance(data, dict) and "positions" in data and data["positions"]:
            pos = 0.0
            for p in data["positions"]:
                if p.get("market") == symbol:
                    size = float(p.get("size", 0))
                    side = (p.get("side") or "").lower()
                    pos += size if side == "long" else -size
            return pos
    except Exception:
        pass
    # fallback to local cache
    return read_pos(store_path, symbol)

# ─────────────────────────────────────────────────────────────────────────────
# Diff & stats (z = x - y)
# ─────────────────────────────────────────────────────────────────────────────
def live_diff(coin: str) -> Tuple[float, float, float]:
    """
    Return (x_mid, y_mid, z) with current orderbooks:
      x = (coin/USDT on CoinEx FUTURES mid) * (USDT/IRT on Nobitex mid)
      y = coin/IRT on Nobitex mid
    """
    # Nobitex USDTIRT mid
    usdt = get_nobitex_orderbook("USDTIRT")
    usdt_mid = float((usdt["bids"][0]["price"] + usdt["asks"][0]["price"]) / 2)

    # Nobitex coinIRT mid
    ob = get_nobitex_orderbook(f"{coin}IRT")
    coin_irt_mid = float((ob["bids"][0]["price"] + ob["asks"][0]["price"]) / 2)

    # CoinEx FUTURES coinUSDT mid
    best_bid, best_ask = coinex_best(f"{coin}USDT")
    coin_usdt_mid = (best_bid + best_ask) / 2

    x = coin_usdt_mid * usdt_mid
    y = coin_irt_mid
    return x, y, (x - y)

def window_stats(obs_db_path: str, window_min: int) -> Tuple[float, float, int]:
    """Compute mean/std over last window from local obs DB if available; else rough online."""
    try:
        import sqlite3
        now = int(time.time())
        st = now - window_min * 60
        con = sqlite3.connect(obs_db_path, timeout=3)
        cur = con.cursor()
        cur.execute("SELECT avg(diff), avg(diff*diff), count(*) FROM obs WHERE time BETWEEN ? AND ?", (st, now))
        row = cur.fetchone() or (None, None, 0)
        con.close()
        n = int(row[2] or 0)
        if n >= 2 and row[0] is not None and row[1] is not None:
            mean = float(row[0]); mean_sq = float(row[1])
            var = max(0.0, mean_sq - mean*mean)
            std = (var * n / (n - 1)) ** 0.5 if n > 1 else 0.0
            return mean, std, n
    except Exception:
        pass
    # Fallback: run a small online average for a few samples
    vals = []
    for _ in range(10):
        _, _, z = live_diff(CFG["coin"])
        vals.append(z); time.sleep(0.2)
    import statistics as st
    if len(vals) >= 2:
        mean = st.fmean(vals)
        std = st.pstdev(vals) if len(vals) < 3 else st.stdev(vals)
        return mean, std, len(vals)
    return 0.0, 0.0, 0

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
CFG = {}

def center_from_b(b_units: float, l_units: float, u_units: float) -> float:
    m = 0.5*(l_units + u_units)
    return m - b_units  # >0 target long, <0 target short

def fok_taker(symbol: str, side: str, qty: float, price: float, qty_prec: int, price_prec: int) -> bool:
    """
    Emulate FOK using: limit @ best, short timeout; if partial -> cancel and revert.
    Returns True if fully filled, False otherwise.
    """
    q = max(0.0, round_qty(qty, qty_prec))
    p = round_price(price, price_prec)
    if q <= 0:
        return True

    # place limit at best price as taker
    try:
        resp = cx.open_order(
            market=symbol, side=side, order_type='limit',
            amount=str(q), price=str(p),
            is_futures=True, leverage=str(CFG.get("leverage", 1))
        )
        order_id = int(resp.get("order_id") or resp.get("order", {}).get("id") or 0)
    except Exception as e:
        log(f"[CX] order error: {e}")
        return False

    # tiny wait & check
    time.sleep(0.4)
    try:
        st = cx.check_order(symbol, order_id, is_futures=True)
        # compatible parsing
        od = st.get("order") or st
        matched = float(od.get("amount_filled") or od.get("matched_amount") or 0.0)
        requested = float(od.get("amount") or q)
        if matched >= requested - 1e-12:
            return True
        # cancel
        cx.cancel_order(symbol, order_id, is_futures=True)
        # revert matched part to restore pos neutrality (market)
        if matched > 0:
            rev_side = "sell" if side == "buy" else "buy"
            try:
                cx.open_order(market=symbol, side=rev_side, order_type='market',
                              amount=str(matched), is_futures=True, leverage=str(CFG.get("leverage", 1)))
            except Exception as e2:
                log(f"[CX] revert market failed: {e2}")
        return False
    except Exception as e:
        log(f"[CX] check/cancel error: {e}")
        return False

def main():
    global CFG
    coin = os.environ.get("COIN", None) or (len(os.sys.argv) > 1 and os.sys.argv[1].upper())
    if not coin:
        raise SystemExit("Usage: python coinex_hedger.py <COIN>")
    CFG = load_cfg(coin)

    sym = CFG["coinex_symbol"]
    pos_store = CFG.get("pos_store_path", "coinex_pos.json")
    qty_prec = int(CFG.get("qty_precision", 6))
    price_prec = int(CFG.get("price_precision", 2))
    l = float(CFG["l_units"]); u = float(CFG["u_units"]); t = float(CFG["t_units"])
    k = float(CFG["k"]); wmin = int(CFG["window_min"]); iv = float(CFG["interval_sec"])
    leverage = int(CFG.get("leverage", 1))

    # ensure leverage=1
    try:
        cx.set_leverage(sym, leverage=str(leverage), margin_mode="cross")
    except Exception as e:
        log(f"[CX] set_leverage warn: {e}")

    last_stats_ts = 0.0
    mean = std = 0.0

    while True:
        time.sleep(iv)
        now = time.time()

        # refresh stats
        if now - last_stats_ts > wmin*60:
            mean, std, n = window_stats(CFG["obs_db_path"], wmin)
            log(f"[CX] mean={mean:.1f} std={std:.1f} (n={n})")
            last_stats_ts = now
            if std <= 0:
                continue

        # live diff and thresholds
        x, y, z = live_diff(coin)
        lower, upper = mean - k*std, mean + k*std

        # only act if signal
        if not (z > upper or z < lower):
            continue

        # compute target center based on Nobitex coin units
        b_units = read_balance(coin, CFG["balance_pickle"], free=True)  # عدد کوین
        target = center_from_b(b_units, l, u)

        # current futures pos (try API, else cache)
        cur = get_coinex_pos(sym, pos_store)
        delta = target - cur
        side = "buy" if delta > 0 else "sell"
        need = abs(delta)

        # inside tolerance? then no action
        if need <= CFG.get("min_qty", 1e-6):
            continue

        # pick taker price
        bb, ba = coinex_best(sym)
        px = ba if side == "buy" else bb

        log(f"[CX] z={z:.0f}  target={target:.6f} cur={cur:.6f} Δ={delta:.6f} → {side.upper()} {need:.6f} @ {px}")
        ok = fok_taker(sym, side, need, px, qty_prec, price_prec)

        # refresh pos (api or local update)
        if ok:
            # trusting API if available
            new_pos = get_coinex_pos(sym, pos_store)
            if new_pos == read_pos(pos_store, sym):  # API not available → update locally
                new_pos = cur + (need if side=="buy" else -need)
            write_pos(pos_store, sym, new_pos)
            log(f"[CX] FOK filled. pos={new_pos:.6f}")
        else:
            # no fill → keep pos unchanged
            write_pos(pos_store, sym, cur)
            log("[CX] FOK not filled. pos unchanged.")

if __name__ == "__main__":
    main()
