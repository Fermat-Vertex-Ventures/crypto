# nobitex_dual_maker.py
from __future__ import annotations
import os, time, json, traceback
from datetime import datetime
from decimal import Decimal
from typing import Tuple
import ccxt  # for Binance backup if ever needed (not critical)
from nobitex_utils import (
    get_nobitex_orderbook, new_order, check_order, cancel_order,
    parse_order_response, read_balance, ri, log
)
from pos_store import read_pos

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
# Stats & diff
# ─────────────────────────────────────────────────────────────────────────────
def window_stats(obs_db_path: str, window_min: int) -> Tuple[float, float, int]:
    import sqlite3
    now = int(time.time())
    st = now - window_min*60
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
    return 0.0, 0.0, 0

def live_diff(coin: str) -> Tuple[float, float, float]:
    # Nobitex USDTIRT mid
    usdt = get_nobitex_orderbook("USDTIRT")
    usdt_mid = float((usdt["bids"][0]["price"] + usdt["asks"][0]["price"]) / 2)
    # Nobitex coinIRT mid
    ob = get_nobitex_orderbook(f"{coin}IRT")
    coin_irt_mid = float((ob["bids"][0]["price"] + ob["asks"][0]["price"]) / 2)
    # Approx coinUSDT mid via Nobitex all orderbooks if available is complex; use Binance as reference:
    # (اختیاری) اگر نمی‌خواهی ccxt استفاده شود، می‌توانی از CoinEx mid استفاده کنی و نتیجه همان است.
    try:
        import coinex_module as cx
        ob_f = cx.future_order_book(f"{coin}USDT", limit=5)
        bb = float(ob_f["bids"][0][0]); ba = float(ob_f["asks"][0][0])
        coin_usdt_mid = (bb + ba) / 2
    except Exception:
        ex = ccxt.binance()
        ob2 = ex.fetch_order_book(f"{coin}/USDT")
        coin_usdt_mid = (ob2["bids"][0][0] + ob2["asks"][0][0]) / 2.0
    x = coin_usdt_mid * usdt_mid
    y = coin_irt_mid
    return x, y, (x - y)

# ─────────────────────────────────────────────────────────────────────────────
# Maker tracking with signal/best-price enforcement
# ─────────────────────────────────────────────────────────────────────────────
def weighted_limit_price(side: str, best_bid: float, best_ask: float, weight: float) -> float:
    return weight * (best_bid if side=="buy" else best_ask) + (1.0 - weight) * (best_ask if side=="buy" else best_bid)

def track_and_cancel(order_id: int, market: str, side: str, timeout: int,
                     mean: float, std: float, k: float, coin: str, poll: float = 2.0) -> Tuple[str, Decimal, Decimal]:
    """
    - اگر بهترین قیمت نبود → CANCEL
    - اگر سیگنال از بین رفت (mean - kσ < z < mean + kσ) → CANCEL
    - اگر timeout → CANCEL
    """
    start = time.time()
    matched = Decimal("0")
    price_used = Decimal("0")
    while True:
        # best prices
        ob = get_nobitex_orderbook(market)
        best_bid = ob["bids"][0]["price"]
        best_ask = ob["asks"][0]["price"]

        # order status
        info = check_order(order_id).get("order", {})
        status = (info.get("status") or "").lower()
        matched = Decimal(str(info.get("matchedAmount", "0")))
        order_price = Decimal(str(info.get("Price", best_ask if side=="buy" else best_bid)))
        price_used = order_price

        # filled?
        if status in {"finished", "filled", "done", "complete"} and info.get("unmatchedAmount", 0) in (0, "0"):
            log("filled")
            return "FILLED", matched, matched * order_price

        # still best-maker?
        if side == "buy" and order_price < best_bid:
            log("no longer best bid → cancel")
            cancel_order(order_id); return "NOT_BEST", matched, matched * order_price
        if side == "sell" and order_price > best_ask:
            log("no longer best ask → cancel")
            cancel_order(order_id); return "NOT_BEST", matched, matched * order_price

        # signal still active?
        _, _, z = live_diff(coin)
        if (mean - k*std) < z < (mean + k*std):
            log("signal vanished → cancel")
            cancel_order(order_id); return "NO_SIGNAL", matched, matched * order_price

        if time.time() - start > timeout:
            log("timeout → cancel")
            cancel_order(order_id); return "TIMEOUT", matched, matched * order_price

        time.sleep(poll)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    coin = os.environ.get("COIN", None) or (len(os.sys.argv) > 1 and os.sys.argv[1].upper())
    if not coin:
        raise SystemExit("Usage: python nobitex_dual_maker.py <COIN>")
    c = load_cfg(coin)

    window_min   = int(c["window_min"])
    k            = float(c["k"])
    w_buy        = float(c.get("w_buy", c.get("weight", 0.5)))
    w_sell       = float(c.get("w_sell", c.get("weight", 0.5)))
    timeout_sec  = int(c.get("timeout_sec", 45))
    iv           = float(c.get("interval_sec", 2.0))
    obs_db_path  = str(c["obs_db_path"])
    balance_path = c.get("balance_pickle", "blnc_data.pickle")
    pos_store    = c.get("pos_store_path", "coinex_pos.json")
    order_qty    = Decimal(str(c["order_quantity"]))  # تعداد کوین برای هر معامله
    l_units      = float(c["l_units"])
    u_units      = float(c["u_units"])
    t_units      = float(c["t_units"])

    # trades log (اختیاری)
    trades_path = c.get("trades_pickle", f"{coin.lower()}_trades.pkl")
    if not os.path.exists(trades_path):
        import pandas as pd
        pd.to_pickle({"buy": [], "sell": []}, trades_path)

    last_stats = 0.0
    mean = std = 0.0

    def within_tobit_window(b_units: float) -> Tuple[bool, float, float, float, float]:
        m = 0.5*(l_units + u_units)
        center = m - b_units
        pos = read_pos(pos_store, f"{coin}USDT")
        return (center - t_units < pos < center + t_units), pos, center, center - t_units, center + t_units

    import pandas as pd
    buys, sells = [], []

    while True:
        try:
            time.sleep(iv)
            now = time.time()

            if now - last_stats > window_min*60:
                mean, std, n = window_stats(obs_db_path, window_min)
                log(f"[NB] mean={mean:.1f} std={std:.1f} (n={n})")
                last_stats = now
                if std <= 0:
                    continue

            # live
            x, y, z = live_diff(coin)
            lower, upper = mean - k*std, mean + k*std

            # CoinEx pos guard
            b_units = read_balance(coin, balance_path, free=True)  # تعداد کوین نوبیتکس
            ok, pos, center, lo, hi = within_tobit_window(b_units)
            if not ok:
                log(f"[NB] skip: CoinEx pos {pos:.6f} outside ({lo:.6f}, {hi:.6f})")
                continue

            # maker prices
            ob = get_nobitex_orderbook(f"{coin}IRT")
            best_bid = float(ob["bids"][0]["price"])
            best_ask = float(ob["asks"][0]["price"])

            # ── SIGNALS (طبق تصحیح تو) ──
            if z > upper:
                # y ارزان‌تر → در نوبیتکس BUY
                side = "buy"
                # inventory guard in UNITS
                if float(b_units + float(order_qty)) > u_units:
                    log("[NB] skip BUY: would exceed u_units")
                    continue
                price_tmn = weighted_limit_price(side, best_bid, best_ask, w_buy)
                price_ri = ri(price_tmn)
                log(f"[NB] BUY {order_qty} @ {price_tmn:.0f}  z={z:.0f} (upper={upper:.0f})")
                resp = new_order(side, order_qty, f"{coin}IRT", price_ri)
                if parse_order_response(resp) == "success":
                    oid = resp["order"]["id"]
                    state, matched, _ = track_and_cancel(oid, f"{coin}IRT", side, timeout_sec, mean, std, k, coin)
                    if matched > 0:
                        buys.append({"id": oid, "p": float(price_tmn), "q": float(matched), "t": datetime.now().isoformat(), "side": "buy"})
                else:
                    log(f"[NB] order rejected: {resp}")

            elif z < lower:
                # y گران‌تر → در نوبیتکس SELL
                side = "sell"
                if float(b_units - float(order_qty)) < l_units:
                    log("[NB] skip SELL: would fall below l_units")
                    continue
                price_tmn = weighted_limit_price(side, best_bid, best_ask, w_sell)
                price_ri = ri(price_tmn)
                log(f"[NB] SELL {order_qty} @ {price_tmn:.0f}  z={z:.0f} (lower={lower:.0f})")
                resp = new_order(side, order_qty, f"{coin}IRT", price_ri)
                if parse_order_response(resp) == "success":
                    oid = resp["order"]["id"]
                    state, matched, _ = track_and_cancel(oid, f"{coin}IRT", side, timeout_sec, mean, std, k, coin)
                    if matched > 0:
                        sells.append({"id": oid, "p": float(price_tmn), "q": float(matched), "t": datetime.now().isoformat(), "side": "sell"})
                else:
                    log(f"[NB] order rejected: {resp}")

            # periodic dump
            if (len(buys) + len(sells)) >= 5:
                try:
                    d = pd.read_pickle(trades_path)
                except FileNotFoundError:
                    d = {"buy": [], "sell": []}
                d["buy"].extend(buys); d["sell"].extend(sells)
                pd.to_pickle(d, trades_path)
                buys.clear(); sells.clear()

        except KeyboardInterrupt:
            log("[NB] interrupted")
            break
        except Exception as e:
            log(f"[NB] error: {e}")
            traceback.print_exc()
            time.sleep(3)

if __name__ == "__main__":
    main()
