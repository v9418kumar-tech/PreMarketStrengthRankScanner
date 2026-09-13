import os
import io
import csv
import gzip
import json
import math
import zipfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

IST = ZoneInfo("Asia/Kolkata")

TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()

QUOTE_URL = "https://api.upstox.com/v3/market-quote/quotes"
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
BHAV_URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"

BATCH_SIZE = 500
MIN_PRICE = 20.0
MIN_PREV_TURNOVER = 10_00_00_000
MIN_GAP = 0.10

HEADERS = {
    "User-Agent": "Mozilla/5.0 PreMarketStrengthRankScanner",
    "Accept": "application/json"
}

INSTRUMENTS = []
PREVIOUS = {}
LAST_RESULT = None


def clamp(x, low=0, high=100):
    return max(low, min(high, float(x)))


def number(v, default=0):
    try:
        return float(v)
    except:
        return default


def integer(v, default=0):
    try:
        return int(float(v))
    except:
        return default


def load_instruments():
    global INSTRUMENTS

    r = requests.get(
        INSTRUMENT_URL,
        headers={"User-Agent": HEADERS["User-Agent"]},
        timeout=30
    )
    r.raise_for_status()

    raw = gzip.decompress(r.content)
    data = json.loads(raw.decode("utf-8"))

    result = []

    for item in data:

        if item.get("segment") != "NSE_EQ":
            continue

        if item.get("instrument_type") != "EQ":
            continue

        symbol = item.get("trading_symbol", "").strip()
        key = item.get("instrument_key", "").strip()

        if not symbol or not key:
            continue

        result.append({
            "symbol": symbol,
            "key": key,
            "name": item.get("short_name")
                    or item.get("name")
                    or symbol
        })

    INSTRUMENTS = result


def previous_trading_day():
    d = datetime.now(IST).date() - timedelta(days=1)

    for _ in range(10):

        if d.weekday() < 5:
            return d

        d -= timedelta(days=1)

    return d


def load_previous_bhavcopy():

    global PREVIOUS

    start = previous_trading_day()

    for back in range(8):

        d = start - timedelta(days=back)

        if d.weekday() >= 5:
            continue

        date_text = d.strftime("%Y%m%d")

        url = BHAV_URL.format(date=date_text)

        try:

            r = requests.get(
                url,
                headers={
                    "User-Agent": HEADERS["User-Agent"],
                    "Accept": "*/*"
                },
                timeout=20
            )

            if r.status_code != 200:
                continue

            if len(r.content) < 1000:
                continue

            with zipfile.ZipFile(io.BytesIO(r.content)) as z:

                csv_file = None

                for name in z.namelist():

                    if name.lower().endswith(".csv"):
                        csv_file = name
                        break

                if not csv_file:
                    continue

                raw = z.read(csv_file)

            text = raw.decode(
                "utf-8-sig",
                errors="replace"
            )

            reader = csv.DictReader(
                io.StringIO(text)
            )

            previous = {}

            for row in reader:

                series = (
                    row.get("SctySrs")
                    or row.get("SERIES")
                    or ""
                ).upper()

                if series != "EQ":
                    continue

                symbol = (
                    row.get("TckrSymb")
                    or row.get("SYMBOL")
                    or ""
                ).strip()

                if not symbol:
                    continue

                close = number(
                    row.get("ClsPric")
                    or row.get("CLOSE")
                )

                previous_close = number(
                    row.get("PrvsClsgPric")
                    or row.get("PREV_CLOSE")
                )

                high = number(
                    row.get("HghPric")
                    or row.get("HIGH")
                )

                low = number(
                    row.get("LwPric")
                    or row.get("LOW")
                )

                volume = integer(
                    row.get("TtlTradgVol")
                    or row.get("TOTTRDQTY")
                )

                turnover = number(
                    row.get("TtlTrfVal")
                    or row.get("TOTTRDVAL")
                )

                if turnover <= 0:
                    turnover = close * volume

                previous[symbol] = {
                    "close": close,
                    "previous_close": previous_close,
                    "high": high,
                    "low": low,
                    "volume": volume,
                    "turnover": turnover
                }

            if previous:

                PREVIOUS = previous

                return date_text

        except Exception:
            continue

    raise RuntimeError(
        "NSE previous-day data could not be loaded."
    )


def quote_batch(keys):

    if not TOKEN:
        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN Render Environment Variable is missing."
        )

    headers = dict(HEADERS)

    headers["Authorization"] = (
        "Bearer " + TOKEN
    )

    response = requests.get(
        QUOTE_URL,
        headers=headers,
        params={
            "instrument_key": ",".join(keys)
        },
        timeout=12
    )

    if response.status_code != 200:

        raise RuntimeError(
            "Upstox error "
            + str(response.status_code)
        )

    return response.json().get(
        "data",
        {}
    )


def get_all_quotes():

    keys = [
        item["key"]
        for item in INSTRUMENTS
    ]

    result = {}

    for i in range(
        0,
        len(keys),
        BATCH_SIZE
    ):

        batch = keys[
            i:i + BATCH_SIZE
        ]

        result.update(
            quote_batch(batch)
        )

    nifty = quote_batch([
        "NSE_INDEX|Nifty 50"
    ])

    result.update(nifty)

    return result


def find_quote(quotes, key, symbol):

    q = quotes.get(
        key.replace("|", ":")
    )

    if q:
        return q

    return quotes.get(
        "NSE_EQ:" + symbol
    )


def get_iep(q):

    value = number(
        q.get(
            "indicative_equilibrium_price"
        )
    )

    if value > 0:
        return value

    ltpc = q.get("ltpc") or {}

    return number(
        ltpc.get("iep")
    )


def get_orders(q):

    buy = number(
        q.get("total_buy_quantity")
    )

    sell = number(
        q.get("total_sell_quantity")
    )

    if buy + sell > 0:
        return buy, sell

    depth = q.get("depth") or {}

    buy = sum(
        number(x.get("quantity"))
        for x in depth.get("buy", [])
    )

    sell = sum(
        number(x.get("quantity"))
        for x in depth.get("sell", [])
    )

    return buy, sell


def percentile_scores(values):

    if not values:
        return []

    order = sorted(
        range(len(values)),
        key=lambda i: values[i]
    )

    result = [50.0] * len(values)

    if len(values) == 1:
        return result

    for rank, index in enumerate(order):

        result[index] = (
            rank / (len(values) - 1)
        ) * 100

    return result


def calculate_scan():

    if not INSTRUMENTS:
        load_instruments()

    if not PREVIOUS:
        load_previous_bhavcopy()

    quotes = get_all_quotes()

    nifty = (
        quotes.get("NSE_INDEX:Nifty 50")
        or quotes.get("NSE_INDEX|Nifty 50")
        or {}
    )

    nifty_iep = get_iep(nifty)

    nifty_previous = number(
        nifty.get("prev_close_price")
    )

    if nifty_iep <= 0:
        nifty_iep = number(
            nifty.get("last_price")
        )

    nifty_gap = 0

    if (
        nifty_iep > 0
        and nifty_previous > 0
    ):
        nifty_gap = (
            (nifty_iep - nifty_previous)
            / nifty_previous
        ) * 100

    candidates = []

    for instrument in INSTRUMENTS:

        symbol = instrument["symbol"]

        previous = PREVIOUS.get(symbol)

        if not previous:
            continue

        quote = find_quote(
            quotes,
            instrument["key"],
            symbol
        )

        if not quote:
            continue

        previous_close = (
            previous["close"]
            or number(
                quote.get(
                    "prev_close_price"
                )
            )
        )

        if previous_close <= 0:
            continue

        iep = get_iep(quote)

        if iep <= 0:
            continue

        if iep < MIN_PRICE:
            continue

        gap = (
            (iep - previous_close)
            / previous_close
        ) * 100

        # We want positive/upside stocks.
        if gap < MIN_GAP:
            continue

        turnover = previous["turnover"]

        if turnover < MIN_PREV_TURNOVER:
            continue

        buy, sell = get_orders(quote)

        total_orders = buy + sell

        previous_volume = max(
            previous["volume"],
            1
        )

        participation = (
            total_orders
            / previous_volume
        )

        if total_orders > 0:

            imbalance = (
                (buy - sell)
                / total_orders
            )

        else:

            imbalance = 0

        # Previous-day strength.
        day_range = (
            previous["high"]
            - previous["low"]
        )

        if day_range > 0:

            close_position = (
                previous["close"]
                - previous["low"]
            ) / day_range

        else:

            close_position = 0.5

        if previous["previous_close"] > 0:

            previous_return = (
                (
                    previous["close"]
                    - previous["previous_close"]
                )
                / previous["previous_close"]
            ) * 100

        else:

            previous_return = 0

        return_score = clamp(
            (
                previous_return + 3
            ) / 6 * 100
        )

        previous_strength = (
            close_position * 100 * 0.60
            + return_score * 0.40
        )

        # Previous-day high position.
        high_position = (
            close_position * 100
        )

        # Relative strength against NIFTY.
        outperformance = (
            gap - nifty_gap
        )

        relative_score = clamp(
            (
                outperformance + 1
            ) / 3 * 100
        )

        candidates.append({

            "symbol": symbol,

            "name": instrument["name"],

            "iep": iep,

            "gap": gap,

            "buy": buy,

            "sell": sell,

            "imbalance": imbalance,

            "participation": participation,

            "previous_strength":
                previous_strength,

            "high_position":
                high_position,

            "relative":
                relative_score,

            "turnover":
                turnover
        })

    if not candidates:

        return {
            "time":
                datetime.now(
                    IST
                ).strftime("%H:%M:%S"),

            "nifty_gap":
                round(nifty_gap, 2),

            "count": 0,

            "rows": [],

            "message":
                "Pre-open data अभी उपलब्ध नहीं है।"
        }

    participation_scores = percentile_scores(
        [
            math.log1p(
                x["participation"]
            )
            for x in candidates
        ]
    )

    liquidity_scores = percentile_scores(
        [
            math.log10(
                max(
                    x["turnover"],
                    1
                )
            )
            for x in candidates
        ]
    )

    rows = []

    for i, c in enumerate(candidates):

        # 1. IEP Gap = 25%
        gap_score = clamp(
            c["gap"]
            / 3
            * 100
        )

        # 2. Pre-open participation = 15%
        quantity_score = (
            participation_scores[i]
        )

        # 3. Buy/Sell imbalance = 15%
        imbalance_score = clamp(
            (
                c["imbalance"]
                + 1
            ) * 50
        )

        # 4. Previous-day strength = 15%
        previous_score = (
            c["previous_strength"]
        )

        # 5. Previous-day high position = 10%
        high_score = (
            c["high_position"]
        )

        # 6. Relative strength vs NIFTY = 10%
        relative_score = (
            c["relative"]
        )

        # 7. Liquidity / quality = 10%
        liquidity_score = (
            liquidity_scores[i]
        )

        final_score = (

            gap_score * 0.25

            + quantity_score * 0.15

            + imbalance_score * 0.15

            + previous_score * 0.15

            + high_score * 0.10

            + relative_score * 0.10

            + liquidity_score * 0.10
        )

        rows.append({

            "symbol":
                c["symbol"],

            "name":
                c["name"],

            "score":
                round(
                    clamp(final_score),
                    1
                ),

            "iep":
                round(
                    c["iep"],
                    2
                ),

            "gap":
                round(
                    c["gap"],
                    2
                ),

            "buy":
                int(c["buy"]),

            "sell":
                int(c["sell"]),

            "imbalance":
                round(
                    c["imbalance"] * 100,
                    1
                ),

            "previous":
                round(
                    previous_score,
                    1
                ),

            "relative":
                round(
                    relative_score,
                    1
                ),

            "turnover":
                round(
                    c["turnover"]
                    / 10000000,
                    2
                )
        })

    rows.sort(
        key=lambda x: (
            -x["score"],
            -x["gap"],
            x["symbol"]
        )
    )

    for rank, row in enumerate(
        rows,
        start=1
    ):

        row["rank"] = rank

    return {

        "time":
            datetime.now(
                IST
            ).strftime("%H:%M:%S"),

        "nifty_gap":
            round(
                nifty_gap,
                2
            ),

        "count":
            len(rows),

        "rows":
            rows[:100],

        "message":
            "Live Pre-Market Ranking"
    }


HTML = """
<!DOCTYPE html>
<html lang="hi">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width,initial-scale=1">

<title>
Pre-Market Strength Rank
</title>

<style>

*{
box-sizing:border-box;
}

body{
margin:0;
padding:7px;
background:#10151b;
color:#e9eef5;
font-family:Arial,sans-serif;
}

h2{
margin:7px 0 2px;
font-size:20px;
}

.sub{
font-size:12px;
color:#9ca7b4;
margin-bottom:8px;
}

button{
width:100%;
padding:11px;
border:0;
border-radius:8px;
background:#2677ee;
color:white;
font-size:15px;
font-weight:bold;
}

.status{
margin:8px 0;
padding:8px;
background:#19222c;
border-radius:8px;
font-size:12px;
}

.tablebox{
overflow:auto;
}

table{
border-collapse:collapse;
width:100%;
min-width:760px;
background:#131a22;
}

th,td{
padding:7px;
border-bottom:1px solid #29333d;
font-size:12px;
white-space:nowrap;
text-align:right;
}

th{
background:#1c2732;
position:sticky;
top:0;
}

th:first-child,
td:first-child,
th:nth-child(2),
td:nth-child(2){
text-align:left;
}

.score{
font-weight:bold;
font-size:14px;
}

.note{
margin-top:8px;
font-size:11px;
color:#8e9aa8;
line-height:1.4;
}

</style>

</head>

<body>

<h2>
🌅 Pre-Market Strength Rank
</h2>

<div class="sub">
NSE EQ • 0–100 Score • Strongest First
</div>

<button onclick="scan()">
SCAN NOW
</button>

<div id="status"
class="status">
Pre-open data का इंतजार…
</div>

<div class="tablebox">

<table>

<thead>

<tr>

<th>Rank</th>
<th>Symbol</th>
<th>Score</th>
<th>IEP</th>
<th>Gap %</th>
<th>Buy Qty</th>
<th>Sell Qty</th>
<th>Imbalance %</th>
<th>Prev Strength</th>
<th>Relative</th>
<th>Turnover ₹Cr</th>

</tr>

</thead>

<tbody id="rows">
</tbody>

</table>

</div>

<div class="note">

25% IEP Gap •
15% Pre-open participation •
15% Buy/Sell imbalance •
15% Previous-day strength •
10% Previous-day high position •
10% NIFTY relative strength •
10% Liquidity/quality

</div>

<script>

async function scan(){

const status =
document.getElementById("status");

status.innerText =
"Scanning…";

try{

const response =
await fetch(
"/api/scan?ts="
+ Date.now()
);

const data =
await response.json();

if(data.error){

status.innerText =
"⚠ "
+ data.error;

return;

}

status.innerText =
"NIFTY IEP Gap: "
+ data.nifty_gap
+ "% • "
+ data.message
+ " • "
+ data.time
+ " • "
+ data.count
+ " candidates";

const body =
document.getElementById("rows");

body.innerHTML =
(data.rows || [])
.map(x => `

<tr>

<td>
${x.rank}
</td>

<td>
<b>${x.symbol}</b>
</td>

<td class="score">
${x.score}
</td>

<td>
₹${x.iep}
</td>

<td>
${x.gap}%
</td>

<td>
${x.buy.toLocaleString()}
</td>

<td>
${x.sell.toLocaleString()}
</td>

<td>
${x.imbalance}%
</td>

<td>
${x.previous}
</td>

<td>
${x.relative}
</td>

<td>
₹${x.turnover}
</td>

</tr>

`)
.join("");

}

catch(error){

status.innerText =
"⚠ Connection problem. SCAN NOW दबाएँ।";

}

}

scan();

setInterval(
scan,
5000
);

</script>

</body>

</html>
"""


@app.route("/")
def home():

    return render_template_string(
        HTML
    )


@app.route("/api/scan")
def api_scan():

    try:

        result = calculate_scan()

        return jsonify(result)

    except Exception as e:

        return jsonify({

            "time":
                datetime.now(
                    IST
                ).strftime("%H:%M:%S"),

            "nifty_gap": 0,

            "count": 0,

            "rows": [],

            "error":
                str(e)[:300]
        })


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "5000"
            )
        )
    )
