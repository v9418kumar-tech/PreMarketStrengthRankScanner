import os
import json
import gzip
import threading
import time
import math
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo

import requests
import upstox_client
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

IST = ZoneInfo("Asia/Kolkata")

TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()

INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/NSE.json.gz"
)

MIN_PRICE = 20.0

# Upstox Full Feed current combined limit = 1500.
# One place is reserved for NIFTY 50.
MAX_SUBSCRIPTIONS = 1500
MAX_EQUITY_SUBSCRIPTIONS = MAX_SUBSCRIPTIONS - 1

NIFTY_KEY = "NSE_INDEX|Nifty 50"


INSTRUMENTS = []
BY_KEY = {}
BY_SYMBOL = {}

FEEDS = {}

STREAMER = None
STREAM_THREAD = None
STREAM_LOCK = threading.Lock()

LAST_TICK = 0.0

STREAM_STATUS = "Not started"
STREAM_ERROR = ""

STARTED = False


# ---------------------------------------------------------
# BASIC HELPERS
# ---------------------------------------------------------

def num(value, default=0.0):

    try:
        return float(value)

    except Exception:
        return default


def clamp(value, low=0.0, high=100.0):

    return max(
        low,
        min(high, num(value))
    )


def percentile(values):

    if not values:
        return []

    if len(values) == 1:
        return [50.0]

    order = sorted(
        range(len(values)),
        key=lambda i: values[i]
    )

    result = [50.0] * len(values)

    for rank, index in enumerate(order):

        result[index] = (
            rank * 100.0
            / (len(values) - 1)
        )

    return result


# ---------------------------------------------------------
# LOAD NSE EQUITY INSTRUMENTS
# ---------------------------------------------------------

def load_instruments():

    global INSTRUMENTS
    global BY_KEY
    global BY_SYMBOL

    response = requests.get(
        INSTRUMENT_URL,
        headers={
            "User-Agent":
                "Mozilla/5.0 PreMarketStrengthRankScanner"
        },
        timeout=30
    )

    response.raise_for_status()

    raw = gzip.decompress(
        response.content
    )

    data = json.loads(
        raw.decode("utf-8")
    )

    result = []

    by_key = {}
    by_symbol = {}

    for item in data:

        if item.get("segment") != "NSE_EQ":
            continue

        if item.get("instrument_type") != "EQ":
            continue

        symbol = str(
            item.get(
                "trading_symbol",
                ""
            )
        ).strip()

        key = str(
            item.get(
                "instrument_key",
                ""
            )
        ).strip()

        if not symbol or not key:
            continue

        row = {

            "symbol":
                symbol,

            "key":
                key,

            "name":
                item.get("short_name")
                or item.get("name")
                or symbol
        }

        result.append(row)

        by_key[key] = row

        by_symbol[symbol] = row

    # Stable ordering.
    result.sort(
        key=lambda x:
            x["symbol"]
    )

    INSTRUMENTS = result

    BY_KEY = by_key

    BY_SYMBOL = by_symbol


# ---------------------------------------------------------
# EXTRACT UPSTOX FEED
# ---------------------------------------------------------

def extract_feed(feed):

    full = (
        feed.get("fullFeed")
        or feed.get("full_feed")
        or {}
    )

    market = (
        full.get("marketFF")
        or full.get("market_ff")
        or {}
    )

    if not isinstance(
        market,
        dict
    ):
        market = {}

    ltpc = (
        full.get("ltpc")
        or feed.get("ltpc")
        or {}
    )

    if not isinstance(
        ltpc,
        dict
    ):
        ltpc = {}

    return (
        full,
        market,
        ltpc
    )


# ---------------------------------------------------------
# WEBSOCKET CALLBACKS
# ---------------------------------------------------------

def on_open():

    global STREAM_STATUS
    global STREAM_ERROR

    STREAM_STATUS = "Connected"

    STREAM_ERROR = ""


def on_message(message):

    global LAST_TICK
    global STREAM_STATUS

    if not isinstance(
        message,
        dict
    ):
        return

    feeds = (
        message.get("feeds")
        or {}
    )

    if not isinstance(
        feeds,
        dict
    ):
        return

    with STREAM_LOCK:

        for key, feed in feeds.items():

            if isinstance(
                feed,
                dict
            ):

                FEEDS[key] = feed

        LAST_TICK = time.time()

    STREAM_STATUS = "Live"


def on_error(error):

    global STREAM_STATUS
    global STREAM_ERROR

    STREAM_STATUS = "Error"

    STREAM_ERROR = str(
        error
    )[:250]


def on_close(*args):

    global STREAM_STATUS

    STREAM_STATUS = "Disconnected"


# ---------------------------------------------------------
# UPSTOX WEBSOCKET THREAD
# ---------------------------------------------------------

def stream_worker():

    global STREAMER
    global STREAM_STATUS
    global STREAM_ERROR

    try:

        if not TOKEN:

            raise RuntimeError(
                "UPSTOX_ACCESS_TOKEN Render "
                "Environment Variable is missing."
            )

        if not INSTRUMENTS:

            load_instruments()

        equity_keys = [

            item["key"]

            for item in
            INSTRUMENTS[
                :MAX_EQUITY_SUBSCRIPTIONS
            ]

        ]

        keys = (
            equity_keys
            + [NIFTY_KEY]
        )

        configuration = (
            upstox_client.Configuration()
        )

        configuration.access_token = TOKEN

        STREAMER = (
            upstox_client.MarketDataStreamerV3(
                upstox_client.ApiClient(
                    configuration
                ),
                keys,
                "full"
            )
        )

        STREAMER.on(
            "open",
            on_open
        )

        STREAMER.on(
            "message",
            on_message
        )

        STREAMER.on(
            "error",
            on_error
        )

        STREAMER.on(
            "close",
            on_close
        )

        STREAMER.auto_reconnect(
            True,
            5,
            20
        )

        STREAM_STATUS = (
            "Connecting ("
            + str(len(equity_keys))
            + " stocks + NIFTY)"
        )

        STREAMER.connect()

    except Exception as error:

        STREAM_STATUS = "Error"

        STREAM_ERROR = str(
            error
        )[:250]


# ---------------------------------------------------------
# START STREAM ONLY ON FIRST USE
# ---------------------------------------------------------

def ensure_stream():

    global STARTED
    global STREAM_THREAD

    if STARTED:
        return

    with STREAM_LOCK:

        if STARTED:
            return

        STARTED = True

        STREAM_THREAD = (
            threading.Thread(
                target=stream_worker,
                daemon=True,
                name="upstox-market-feed"
            )
        )

        STREAM_THREAD.start()


# ---------------------------------------------------------
# GET STORED FEED
# ---------------------------------------------------------

def get_feed_for_key(key):

    with STREAM_LOCK:

        feed = FEEDS.get(
            key
        )

        if feed:
            return feed

        return FEEDS.get(
            key.replace(
                "|",
                ":"
            )
        )


# ---------------------------------------------------------
# LTP / PREVIOUS CLOSE / IEP
# ---------------------------------------------------------

def get_ltp_close(feed):

    _, _, ltpc = (
        extract_feed(feed)
    )

    return (

        num(
            ltpc.get(
                "ltp"
            )
        ),

        num(
            ltpc.get(
                "cp"
            )
        ),

        num(
            ltpc.get(
                "iep"
            )
        )
    )


# ---------------------------------------------------------
# LIVE PRE-OPEN DATA
# ---------------------------------------------------------

def get_market_values(feed):

    _, market, ltpc = (
        extract_feed(feed)
    )

    # IEP
    iep = num(
        market.get(
            "iep"
        )
    )

    if iep <= 0:

        iep = num(
            ltpc.get(
                "iep"
            )
        )

    # Total Buy Quantity
    buy = num(
        market.get(
            "tbq"
        )
    )

    # Total Sell Quantity
    sell = num(
        market.get(
            "tsq"
        )
    )

    # Compatibility fallback
    if buy <= 0 or sell <= 0:

        efeed = (

            market.get(
                "eFeedDetails"
            )

            or

            market.get(
                "e_feed_details"
            )

            or {}
        )

        if buy <= 0:

            buy = num(
                efeed.get(
                    "tbq"
                )
            )

        if sell <= 0:

            sell = num(
                efeed.get(
                    "tsq"
                )
            )

    # Actual indicative imbalance
    actual_imbalance = num(
        market.get(
            "iiqTotal"
        )
    )

    total = (
        buy
        + sell
    )

    # Buy/Sell imbalance
    if total > 0:

        buy_sell_imbalance = (
            (buy - sell)
            / total
        )

    else:

        buy_sell_imbalance = 0.0

    # Fallback if actual imbalance field
    # is not available.
    if (
        actual_imbalance == 0
        and total > 0
    ):

        actual_imbalance = (
            buy - sell
        )

    ieq = num(
        market.get(
            "ieq"
        )
    )

    return (

        iep,

        buy,

        sell,

        buy_sell_imbalance,

        actual_imbalance,

        ieq
    )


# ---------------------------------------------------------
# PREVIOUS DAY FROM UPSTOX DAILY CANDLE
# ---------------------------------------------------------

def get_previous_day(feed):

    full, market, ltpc = (
        extract_feed(feed)
    )

    ohlc_block = (

        full.get(
            "marketOHLC"
        )

        or

        full.get(
            "market_ohlc"
        )

        or {}
    )

    candles = (
        ohlc_block.get(
            "ohlc"
        )
        or []
    )

    if not isinstance(
        candles,
        list
    ):
        candles = []

    daily = None

    for candle in candles:

        if str(
            candle.get(
                "interval",
                ""
            )
        ).lower() == "1d":

            daily = candle

            break

    if not daily:

        return {

            "open": 0,

            "high": 0,

            "low": 0,

            "close":
                num(
                    ltpc.get(
                        "cp"
                    )
                ),

            "volume": 0
        }

    close = num(
        daily.get(
            "close"
        )
    )

    volume = num(
        daily.get(
            "vol"
        )
    )

    if close <= 0:

        close = num(
            ltpc.get(
                "cp"
            )
        )

    return {

        "open":
            num(
                daily.get(
                    "open"
                )
            ),

        "high":
            num(
                daily.get(
                    "high"
                )
            ),

        "low":
            num(
                daily.get(
                    "low"
                )
            ),

        "close":
            close,

        "volume":
            volume
    }


# ---------------------------------------------------------
# MAIN SCANNER
# ---------------------------------------------------------

def scan():

    ensure_stream()

    now = datetime.now(
        IST
    )

    current_time = now.time()

    # Pre-open window.
    if (
        current_time
        < dt_time(
            8,
            55
        )

        or

        current_time
        > dt_time(
            9,
            16
        )
    ):

        return {

            "time":
                now.strftime(
                    "%H:%M:%S"
                ),

            "count": 0,

            "rows": [],

            "message":
                "Pre-open session is not active.",

            "nifty_gap": 0,

            "stream":
                STREAM_STATUS
        }

    with STREAM_LOCK:

        snapshot = dict(
            FEEDS
        )

    # -----------------------------------------------------
    # NIFTY
    # -----------------------------------------------------

    nifty_feed = (
        snapshot.get(
            NIFTY_KEY
        )
    )

    if not nifty_feed:

        nifty_feed = snapshot.get(
            NIFTY_KEY.replace(
                "|",
                ":"
            )
        )

    if not nifty_feed:

        return {

            "time":
                now.strftime(
                    "%H:%M:%S"
                ),

            "count": 0,

            "rows": [],

            "message":
                "NIFTY pre-open data is "
                "not received yet.",

            "nifty_gap": 0,

            "stream":
                STREAM_STATUS
        }

    (
        nifty_iep,
        _,
        _,
        _,
        _,
        _
    ) = get_market_values(
        nifty_feed
    )

    (
        _,
        nifty_cp,
        nifty_ltpc_iep
    ) = get_ltp_close(
        nifty_feed
    )

    if nifty_iep <= 0:

        nifty_iep = (
            nifty_ltpc_iep
        )

    if (
        nifty_iep <= 0
        or nifty_cp <= 0
    ):

        return {

            "time":
                now.strftime(
                    "%H:%M:%S"
                ),

            "count": 0,

            "rows": [],

            "message":
                "NIFTY IEP अभी उपलब्ध नहीं है.",

            "nifty_gap": 0,

            "stream":
                STREAM_STATUS
        }

    nifty_gap = (

        (
            nifty_iep
            - nifty_cp
        )
        / nifty_cp

    ) * 100


    # -----------------------------------------------------
    # STOCK CANDIDATES
    # -----------------------------------------------------

    candidates = []

    for item in INSTRUMENTS[
        :MAX_EQUITY_SUBSCRIPTIONS
    ]:

        feed = snapshot.get(
            item["key"]
        )

        if not feed:

            feed = snapshot.get(
                item["key"].replace(
                    "|",
                    ":"
                )
            )

        if not feed:
            continue

        (
            iep,
            buy,
            sell,
            bs_imbalance,
            actual_imbalance,
            ieq
        ) = get_market_values(
            feed
        )

        if iep <= 0:
            continue

        if iep < MIN_PRICE:
            continue

        (
            _,
            previous_close,
            _
        ) = get_ltp_close(
            feed
        )

        if previous_close <= 0:
            continue

        gap = (

            (
                iep
                - previous_close
            )
            / previous_close

        ) * 100

        # Positive / upside shares only.
        if gap <= 0:
            continue

        total_orders = (
            buy
            + sell
        )

        if total_orders <= 0:
            continue

        # -------------------------------------------------
        # PREVIOUS DAY STRENGTH
        # -------------------------------------------------

        previous = (
            get_previous_day(
                feed
            )
        )

        previous_day_close = (
            previous["close"]
            or previous_close
        )

        previous_return = 0.0

        if (
            previous_day_close > 0
            and previous["open"] > 0
        ):

            previous_return = (

                (
                    previous_day_close
                    - previous["open"]
                )
                / previous["open"]

            ) * 100

        day_range = (

            previous["high"]
            - previous["low"]
        )

        if day_range > 0:

            close_position = (

                (
                    previous_day_close
                    - previous["low"]
                )
                / day_range
            )

        else:

            close_position = 0.5

        return_score = clamp(

            (
                previous_return
                + 3
            )
            / 6
            * 100
        )

        previous_strength = (

            return_score
            * 0.40

            +

            clamp(
                close_position
                * 100
            )
            * 0.60
        )

        high_position = clamp(
            close_position
            * 100
        )

        # -------------------------------------------------
        # RELATIVE STRENGTH VS NIFTY
        # -------------------------------------------------

        outperformance = (
            gap
            - nifty_gap
        )

        relative_score = clamp(

            (
                outperformance
                + 1
            )
            / 3
            * 100
        )

        # Previous-day turnover
        previous_turnover = (

            previous_day_close
            * previous["volume"]
        )

        candidates.append({

            "symbol":
                item["symbol"],

            "name":
                item["name"],

            "iep":
                iep,

            "gap":
                gap,

            "buy":
                buy,

            "sell":
                sell,

            "imbalance":
                bs_imbalance,

            "actual_imbalance":
                actual_imbalance,

            "ieq":
                ieq,

            "previous":
                previous_strength,

            "high_position":
                high_position,

            "relative":
                relative_score,

            "turnover":
                previous_turnover
        })


    # -----------------------------------------------------
    # NO CANDIDATES
    # -----------------------------------------------------

    if not candidates:

        return {

            "time":
                now.strftime(
                    "%H:%M:%S"
                ),

            "count": 0,

            "rows": [],

            "message":
                "Live pre-open data received, "
                "but no positive candidate has "
                "complete order data yet.",

            "nifty_gap":
                round(
                    nifty_gap,
                    2
                ),

            "stream":
                STREAM_STATUS
        }


    # -----------------------------------------------------
    # LIVE ORDER RANKINGS
    # -----------------------------------------------------

    buy_scores = percentile([

        math.log1p(
            x["buy"]
        )

        for x in candidates
    ])


    imbalance_scores = percentile([

        x["imbalance"]

        for x in candidates
    ])


    liquidity_scores = percentile([

        math.log10(
            max(
                x["turnover"],
                1
            )
        )

        for x in candidates
    ])


    # -----------------------------------------------------
    # FINAL SCORE
    # -----------------------------------------------------

    rows = []

    for i, c in enumerate(
        candidates
    ):

        # 30% IEP Gap
        gap_score = clamp(

            c["gap"]
            / 3
            * 100
        )

        # 25% Buy/Sell imbalance
        imbalance_score = (
            imbalance_scores[i]
        )

        # 20% Absolute Buy Quantity
        buy_score = (
            buy_scores[i]
        )

        # 10% NIFTY relative strength
        relative_score = (
            c["relative"]
        )

        # 10% Previous-day strength
        previous_score = (
            c["previous"]
        )

        # 5% Liquidity
        liquidity_score = (
            liquidity_scores[i]
        )

        final_score = (

            gap_score
            * 0.30

            +

            imbalance_score
            * 0.25

            +

            buy_score
            * 0.20

            +

            relative_score
            * 0.10

            +

            previous_score
            * 0.10

            +

            liquidity_score
            * 0.05
        )

        rows.append({

            "symbol":
                c["symbol"],

            "name":
                c["name"],

            "score":
                round(
                    clamp(
                        final_score
                    ),
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
                int(
                    c["buy"]
                ),

            "sell":
                int(
                    c["sell"]
                ),

            "imbalance":
                round(
                    c["imbalance"]
                    * 100,
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


    # -----------------------------------------------------
    # SORT
    # -----------------------------------------------------

    rows.sort(

        key=lambda x: (

            -x["score"],

            -x["gap"],

            -x["imbalance"],

            x["symbol"]
        )
    )


    for rank, row in enumerate(
        rows,
        1
    ):

        row["rank"] = rank


    return {

        "time":
            now.strftime(
                "%H:%M:%S"
            ),

        "count":
            len(rows),

        "rows":
            rows[:100],

        "message":
            "Live Pre-Market Ranking",

        "nifty_gap":
            round(
                nifty_gap,
                2
            ),

        "stream":
            STREAM_STATUS,

        "subscribed":
            min(
                len(INSTRUMENTS),
                MAX_EQUITY_SUBSCRIPTIONS
            )
    }


# ---------------------------------------------------------
# HTML
# ---------------------------------------------------------

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
line-height:1.45;
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
line-height:1.5;
}

</style>

</head>

<body>

<h2>
🌅 Pre-Market Strength Rank
</h2>

<div class="sub">

NSE EQ • Live Pre-Open Order Flow •
0–100 • Strongest First

</div>

<button onclick="scan()">

SCAN NOW

</button>

<div id="status"
class="status">

Pre-open live feed का इंतजार…

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

30% IEP Gap •
25% Buy/Sell Imbalance •
20% Buy Quantity •
10% NIFTY Relative •
10% Previous Strength •
5% Liquidity

<br>

Live data:
Upstox Market Data Feed V3 Full

</div>


<script>

async function scan(){

const status =
document.getElementById(
"status"
);

status.innerText =
"Live feed से data लिया जा रहा है…";

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

data.message

+ " • NIFTY IEP Gap: "
+ data.nifty_gap
+ "%"

+ " • "
+ data.time

+ " • Candidates: "
+ data.count

+ " • Feed: "
+ data.stream;


const body =
document.getElementById(
"rows"
);


body.innerHTML =
(data.rows || [])

.map(
x => `

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

`
)

.join("");

}

catch(error){

status.innerText =
"⚠ Connection problem. "
+ "SCAN NOW दबाएँ।";

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


# ---------------------------------------------------------
# HOME
# ---------------------------------------------------------

@app.route("/")
def home():

    ensure_stream()

    return render_template_string(
        HTML
    )


# ---------------------------------------------------------
# API
# ---------------------------------------------------------

@app.route("/api/scan")
def api_scan():

    try:

        result = scan()

        return jsonify(
            result
        )

    except Exception as error:

        return jsonify({

            "time":
                datetime.now(
                    IST
                ).strftime(
                    "%H:%M:%S"
                ),

            "count": 0,

            "rows": [],

            "nifty_gap": 0,

            "stream":
                STREAM_STATUS,

            "error":
                str(error)[:300]
        })


# ---------------------------------------------------------
# LOCAL RUN
# ---------------------------------------------------------

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
