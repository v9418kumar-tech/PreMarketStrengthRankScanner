import os,json,gzip,threading,time,math
from datetime import datetime,time as dt_time
from zoneinfo import ZoneInfo

import requests
import upstox_client
from flask import Flask,jsonify,render_template_string

app=Flask(__name__)

IST=ZoneInfo("Asia/Kolkata")
TOKEN=os.getenv("UPSTOX_ACCESS_TOKEN","").strip()

INSTRUMENT_URL="https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

MIN_PRICE=20.0
MAX_EQUITY_SUBSCRIPTIONS=1499
NIFTY_KEY="NSE_INDEX|Nifty 50"

INSTRUMENTS=[]
FEEDS={}

STREAMER=None
STREAM_THREAD=None
STREAM_LOCK=threading.Lock()

LAST_TICK=0
STREAM_STATUS="Not started"
STREAM_ERROR=""
STARTED=False


def num(v,d=0.0):
    try:
        return float(v)
    except:
        return d


def clamp(v,lo=0,hi=100):
    return max(lo,min(hi,num(v)))


def percentile(values):
    if not values:
        return []
    if len(values)==1:
        return [50.0]

    order=sorted(range(len(values)),key=lambda i:values[i])
    out=[50.0]*len(values)

    for r,i in enumerate(order):
        out[i]=r*100.0/(len(values)-1)

    return out


def load_instruments():
    global INSTRUMENTS

    r=requests.get(
        INSTRUMENT_URL,
        headers={"User-Agent":"Mozilla/5.0 PreMarketStrengthRankScanner"},
        timeout=30
    )
    r.raise_for_status()

    data=json.loads(
        gzip.decompress(r.content).decode("utf-8")
    )

    result=[]

    for x in data:

        if x.get("segment")!="NSE_EQ":
            continue

        if x.get("instrument_type")!="EQ":
            continue

        symbol=str(x.get("trading_symbol","")).strip()
        key=str(x.get("instrument_key","")).strip()

        if not symbol or not key:
            continue

        result.append({
            "symbol":symbol,
            "key":key,
            "name":x.get("short_name") or x.get("name") or symbol
        })

    result.sort(key=lambda x:x["symbol"])
    INSTRUMENTS=result


def extract_feed(feed):

    full=(
        feed.get("fullFeed")
        or feed.get("full_feed")
        or feed.get("ff")
        or {}
    )

    market=(
        full.get("marketFF")
        or full.get("market_ff")
        or {}
    )

    ltpc=(
        full.get("ltpc")
        or feed.get("ltpc")
        or {}
    )

    if not isinstance(market,dict):
        market={}

    if not isinstance(ltpc,dict):
        ltpc={}

    return full,market,ltpc


def on_open():
    global STREAM_STATUS,STREAM_ERROR

    STREAM_STATUS="Connected"
    STREAM_ERROR=""


def on_message(message):
    global LAST_TICK,STREAM_STATUS

    if not isinstance(message,dict):
        return

    feeds=message.get("feeds") or {}

    if not isinstance(feeds,dict):
        return

    with STREAM_LOCK:

        for key,feed in feeds.items():

            if isinstance(feed,dict):
                FEEDS[key]=feed

        LAST_TICK=time.time()

    STREAM_STATUS="Live"


def on_error(error):
    global STREAM_STATUS,STREAM_ERROR

    STREAM_STATUS="Error"
    STREAM_ERROR=str(error)[:250]


def on_close(*args):
    global STREAM_STATUS

    STREAM_STATUS="Disconnected"


def stream_worker():

    global STREAMER,STREAM_STATUS,STREAM_ERROR

    try:

        if not TOKEN:
            raise RuntimeError(
                "UPSTOX_ACCESS_TOKEN Render Environment Variable is missing."
            )

        if not INSTRUMENTS:
            load_instruments()

        equity_keys=[
            x["key"]
            for x in INSTRUMENTS[:MAX_EQUITY_SUBSCRIPTIONS]
        ]

        keys=equity_keys+[NIFTY_KEY]

        configuration=upstox_client.Configuration()
        configuration.access_token=TOKEN

        STREAMER=upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(configuration),
            keys,
            "full"
        )

        STREAMER.on("open",on_open)
        STREAMER.on("message",on_message)
        STREAMER.on("error",on_error)
        STREAMER.on("close",on_close)

        STREAMER.auto_reconnect(True,5,20)

        STREAM_STATUS=(
            f"Connecting ({len(equity_keys)} stocks + NIFTY)"
        )

        STREAMER.connect()

    except Exception as e:

        STREAM_STATUS="Error"
        STREAM_ERROR=str(e)[:250]


def ensure_stream():

    global STARTED,STREAM_THREAD

    if STARTED:
        return

    with STREAM_LOCK:

        if STARTED:
            return

        STARTED=True

        STREAM_THREAD=threading.Thread(
            target=stream_worker,
            daemon=True,
            name="upstox-market-feed"
        )

        STREAM_THREAD.start()


def get_feed(key,snapshot):

    feed=snapshot.get(key)

    if feed:
        return feed

    return snapshot.get(key.replace("|",":"))


def get_ltp_close(feed):

    _,_,ltpc=extract_feed(feed)

    return(
        num(ltpc.get("ltp")),
        num(ltpc.get("cp")),
        num(ltpc.get("iep"))
    )


def get_market_values(feed):

    _,market,ltpc=extract_feed(feed)

    iep=num(market.get("iep"))

    if iep<=0:
        iep=num(ltpc.get("iep"))

    buy=num(market.get("tbq"))
    sell=num(market.get("tsq"))

    if buy<=0 or sell<=0:

        efeed=(
            market.get("eFeedDetails")
            or market.get("e_feed_details")
            or {}
        )

        if buy<=0:
            buy=num(efeed.get("tbq"))

        if sell<=0:
            sell=num(efeed.get("tsq"))

    ieq=num(market.get("ieq"))

    actual_imbalance=num(
        market.get("iiqTotal")
    )

    total=buy+sell

    if total>0:
        imbalance=(buy-sell)/total
    else:
        imbalance=0.0

    if actual_imbalance==0 and total>0:
        actual_imbalance=buy-sell

    return(
        iep,
        buy,
        sell,
        imbalance,
        actual_imbalance,
        ieq
    )


def get_previous_day(feed):

    full,_,ltpc=extract_feed(feed)

    block=(
        full.get("marketOHLC")
        or full.get("market_ohlc")
        or {}
    )

    candles=block.get("ohlc") or []

    if not isinstance(candles,list):
        candles=[]

    daily=None

    for c in candles:

        if str(
            c.get("interval","")
        ).lower()=="1d":

            daily=c
            break

    if not daily:

        return{
            "open":0,
            "high":0,
            "low":0,
            "close":num(ltpc.get("cp")),
            "volume":0
        }

    close=num(daily.get("close"))

    if close<=0:
        close=num(ltpc.get("cp"))

    return{
        "open":num(daily.get("open")),
        "high":num(daily.get("high")),
        "low":num(daily.get("low")),
        "close":close,
        "volume":num(daily.get("vol"))
    }


def scan():

    ensure_stream()

    now=datetime.now(IST)
    current=now.time()

    if current<dt_time(9,0):

        return{
            "time":now.strftime("%H:%M:%S"),
            "updated":"",
            "count":0,
            "rows":[],
            "top10":[],
            "message":"Pre-open 9:00 AM पर शुरू होगा.",
            "nifty_gap":0,
            "stream":STREAM_STATUS
        }

    if current>=dt_time(9,15):

        return{
            "time":now.strftime("%H:%M:%S"),
            "updated":"",
            "count":0,
            "rows":[],
            "top10":[],
            "message":"Pre-open session समाप्त हो चुका है.",
            "nifty_gap":0,
            "stream":STREAM_STATUS
        }

    with STREAM_LOCK:
        snapshot=dict(FEEDS)
        last_tick=LAST_TICK

    if not snapshot:

        return{
            "time":now.strftime("%H:%M:%S"),
            "updated":"",
            "count":0,
            "rows":[],
            "top10":[],
            "message":"Upstox live pre-open feed का इंतजार…",
            "nifty_gap":0,
            "stream":STREAM_STATUS
        }

    nifty_feed=get_feed(
        NIFTY_KEY,
        snapshot
    )

    nifty_gap=0.0

    if nifty_feed:

        n_iep,_,_,_,_,_=get_market_values(
            nifty_feed
        )

        _,n_cp,n_ltpc_iep=get_ltp_close(
            nifty_feed
        )

        if n_iep<=0:
            n_iep=n_ltpc_iep

        if n_iep>0 and n_cp>0:

            nifty_gap=(
                (n_iep-n_cp)
                /n_cp
            )*100


    candidates=[]

    for item in INSTRUMENTS[:MAX_EQUITY_SUBSCRIPTIONS]:

        feed=get_feed(
            item["key"],
            snapshot
        )

        if not feed:
            continue

        (
            iep,
            buy,
            sell,
            imbalance,
            actual_imbalance,
            ieq
        )=get_market_values(feed)

        _,previous_close,ltpc_iep=get_ltp_close(feed)

        if previous_close<=0:
            continue

        if iep<=0:
            iep=ltpc_iep

        display_price=(
            iep
            if iep>0
            else previous_close
        )

        if display_price<MIN_PRICE:
            continue

        total_orders=buy+sell

        if total_orders<=0:
            continue

        gap=0.0

        if iep>0:

            gap=(
                (iep-previous_close)
                /previous_close
            )*100

            # IEP मिलने के बाद केवल positive
            # candidates रखें.
            if gap<=0:
                continue

        previous=get_previous_day(feed)

        prev_close=(
            previous["close"]
            or previous_close
        )

        previous_return=0.0

        if previous["open"]>0 and prev_close>0:

            previous_return=(
                (prev_close-previous["open"])
                /previous["open"]
            )*100

        day_range=(
            previous["high"]
            -previous["low"]
        )

        if day_range>0:

            close_position=(
                (prev_close-previous["low"])
                /day_range
            )

        else:
            close_position=0.5

        return_score=clamp(
            (previous_return+3)
            /6
            *100
        )

        previous_strength=(
            return_score*0.40
            +
            clamp(close_position*100)*0.60
        )

        outperformance=gap-nifty_gap

        relative_score=clamp(
            (outperformance+1)
            /3
            *100
        )

        turnover=(
            prev_close
            *previous["volume"]
        )

        candidates.append({

            "symbol":item["symbol"],

            "name":item["name"],

            "iep":iep,

            "price":display_price,

            "gap":gap,

            "buy":buy,

            "sell":sell,

            "imbalance":imbalance,

            "ieq":ieq,

            "previous":previous_strength,

            "relative":relative_score,

            "turnover":turnover
        })


    if not candidates:

        return{

            "time":now.strftime("%H:%M:%S"),

            "updated":(
                datetime.fromtimestamp(
                    last_tick,
                    IST
                ).strftime("%H:%M:%S")
                if last_tick else ""
            ),

            "count":0,

            "rows":[],

            "top10":[],

            "message":(
                "Live feed connected है, "
                "लेकिन अभी positive Buy/Sell candidate नहीं मिला."
            ),

            "nifty_gap":round(nifty_gap,2),

            "stream":STREAM_STATUS
        }


    buy_scores=percentile([

        math.log1p(x["buy"])
        for x in candidates

    ])


    imbalance_scores=percentile([

        x["imbalance"]
        for x in candidates

    ])


    ieq_scores=percentile([

        math.log1p(x["ieq"])
        for x in candidates

    ])


    liquidity_scores=percentile([

        math.log10(
            max(
                x["turnover"],
                1
            )
        )

        for x in candidates

    ])


    rows=[]


    for i,c in enumerate(candidates):

        gap_score=clamp(
            c["gap"]/3*100
        )

        buy_score=buy_scores[i]

        imbalance_score=(
            imbalance_scores[i]
        )

        ieq_score=ieq_scores[i]

        relative_score=c["relative"]

        previous_score=c["previous"]

        liquidity_score=(
            liquidity_scores[i]
        )


        final_score=(

            gap_score*0.25

            +

            imbalance_score*0.20

            +

            buy_score*0.15

            +

            ieq_score*0.15

            +

            relative_score*0.10

            +

            previous_score*0.10

            +

            liquidity_score*0.05
        )


        rows.append({

            "symbol":c["symbol"],

            "score":round(
                clamp(final_score),
                1
            ),

            "iep":round(
                c["price"],
                2
            ),

            "gap":round(
                c["gap"],
                2
            ),

            "buy":int(
                c["buy"]
            ),

            "sell":int(
                c["sell"]
            ),

            "imbalance":round(
                c["imbalance"]*100,
                1
            ),

            "ieq":int(
                c["ieq"]
            ),

            "previous":round(
                previous_score,
                1
            ),

            "relative":round(
                relative_score,
                1
            ),

            "turnover":round(
                c["turnover"]/10000000,
                2
            )
        })


    rows.sort(

        key=lambda x:(

            -x["score"],

            -x["gap"],

            -x["imbalance"],

            x["symbol"]
        )
    )


    for rank,row in enumerate(
        rows,
        1
    ):

        row["rank"]=rank


    updated=""

    if last_tick:

        updated=datetime.fromtimestamp(
            last_tick,
            IST
        ).strftime("%H:%M:%S")


    return{

        "time":now.strftime("%H:%M:%S"),

        "updated":updated,

        "count":len(rows),

        "rows":rows[:100],

        "top10":rows[:10],

        "message":"Live Pre-Market Ranking",

        "nifty_gap":round(
            nifty_gap,
            2
        ),

        "stream":STREAM_STATUS,

        "subscribed":min(
            len(INSTRUMENTS),
            MAX_EQUITY_SUBSCRIPTIONS
        )
    }


HTML="""
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
box-sizing:border-box
}

body{
margin:0;
padding:7px;
background:#10151b;
color:#e9eef5;
font-family:Arial,sans-serif
}

h2{
margin:7px 0 2px;
font-size:20px
}

.sub{
font-size:12px;
color:#9ca7b4;
margin-bottom:8px
}

button{
width:100%;
padding:11px;
border:0;
border-radius:8px;
background:#2677ee;
color:white;
font-size:15px;
font-weight:bold
}

.status{
margin:8px 0;
padding:8px;
background:#19222c;
border-radius:8px;
font-size:12px;
line-height:1.5
}

.topbox{
margin:8px 0;
padding:8px;
background:#151e27;
border-radius:8px
}

.tophead{
font-weight:bold;
font-size:14px;
margin-bottom:6px
}

.toprow{
display:flex;
justify-content:space-between;
padding:5px 2px;
border-bottom:1px solid #29333d;
font-size:12px
}

.toprow:last-child{
border-bottom:0
}

.tablebox{
overflow:auto
}

table{
border-collapse:collapse;
width:100%;
min-width:850px;
background:#131a22
}

th,td{
padding:7px;
border-bottom:1px solid #29333d;
font-size:12px;
white-space:nowrap;
text-align:right
}

th{
background:#1c2732;
position:sticky;
top:0
}

th:first-child,
td:first-child,
th:nth-child(2),
td:nth-child(2){
text-align:left
}

.score{
font-weight:bold;
font-size:14px
}

.note{
margin-top:8px;
font-size:11px;
color:#8e9aa8;
line-height:1.5
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

9:00 AM pre-open feed का इंतजार…

</div>


<div class="topbox">

<div class="tophead">
🏆 Top 10 Pre-Market Strength
</div>

<div id="top10">
अभी data उपलब्ध नहीं है।
</div>

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

<th>IEQ</th>

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
20% Buy/Sell Imbalance •
15% Buy Quantity •
15% IEQ Participation •
10% NIFTY Relative •
10% Previous Strength •
5% Liquidity

<br><br>

<strong>
Last Updated = Upstox feed का आखिरी प्राप्त tick
</strong>

<br>

IEP/IEQ उपलब्ध होते ही ranking अधिक accurate होगी।

</div>


<script>

async function scan(){

const status=
document.getElementById(
"status"
);

try{

const response=
await fetch(
"/api/scan?ts="
+Date.now()
);

const data=
await response.json();


if(data.error){

status.innerText=
"⚠ "
+data.error;

return;

}


status.innerText=

data.message

+" • NIFTY Gap: "
+data.nifty_gap
+"%"

+" • Scan: "
+data.time

+" • Last Updated: "
+(data.updated || "--")

+" • Candidates: "
+data.count

+" • Feed: "
+data.stream;


const top=
document.getElementById(
"top10"
);


if(data.top10 && data.top10.length){

top.innerHTML=
data.top10.map(
x=>`

<div class="toprow">

<span>
<b>#${x.rank}</b>
&nbsp; ${x.symbol}
</span>

<span>
<b>${x.score}</b>
&nbsp; ${x.gap}%
</span>

</div>

`
).join("");

}else{

top.innerHTML=
"अभी data उपलब्ध नहीं है।";

}


const body=
document.getElementById(
"rows"
);


body.innerHTML=
(data.rows || []).map(
x=>`

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
${x.ieq.toLocaleString()}
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
).join("");

}

catch(error){

status.innerText=
"⚠ Connection problem. "
+"SCAN NOW दबाएँ।";

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

    ensure_stream()

    return render_template_string(
        HTML
    )


@app.route("/api/scan")
def api_scan():

    try:

        return jsonify(
            scan()
        )

    except Exception as e:

        return jsonify({

            "time":
                datetime.now(
                    IST
                ).strftime(
                    "%H:%M:%S"
                ),

            "count":0,

            "rows":[],

            "top10":[],

            "nifty_gap":0,

            "stream":
                STREAM_STATUS,

            "error":
                str(e)[:300]
        })


if __name__=="__main__":

    app.run(

        host="0.0.0.0",

        port=int(
            os.getenv(
                "PORT",
                "5000"
            )
        )
    )
