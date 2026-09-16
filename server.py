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

# NORMAL / RELAXED CONDITIONS
MAX_EQUITY_SUBSCRIPTIONS=1499
MIN_GAP=-0.25
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


# -------------------------------------------------
# BASIC HELPERS
# -------------------------------------------------

def num(v,d=0.0):
    try:
        if v is None:
            return d
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

    order=sorted(
        range(len(values)),
        key=lambda i:values[i]
    )

    out=[50.0]*len(values)

    for r,i in enumerate(order):
        out[i]=r*100.0/(len(values)-1)

    return out


# -------------------------------------------------
# INSTRUMENTS
# -------------------------------------------------

def load_instruments():
    global INSTRUMENTS

    if INSTRUMENTS:
        return INSTRUMENTS

    try:
        r=requests.get(
            INSTRUMENT_URL,
            timeout=30
        )
        r.raise_for_status()

        raw=gzip.decompress(r.content)

        data=json.loads(raw.decode("utf-8"))

        arr=[]

        for x in data:
            if not isinstance(x,dict):
                continue

            segment=str(
                x.get("segment","")
            ).upper()

            inst_type=str(
                x.get("instrument_type","")
            ).upper()

            if segment!="NSE_EQ":
                continue

            if inst_type!="EQ":
                continue

            key=x.get("instrument_key")

            symbol=x.get("trading_symbol")

            if not key or not symbol:
                continue

            arr.append({
                "symbol":str(symbol),
                "key":str(key),
                "name":str(
                    x.get("name") or symbol
                )
            })

        arr.sort(
            key=lambda x:x["symbol"]
        )

        INSTRUMENTS=arr

        return INSTRUMENTS

    except Exception as e:
        print("Instrument error:",e)
        return []


# -------------------------------------------------
# FEED EXTRACTION
# -------------------------------------------------

def extract_feed(feed):

    if not isinstance(feed,dict):
        return {},{},{}

    full=(
        feed.get("fullFeed")
        or feed.get("full_feed")
        or feed.get("ff")
        or {}
    )

    if not isinstance(full,dict):
        full={}

    market=(
        full.get("marketFF")
        or full.get("market_ff")
        or {}
    )

    if not isinstance(market,dict):
        market={}

    ltpc=(
        market.get("ltpc")
        or full.get("ltpc")
        or feed.get("ltpc")
        or {}
    )

    if not isinstance(ltpc,dict):
        ltpc={}

    return full,market,ltpc


def get_feed(key,snapshot):

    feed=snapshot.get(key)

    if feed:
        return feed

    alt=key.replace("|",":")

    feed=snapshot.get(alt)

    if feed:
        return feed

    return {}


# -------------------------------------------------
# LTP / CLOSE / IEP
# -------------------------------------------------

def get_ltp_close(feed):

    _,market,ltpc=extract_feed(feed)

    ltp=num(
        ltpc.get("ltp")
    )

    cp=num(
        ltpc.get("cp")
    )

    iep=num(
        ltpc.get("iep")
    )

    if iep<=0:
        iep=num(
            market.get("iep")
        )

    if ltp<=0:
        ltp=num(
            market.get("ltp")
        )

    return ltp,cp,iep


# -------------------------------------------------
# MARKET VALUES
# -------------------------------------------------

def get_market_values(feed):

    _,market,ltpc=extract_feed(feed)

    iep=0.0
    buy=0.0
    sell=0.0
    ieq=0.0
    actual_imbalance=0.0

    # IEP
    iep=num(
        market.get("iep")
    )

    if iep<=0:
        iep=num(
            ltpc.get("iep")
        )

    # BUY
    buy=num(
        market.get("tbq")
    )

    # SELL
    sell=num(
        market.get("tsq")
    )

    # Additional possible locations
    efeed=(
        market.get("eFeedDetails")
        or market.get("e_feed_details")
        or {}
    )

    if isinstance(efeed,dict):

        if buy<=0:
            buy=num(
                efeed.get("tbq")
            )

        if sell<=0:
            sell=num(
                efeed.get("tsq")
            )

        if ieq<=0:
            ieq=num(
                efeed.get("ieq")
            )

    # Some feeds can provide these through ltpc
    if buy<=0:
        buy=num(
            ltpc.get("tbq")
        )

    if sell<=0:
        sell=num(
            ltpc.get("tsq")
        )

    # IEQ
    ieq=num(
        market.get("ieq")
    )

    if ieq<=0 and isinstance(efeed,dict):
        ieq=num(
            efeed.get("ieq")
        )

    # Total imbalance
    actual_imbalance=num(
        market.get("iiqTotal")
    )

    if actual_imbalance==0:
        actual_imbalance=num(
            market.get("iiq_total")
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


# -------------------------------------------------
# PREVIOUS DAY
# -------------------------------------------------

def get_previous_day(feed):

    full,_,ltpc=extract_feed(feed)

    block=(
        full.get("marketOHLC")
        or full.get("market_ohlc")
        or {}
    )

    if not isinstance(block,dict):
        block={}

    candles=(
        block.get("ohlc")
        or []
    )

    if not isinstance(candles,list):
        candles=[]

    daily=None

    for c in candles:

        if not isinstance(c,dict):
            continue

        if str(
            c.get("interval","")
        ).lower()=="1d":

            daily=c
            break

    if not daily:

        return {
            "open":0,
            "high":0,
            "low":0,
            "close":num(
                ltpc.get("cp")
            ),
            "volume":0
        }

    close=num(
        daily.get("close")
    )

    if close<=0:
        close=num(
            ltpc.get("cp")
        )

    return {
        "open":num(
            daily.get("open")
        ),
        "high":num(
            daily.get("high")
        ),
        "low":num(
            daily.get("low")
        ),
        "close":close,
        "volume":num(
            daily.get("vol")
        )
    }


# -------------------------------------------------
# WEBSOCKET
# -------------------------------------------------

def on_open():

    global STREAM_STATUS,STREAM_ERROR

    STREAM_STATUS="Connected"
    STREAM_ERROR=""

    print("UPSTOX STREAM CONNECTED")


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

    STREAM_ERROR=str(error)

    print("STREAM ERROR:",error)


def on_close():

    global STREAM_STATUS

    STREAM_STATUS="Closed"

    print("UPSTOX STREAM CLOSED")


def stream_worker():

    global STREAMER

    if not TOKEN:

        print("UPSTOX_ACCESS_TOKEN missing")

        return

    instruments=load_instruments()

    if not instruments:

        print("No instruments loaded")

        return

    equity_keys=[
        x["key"]
        for x in instruments[
            :MAX_EQUITY_SUBSCRIPTIONS
        ]
    ]

    keys=equity_keys+[NIFTY_KEY]

    print(
        "Subscribing:",
        len(keys),
        "instruments"
    )

    try:

        configuration=upstox_client.Configuration()

        configuration.access_token=TOKEN

        api_client=upstox_client.ApiClient(
            configuration
        )

        STREAMER=upstox_client.MarketDataStreamerV3(
            api_client,
            keys,
            "full"
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

        STREAMER.connect()

    except Exception as e:

        global STREAM_STATUS,STREAM_ERROR

        STREAM_STATUS="Error"

        STREAM_ERROR=str(e)

        print(
            "STREAM WORKER ERROR:",
            e
        )


def ensure_stream():

    global STREAM_THREAD,STARTED

    if STARTED:
        return

    STARTED=True

    STREAM_THREAD=threading.Thread(
        target=stream_worker,
        daemon=True
    )

    STREAM_THREAD.start()


# -------------------------------------------------
# SCANNER
# -------------------------------------------------

def scan():

    ensure_stream()

    now=datetime.now(IST)

    current_time=now.time()

    if current_time<dt_time(9,0):

        return {
            "ok":True,
            "message":
                "Pre-open अभी शुरू नहीं हुआ है.",
            "candidates":[],
            "top10":[],
            "diagnostics":{}
        }

    if current_time>=dt_time(9,15):

        return {
            "ok":True,
            "message":
                "Pre-open session समाप्त हो गया है.",
            "candidates":[],
            "top10":[],
            "diagnostics":{}
        }

    with STREAM_LOCK:

        snapshot=dict(FEEDS)

    if not snapshot:

        return {
            "ok":True,
            "message":
                "Live feed आ रहा है, data collect हो रहा है...",
            "candidates":[],
            "top10":[],
            "diagnostics":{}
        }

    instruments=load_instruments()

    diagnostics={
        "feeds_seen":0,
        "iep_count":0,
        "price_count":0,
        "positive_gap":0,
        "normal_gap":0,
        "buy_sell_data":0,
        "candidates":0
    }

    # ---------------------------------------------
    # NIFTY
    # ---------------------------------------------

    nifty_feed=get_feed(
        NIFTY_KEY,
        snapshot
    )

    nifty_iep,nifty_buy,nifty_sell,_,_,_=get_market_values(
        nifty_feed
    )

    _,nifty_cp,nifty_ltpc_iep=get_ltp_close(
        nifty_feed
    )

    if nifty_iep<=0:
        nifty_iep=nifty_ltpc_iep

    nifty_gap=0.0

    if nifty_iep>0 and nifty_cp>0:

        nifty_gap=(
            (nifty_iep-nifty_cp)
            /nifty_cp
        )*100


    candidates=[]

    # ---------------------------------------------
    # STOCK LOOP
    # ---------------------------------------------

    for inst in instruments[
        :MAX_EQUITY_SUBSCRIPTIONS
    ]:

        key=inst["key"]

        feed=get_feed(
            key,
            snapshot
        )

        if not feed:
            continue

        diagnostics["feeds_seen"]+=1

        iep,buy,sell,imbalance,actual_imbalance,ieq=\
            get_market_values(feed)

        ltp,previous_close_from_ltpc,ltpc_iep=\
            get_ltp_close(feed)

        previous=get_previous_day(feed)

        previous_close=num(
            previous.get("close")
        )

        if previous_close<=0:
            previous_close=previous_close_from_ltpc

        if previous_close<=0:
            continue

        # IEP fallback
        if iep<=0:
            iep=ltpc_iep

        # Price fallback
        display_price=iep

        if display_price<=0:
            display_price=ltp

        if display_price<=0:
            display_price=previous_close

        if display_price<=0:
            continue

        diagnostics["price_count"]+=1

        if display_price<MIN_PRICE:
            continue

        if iep>0:
            diagnostics["iep_count"]+=1

        # -----------------------------------------
        # NORMAL GAP
        # -----------------------------------------

        gap=0.0

        if iep>0 and previous_close>0:

            gap=(
                (iep-previous_close)
                /previous_close
            )*100

        # बहुत छोटी negative gap भी allow
        if gap<MIN_GAP:
            continue

        diagnostics["normal_gap"]+=1

        if gap>0:
            diagnostics["positive_gap"]+=1

        # -----------------------------------------
        # BUY / SELL
        # -----------------------------------------

        total_orders=buy+sell

        if total_orders>0:
            diagnostics["buy_sell_data"]+=1

        # अब Buy/Sell 0 होने पर share reject नहीं होगा

        # -----------------------------------------
        # PREVIOUS STRENGTH
        # -----------------------------------------

        prev_open=num(
            previous.get("open")
        )

        prev_high=num(
            previous.get("high")
        )

        prev_low=num(
            previous.get("low")
        )

        prev_volume=num(
            previous.get("volume")
        )

        previous_return=0.0

        if prev_open>0:

            previous_return=(
                (previous_close-prev_open)
                /prev_open
            )*100

        close_position=0.5

        if prev_high>prev_low:

            close_position=(
                previous_close-prev_low
            )/(prev_high-prev_low)

            close_position=clamp(
                close_position,
                0,
                1
            )

        return_score=clamp(
            (previous_return+5)/10*100
        )

        previous_strength=(
            return_score*0.40
            +
            close_position*100*0.60
        )

        # -----------------------------------------
        # RELATIVE STRENGTH
        # -----------------------------------------

        outperformance=(
            gap-nifty_gap
        )

        relative_score=clamp(
            (outperformance+1)/3*100
        )

        # -----------------------------------------
        # TURNOVER
        # -----------------------------------------

        turnover=(
            previous_close*prev_volume
        )

        candidates.append({

            "symbol":inst["symbol"],

            "name":inst["name"],

            "score":0,

            "iep":display_price,

            "gap":gap,

            "buy":buy,

            "sell":sell,

            "imbalance":imbalance*100,

            "ieq":ieq,

            "previous_strength":
                previous_strength,

            "relative":
                relative_score,

            "turnover":
                turnover/10000000,

            "actual_imbalance":
                actual_imbalance
        })


    diagnostics["candidates"]=len(candidates)

    if not candidates:

        return {
            "ok":True,
            "message":
                "Live feed connected है, लेकिन अभी candidate नहीं मिला.",
            "candidates":[],
            "top10":[],
            "nifty_gap":round(
                nifty_gap,2
            ),
            "diagnostics":diagnostics
        }


    # ---------------------------------------------
    # PERCENTILE RANKING
    # ---------------------------------------------

    buy_scores=percentile([
        c["buy"]
        for c in candidates
    ])

    imbalance_scores=percentile([
        c["imbalance"]
        for c in candidates
    ])

    ieq_scores=percentile([
        c["ieq"]
        for c in candidates
    ])

    liquidity_scores=percentile([
        c["turnover"]
        for c in candidates
    ])


    # ---------------------------------------------
    # FINAL SCORE
    # ---------------------------------------------

    for i,c in enumerate(candidates):

        gap_score=clamp(
            (c["gap"]+0.25)
            /2.25*100
        )

        c["score"]=round(
            gap_score*0.25
            +
            imbalance_scores[i]*0.20
            +
            buy_scores[i]*0.15
            +
            ieq_scores[i]*0.15
            +
            c["relative"]*0.10
            +
            c["previous_strength"]*0.10
            +
            liquidity_scores[i]*0.05,
            1
        )

    # ---------------------------------------------
    # SORT
    # ---------------------------------------------

    candidates.sort(
        key=lambda c:(
            c["score"],
            c["gap"],
            c["imbalance"],
            c["buy"]
        ),
        reverse=True
    )

    for rank,c in enumerate(
        candidates,
        start=1
    ):
        c["rank"]=rank

    top10=candidates[:10]

    return {

        "ok":True,

        "message":
            "Live pre-open data मिल रहा है.",

        "scan_time":
            now.strftime("%H:%M:%S"),

        "last_updated":
            now.strftime("%H:%M:%S"),

        "nifty_gap":
            round(nifty_gap,2),

        "candidates":
            candidates[:100],

        "top10":
            top10,

        "diagnostics":
            diagnostics,

        "feed_status":
            STREAM_STATUS,

        "feed_error":
            STREAM_ERROR
    }


# -------------------------------------------------
# API
# -------------------------------------------------

@app.route("/")
def index():

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

        print(
            "SCAN ERROR:",
            e
        )

        return jsonify({

            "ok":False,

            "message":
                "Scanner error: "+str(e),

            "candidates":[],

            "top10":[],

            "diagnostics":{}

        })


# -------------------------------------------------
# HTML
# -------------------------------------------------

HTML=r"""
<!DOCTYPE html>
<html>
<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width,initial-scale=1">

<title>Pre-Market Strength Rank</title>

<style>

body{
    margin:0;
    background:#07111f;
    color:#e8eef7;
    font-family:Arial,sans-serif;
}

.wrap{
    max-width:1400px;
    margin:auto;
    padding:14px;
}

h1{
    margin:0 0 5px;
    font-size:25px;
}

.sub{
    color:#9fb0c5;
    margin-bottom:12px;
}

button{
    background:#1769ff;
    color:white;
    border:0;
    padding:10px 18px;
    border-radius:8px;
    font-size:15px;
    cursor:pointer;
}

.status{
    margin-top:12px;
    background:#101d30;
    border:1px solid #233550;
    border-radius:10px;
    padding:12px;
    line-height:1.6;
}

.box{
    margin-top:14px;
    background:#0d1929;
    border:1px solid #223650;
    border-radius:10px;
    padding:12px;
}

.box h2{
    margin:0 0 10px;
    font-size:18px;
}

.top{
    display:grid;
    grid-template-columns:repeat(5,1fr);
    gap:8px;
}

.card{
    background:#14243a;
    border-radius:8px;
    padding:10px;
}

.card .rank{
    color:#8fb8ff;
    font-size:13px;
}

.card .sym{
    font-size:18px;
    font-weight:bold;
    margin:4px 0;
}

.card .score{
    font-size:20px;
    font-weight:bold;
}

.tablebox{
    overflow:auto;
    margin-top:14px;
}

table{
    width:100%;
    border-collapse:collapse;
    min-width:1050px;
}

th,td{
    padding:9px 8px;
    border-bottom:1px solid #1d2e45;
    text-align:right;
    white-space:nowrap;
}

th{
    background:#132238;
    color:#b9c9dc;
    position:sticky;
    top:0;
}

th:nth-child(2),
td:nth-child(2){
    text-align:left;
}

tr:hover{
    background:#12243b;
}

.strong{
    background:#063b78;
}

.green{
    color:#55e69a;
}

.red{
    color:#ff7272;
}

.small{
    color:#8ea2bb;
    font-size:12px;
    margin-top:8px;
}

@media(max-width:800px){

    .top{
        grid-template-columns:repeat(2,1fr);
    }

    h1{
        font-size:21px;
    }
}

</style>

</head>

<body>

<div class="wrap">

<h1>🌅 Pre-Market Strength Rank</h1>

<div class="sub">
NSE EQ • Live Pre-Open Order Flow • 0–100 • Strongest First
</div>

<button onclick="loadData()">
SCAN NOW
</button>

<div class="status" id="status">
Scanner starting...
</div>

<div class="box">

<h2>🏆 TOP 10</h2>

<div class="top" id="top10">
Waiting for data...
</div>

</div>

<div class="box">

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

</div>

<div class="small">
Relaxed pre-open mode • Small negative gap allowed •
Buy/Sell data आने में delay होने पर stock हटाया नहीं जाएगा
</div>

</div>


<script>

function n(v,d=0){

    let x=Number(v);

    return Number.isFinite(x) ? x : d;
}


function fmt(v,d=2){

    return n(v).toLocaleString(
        "en-IN",
        {
            minimumFractionDigits:d,
            maximumFractionDigits:d
        }
    );
}


function loadData(){

    fetch(
        "/api/scan?ts="+Date.now()
    )
    .then(r=>r.json())
    .then(data=>render(data))
    .catch(err=>{

        document.getElementById("status").innerText=
            "Scanner connection error: "+err;

    });

}


function render(data){

    let diag=data.diagnostics || {};

    let status=data.message || "";

    status +=
        " • NIFTY Gap: "+
        fmt(data.nifty_gap,2)+
        "%";

    status +=
        " • Scan: "+
        (data.scan_time || "--");

    status +=
        " • Last Updated: "+
        (data.last_updated || "--");

    status +=
        " • Candidates: "+
        (data.candidates || []).length;

    status +=
        " • Feed: "+
        (data.feed_status || "Unknown");

    status +=
        " • IEP: "+
        (diag.iep_count || 0);

    status +=
        " • Positive Gap: "+
        (diag.positive_gap || 0);

    document.getElementById(
        "status"
    ).innerText=status;


    let top=data.top10 || [];

    let topHtml="";

    if(!top.length){

        topHtml=
            '<div class="card">No data अभी</div>';

    }else{

        top.forEach(c=>{

            topHtml +=

            '<div class="card">'+

            '<div class="rank">#'+
            c.rank+
            '</div>'+

            '<div class="sym">'+
            c.symbol+
            '</div>'+

            '<div class="score">'+
            fmt(c.score,1)+
            '</div>'+

            '<div>IEP ₹'+
            fmt(c.iep,2)+
            '</div>'+

            '<div class="'+
            (c.gap>=0?'green':'red')+
            '">Gap '+
            fmt(c.gap,2)+
            '%</div>'+

            '</div>';

        });

    }

    document.getElementById(
        "top10"
    ).innerHTML=topHtml;


    let rows="";

    (data.candidates || []).forEach(c=>{

        let cls=c.score>=70 ?
            "strong":"";

        rows +=

        '<tr class="'+cls+'">'+

        '<td>'+c.rank+'</td>'+

        '<td><b>'+c.symbol+'</b></td>'+

        '<td><b>'+fmt(c.score,1)+'</b></td>'+

        '<td>₹'+fmt(c.iep,2)+'</td>'+

        '<td class="'+
        (c.gap>=0?'green':'red')+
        '">'+
        fmt(c.gap,2)+
        '%</td>'+

        '<td>'+
        fmt(c.buy,0)+
        '</td>'+

        '<td>'+
        fmt(c.sell,0)+
        '</td>'+

        '<td>'+
        fmt(c.imbalance,2)+
        '%</td>'+

        '<td>'+
        fmt(c.ieq,0)+
        '</td>'+

        '<td>'+
        fmt(c.previous_strength,1)+
        '</td>'+

        '<td>'+
        fmt(c.relative,1)+
        '</td>'+

        '<td>'+
        fmt(c.turnover,2)+
        '</td>'+

        '</tr>';

    });


    if(!rows){

        rows=
        '<tr><td colspan="12" style="text-align:center">'+
        'अभी candidate नहीं मिला'+
        '</td></tr>';

    }

    document.getElementById(
        "rows"
    ).innerHTML=rows;

}


loadData();

setInterval(
    loadData,
    5000
);

</script>

</body>
</html>
"""


# -------------------------------------------------
# START
# -------------------------------------------------

if __name__=="__main__":

    ensure_stream()

    port=int(
        os.getenv(
            "PORT",
            "5000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
