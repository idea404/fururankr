# https://content1.edgar-online.com/cfeed/ext/charts.dll?81-0-0-0-0-103112018-03NA000000BABL&SF:1000-FREQ=6-STOK=ZiMwFay/KjxipW63av/xteRmDedqCRw2w31qEB+JjY8OLOvycv1kdpz4nyZfdF2D-1635934351244
#
# https://content1.edgar-online.com/cfeed/ext/charts.dll?81-0-0-0-0-103112018-03NA000000BABL&SF:1000-FREQ=6-STOK=ZiMwFay/KjxipW63av/xteRmDedqCRw2w31qEB+JjY8OLOvycv1kdpz4nyZ2222D-1635934351244
#
# https://content1.edgar-online.com/cfeed/ext/charts.dll?81-0-0-0-0-115122017-03NA000000BABL&SF:1000-FREQ=6-STOK=ZiMwFay/KjxipW63av/xteRmDedqCRw2w31qEB+JjY8OLOvycv1kdpz4nyZfdF2D-1635934581514

# https://finance-services.msn.com/Market.svc/ChartDataV5?symbols=125.1.BABL.PNK&chartType=5y&isEOD=False&lang=en-GB&isCS=true&isVol=true

#
# ~date|close|v|open|high|low~
#
# https://www.reddit.com/r/algotrading/comments/agiowu/is_there_anywhere_to_get_otc_market_data_for_free/
#
# POST https://app-money.tmx.com/graphql
# --- BODY ---
# {
#   "operationName": "getTimeSeriesData",
#   "variables": {
#     "symbol": "BABL:US",
#     "freq": "week",
#     "start": "2016-11-03",
#     "end": null
#   },
#   "query": "query getTimeSeriesData($symbol: String!, $freq: String, $interval: Int, $start: String, $end: String, $startDateTime: Int, $endDateTime: Int) {\n  getTimeSeriesData(symbol: $symbol, freq: $freq, interval: $interval, start: $start, end: $end, startDateTime: $startDateTime, endDateTime: $endDateTime) {\n    dateTime\n    open\n    high\n    low\n    close\n    volume\n    __typename\n  }\n}\n"
# }
