
Order submission front-end to Interactive Brokers
1) Load buy orders from spreadsheet using ATR position sizing and ATR stops
    provide buttons to put in entry and, after order submission, put in stop order
    Limitation: for limit orders, must use TWS to determine if filled - no polling
2) Load portfolio positions to allow 
    a. Selling all or portion of position
    b. Put in stop orders: 
        1. Percent only is trailing %, 
        2. Dollar value only is dollar stop, 
        3. Both % and $ amounts is initial dollar trail amount and then trailing percent.

Other tools to come.
