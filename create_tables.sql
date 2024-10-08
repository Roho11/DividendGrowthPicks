-- Table: public.fundamentals

-- DROP TABLE IF EXISTS public.fundamentals;

CREATE TABLE IF NOT EXISTS public.fundamentals
(
    lastupdated text COLLATE pg_catalog."default",
    sector text COLLATE pg_catalog."default",
    industry text COLLATE pg_catalog."default",
    gicsector text COLLATE pg_catalog."default",
    gicindustry text COLLATE pg_catalog."default",
    marketcapmln double precision,
    marketcapmlnusd double precision,
    marketcapname text COLLATE pg_catalog."default",
    eps double precision,
    beta double precision,
    pe double precision,
    ebitda double precision,
    forwardpe double precision,
    forwardeps double precision,
    payoutratio double precision,
    divstreak double precision,
    divgrowth1y double precision,
    divgrowth3y double precision,
    divsafetyscore double precision,
    divsafetyscorelocked double precision,
    lastquaterepssurprise double precision,
    lastquaterrevenuesurprise double precision,
    nextquaterepsestimate double precision,
    nextquaterrevenueestimate double precision,
    nextearningsreportdate text COLLATE pg_catalog."default",
    lastearningsreportdate text COLLATE pg_catalog."default",
    nextearningsreportreportingdate text COLLATE pg_catalog."default",
    lastearningsreportreportingdate text COLLATE pg_catalog."default",
    revenuegrowthyoy double precision,
    netincomegrowthyoy double precision,
    cashflowgrowthyoy double precision,
    companydescription text COLLATE pg_catalog."default",
    ticker text COLLATE pg_catalog."default",
    ignorecurrency double precision,
    nominal double precision,
    currentnominal double precision,
    realnominal double precision,
    maturitydate double precision,
    offerdate double precision,
    effectiveyield double precision,
    yieldcoupon double precision,
    currentyield double precision,
    modifcurrentyield double precision,
    yieldtomaturity double precision,
    yieldtomaturityportfolio double precision,
    effectiveyieldportfolio double precision,
    nkd double precision,
    couponssumm double precision,
    duration double precision,
    listinglevel double precision,
    countryiso text COLLATE pg_catalog."default",
    isin text COLLATE pg_catalog."default",
    bondtype double precision,
    issuedate double precision,
    term double precision,
    dividendtax double precision,
    lotsize double precision,
    expenseratio double precision,
    assetinfoid text COLLATE pg_catalog."default",
    currency text COLLATE pg_catalog."default",
    currentprice double precision,
    prevcloseprice double precision,
    lastdaygainsamount double precision,
    lastdaygainspercent double precision,
    divcurrency text COLLATE pg_catalog."default",
    nextdividenddate double precision,
    exdividenddate text COLLATE pg_catalog."default",
    nextdividendpershare double precision,
    divyieldfwd double precision,
    isdivyieldttm double precision,
    divperyearfwd double precision,
    divpaidttm double precision,
    divgrowth5y double precision,
    divgrowthstreak double precision,
    divfrequency double precision,
    financialscurrency text COLLATE pg_catalog."default",
    marketcapcurrency text COLLATE pg_catalog."default",
    type text COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    status double precision,
    traceid text COLLATE pg_catalog."default",
    CONSTRAINT unique_ticker_lastupdated UNIQUE (ticker, lastupdated)
)

TABLESPACE pg_default;


-- Table: public.results

-- DROP TABLE IF EXISTS public.results;

CREATE TABLE IF NOT EXISTS public.results
(
    lastupdated text COLLATE pg_catalog."default",
    ticker text COLLATE pg_catalog."default",
    currentprice double precision,
    sector text COLLATE pg_catalog."default",
    industry text COLLATE pg_catalog."default",
    companydescription text COLLATE pg_catalog."default",
    marketcapmln double precision,
    marketcapname text COLLATE pg_catalog."default",
    eps double precision,
    forwardeps double precision,
    pe double precision,
    payoutratio double precision,
    divyieldfwd double precision,
    divperyearfwd double precision,
    divgrowth1y double precision,
    divgrowth3y double precision,
    divgrowth5y double precision,
    divgrowthstreak bigint,
    divfrequency bigint,
    freecashflowpayout double precision,
    freecashflow bigint,
    sharenum double precision,
    divgrowthstreak_norm double precision,
    divyieldfwd_norm double precision,
    payoutratio_norm double precision,
    freecashflowpayout_norm double precision,
    inflation_norm1 bigint,
    inflation_norm3 bigint,
    inflation_norm5 double precision,
    inflation_norm double precision,
    points double precision
)

TABLESPACE pg_default;


CREATE TABLE last_results AS
SELECT *
FROM results
WHERE 1=0;
