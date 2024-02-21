/*This SQL script calculates security-level institutional ownership*/

CREATE TABLE work.mic_exchange (
iso text,
mic_exchange_code text
);

COPY work.mic_exchange (iso, mic_exchange_code)
FROM '/home/ubuntu/jmp/data/mic_exchange.csv'
DELIMITER ','
CSV HEADER;
/*------------------------------------*/
/*#1: subset of qualifying securities*/
/*-----------------------------------*/
-- proxy for termination date from security prices table;
CREATE TABLE work.termination AS
SELECT fsym_ID, max(price_date) AS termination_date
FROM factset.own_sec_prices_eq
GROUP BY fsym_ID;

select count(*) from work.termination;

/* securities that are defined as Equity or ADR in ownership, or Preferred if defined as PREFEQ in sym_coverage;*/
/*add exchange information to security table*/


CREATE TABLE work.equity_secs AS
SELECT t1.fsym_id, t1.issue_type, t1.iso_country, t1.fref_security_type,
t2.iso AS ex_country, /*legacy code for security listing information*/
security_name
FROM (SELECT a.fsym_id, a.issue_type, a.iso_country, a.mic_exchange_code, a.security_name, b.fref_security_type
      FROM factset.own_sec_coverage_eq a LEFT JOIN factset.sym_coverage b
ON a.fsym_id = b.fsym_id
WHERE a.fsym_id IN (SELECT DISTINCT c.fsym_id FROM factset.own_sec_prices_eq c)
AND (a.issue_type IN ('EQ','AD') OR (issue_type = 'PF' AND b.fref_security_type = 'PREFEQ')))  t1,
work.mic_exchange t2
WHERE t1.mic_exchange_code=t2.mic_exchange_code;

select count(*) from work.equity_secs;

/*add dummies for local stock and depository receipts*/

CREATE TABLE work.own_basic AS
SELECT a.*, b.factset_entity_id, c.termination_date,
CASE WHEN iso_country=ex_country THEN 1 ELSE 0 END AS islocal,
CASE WHEN issue_type='AD' THEN 1 ELSE 0 END AS isdr
from work.equity_secs a, factset.own_sec_entity_eq b, work.termination c
WHERE a.fsym_id = b.fsym_id AND a.fsym_id = c.fsym_id;

UPDATE WORK.own_basic AS a
	SET factset_entity_id =  b.factset_entity_id
	FROM work.dlc b
    WHERE a.factset_entity_id = b.dlc_entity_id
	AND  exists (SELECT 1 FROM work.dlc b WHERE a.factset_entity_id = b.dlc_entity_id);

--Checked no duplicate

select count(*) from work.own_basic;

ALTER TABLE work.own_basic
ADD COLUMN entity_proper_name TEXT;

UPDATE work.own_basic a
SET entity_proper_name=b.entity_proper_name
FROM  factset.edm_standard_entity b
WHERE a.factset_entity_id=b.factset_entity_id;

DELETE FROM work.own_basic where entity_proper_name is null;

select count(*) from work.own_basic;  --This is the number on SAS cloud

/*------------------------------------*/
/*#2. Price and market cap procedures*/
/*-----------------------------------*/
CREATE TABLE work.prices_historical AS
SELECT
    a.fsym_id,
    (EXTRACT(YEAR FROM a.price_date) * 100 + EXTRACT(MONTH FROM a.price_date))::int AS month,
    (EXTRACT(YEAR FROM a.price_date) * 100 + EXTRACT(QUARTER FROM a.price_date))::int AS quarter,
    CASE
        WHEN EXTRACT(MONTH FROM a.price_date) IN (3, 6, 9, 12) THEN 1
        ELSE 0
    END AS eoq,
    a.adj_shares_outstanding,
    a.adj_price,
    (a.adj_price * a.adj_shares_outstanding / 1000000) AS own_mktcap
FROM
    factset.own_sec_prices_eq a
JOIN
    (SELECT
        fsym_id,
        EXTRACT(YEAR FROM price_date) * 100 + EXTRACT(MONTH FROM price_date)::int AS month,
        MAX(price_date) AS maxdate
     FROM
        factset.own_sec_prices_eq
     GROUP BY
        fsym_id, EXTRACT(YEAR FROM price_date), EXTRACT(MONTH FROM price_date)
    ) b ON a.fsym_id = b.fsym_id AND a.price_date = b.maxdate
ORDER BY
    a.fsym_id, month;

--21218612
select count(*) from work.prices_historical;

CREATE TABLE work.sec_mktcap AS
SELECT
    fsym_id,
    quarter,
    own_mktcap,
    adj_price,
    adj_shares_outstanding
FROM
    work.prices_historical
WHERE
    -- Assuming "month" has a numerical part that ends in the months of interest; otherwise, you'll need to adjust this logic to fit your data model.
    MOD(month::int , 100) IN (3,6,9,12)
    AND own_mktcap IS NOT NULL;

SELECT COUNT(*) from work.sec_mktcap;


/*---------------------------------*/
/* #3 Start merging ownership data */
/*---------------------------------*/

DO $$
DECLARE
    sqtr INTEGER := 200001; -- Starting quarter
    eqtr INTEGER; -- Variable to hold the calculated ending quarter
BEGIN
    -- Calculate the ending quarter based on the maximum report_date
    SELECT INTO eqtr
        (DATE_PART('year', max(report_date)) * 100 + DATE_PART('quarter', max(report_date)))::int
    FROM
        factset.own_inst_13f_detail_eq;

    -- Use the calculated sqtr and eqtr to filter and select necessary data into a new table
    CREATE TABLE work.own_inst_13f_detail AS
    SELECT
        factset_entity_id,
        fsym_id,
        report_date,
        (DATE_PART('year', report_date) * 100 + DATE_PART('quarter', report_date))::int AS quarter,
        adj_holding
    FROM
        factset.own_inst_13f_detail_eq
    WHERE
        (DATE_PART('year', report_date) * 100 + DATE_PART('quarter', report_date))::int BETWEEN sqtr AND eqtr;
END $$;

SELECT count(*) FROM work.own_inst_13f_detail;

/*last report date each quarter*/

CREATE TABLE work.max13f AS
SELECT  factset_entity_id,
		quarter,
		max(report_date) as maxofdlr
FROM work.own_inst_13f_detail
GROUP BY factset_entity_id, quarter;

/*last report each qtr*/
CREATE TABLE work.aux13f AS
SELECT b.*, (DATE_PART('year', b.report_date)*100+DATE_PART('month', b.report_date))::int AS month
FROM  work.max13f a, work.own_inst_13f_detail b
WHERE a.factset_entity_id = b.factset_entity_id
AND   a.maxofdlr = b.report_date;

--106598632

select count(*) from work.aux13f;


CREATE TABLE work.v1_holdings13f AS
SELECT t1.factset_entity_id, t1.fsym_ID, t1.quarter, t1.adj_holding, t3.adj_price, t3.adj_shares_outstanding,
t1.adj_holding / NULLIF(t3.adj_shares_outstanding, 0) AS io,
t3.own_mktcap AS sec_mv,
t1.adj_holding*t3.adj_price/1000000 AS dollarholding  /*for portfolio weight and identifying global institutions*/
FROM work.aux13f t1, work.own_basic t2, work.prices_historical t3
WHERE t1.fsym_ID = t2.fsym_ID
AND t1.fsym_ID = t3.fsym_ID
AND t1.month = t3.month;

SELECT COUNT(*) FROM work.v1_holdings13f;

SELECT COUNT(*) FROM work.v1_holdings13f where adj_shares_outstanding =0;

DELETE FROM work.v1_holdings13f
WHERE factset_entity_id in ('0FSVG4-E','000V4B-E')
OR dollarholding > sec_mv;

--So far same as on Cedar


