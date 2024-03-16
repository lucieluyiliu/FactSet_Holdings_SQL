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


--7187379
SELECT COUNT(*) from work.sec_mktcap;

/*Firm-level market capitalization*/

/*11,433,354*/
CREATE TABLE work.own_mktcap1 AS
SELECT a.*,
b.factset_entity_id, b.issue_type, b.fref_security_type,
a.adj_price * a.adj_shares_outstanding/1000000 AS own_mv, islocal
FROM factset.own_sec_prices_eq a, work.own_basic b
WHERE a.fsym_ID = b.fsym_ID
AND b.issue_type != 'AD';

-- exclude unilever ADR classified as "EQ";
DELETE
FROM work.own_mktcap1
WHERE fsym_id='DXVFL5-S' and price_date >='2015-09-30';

--11433307
SELECT count(*) from work.own_mktcap1;


/*Company level monthly MV*/
--10,423,303
CREATE TABLE work.hmktcap as
SELECT factset_entity_id,
DATE_PART('year', price_date)*100+DATE_PART('month', price_date) AS month,
DATE_PART('year', price_date)*100+DATE_PART('quarter', price_date) AS quarter,
CASE
	WHEN DATE_PART('month', price_date) IN (3,6,9,12) THEN 1
	ELSE 0
END AS eoq,
sum(own_mv) AS mktcap_usd
FROM work.own_mktcap1
WHERE factset_entity_id IS NOT NULL
GROUP BY factset_entity_id, price_date, month, quarter, eoq
HAVING sum(own_mv) IS NOT NULL and sum(own_mv) > 0;

SELECT COUNT(*) from work.hmktcap;



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

select count(*) from work.max13f;

/*last report each qtr*/
CREATE TABLE work.aux13f AS
SELECT b.*, (DATE_PART('year', b.report_date)*100+DATE_PART('month', b.report_date))::int AS month
FROM  work.max13f a, work.own_inst_13f_detail b
WHERE a.factset_entity_id = b.factset_entity_id
AND   a.maxofdlr = b.report_date;

--106598632

select count(*) from work.aux13f;

--91, 983, 266 (same as SAS)

CREATE TABLE work.v0_holdings13f AS
SELECT t1.factset_entity_id, t1.fsym_ID, t1.quarter, t1.adj_holding, t3.adj_price, t3.adj_shares_outstanding,
t1.adj_holding / NULLIF(t3.adj_shares_outstanding, 0) AS io_sec,
(t1.adj_holding*t3.adj_price/1000000)/NULLIF(t4.mktcap_usd,0) AS io_firm,
t3.own_mktcap AS sec_mv,
t1.adj_holding*t3.adj_price/1000000 AS dollarholding  /*for portfolio weight and identifying global institutions*/
FROM work.aux13f t1, work.own_basic t2, work.prices_historical t3, work.hmktcap t4
WHERE t1.fsym_ID = t2.fsym_ID
AND t1.fsym_ID = t3.fsym_ID
AND t1.month = t3.month
AND t2.factset_entity_id=t4.factset_entity_id
AND t1.month=t4.month;

SELECT COUNT(*) FROM work.v0_holdings13f;  --91993400

SELECT COUNT(*) FROM work.v0_holdings13f where adj_shares_outstanding =0;

--91944666, same as SAS
DELETE FROM work.v0_holdings13f
WHERE factset_entity_id in ('0FSVG4-E','000V4B-E')
OR dollarholding > sec_mv;

--So far same as on Cedar

/*mutual funds*/

DO $$
DECLARE
    sqtr INTEGER := 200001; -- Starting quarter
    eqtr INTEGER := 202304; -- Variable to hold the calculated ending quarter
BEGIN

    -- Use the calculated sqtr and eqtr to filter and select necessary data into a new table
    CREATE TABLE work.own_fund_detail AS
    SELECT
        factset_fund_id,
        fsym_id,
        report_date,
        (DATE_PART('year', report_date) * 100 + DATE_PART('quarter', report_date))::int AS quarter,
        adj_holding
    FROM
        factset.own_fund_detail_eq
    WHERE
        (DATE_PART('year', report_date) * 100 + DATE_PART('quarter', report_date))::int BETWEEN sqtr AND eqtr;
END $$;

SELECT count(*) FROM factset.own_fund_detail_eq;


--467944750, same as on WRDS


--2558868
CREATE TABLE work.maxmf AS
SELECT  factset_fund_id,
		quarter,
		max(report_date) as maxofdlr
FROM work.own_fund_detail
GROUP BY  factset_fund_id, quarter;

select count(*) from work.maxmf;


CREATE TABLE work.auxmf as
SELECT b.*, DATE_PART('year', b.report_date)*100+DATE_PART('month', b.report_date)::int as month
FROM  work.maxmf a, work.own_fund_detail b
WHERE a.factset_fund_id = b.factset_fund_id
AND   a.maxofdlr = b.report_date;

select count(*) from work.auxmf;

--242085205 same as WRDS
CREATE TABLE work.v0_holdingsmf AS
SELECT t1.factset_fund_id, t1.fsym_ID, t1.quarter, t1.adj_holding, t3.adj_price, t3.adj_shares_outstanding,
t1.adj_holding / NULLIF(t3.adj_shares_outstanding, 0) AS io_sec,
(t1.adj_holding*t3.adj_price/1000000)/NULLIF(t4.mktcap_usd,0) AS io_firm,
t3.own_mktcap AS sec_mv,
t1.adj_holding*t3.adj_price/1000000 AS dollarholding  /*keep it in case need it for portfolio weight*/
/*2023-06-25: decided to use rolled over io and market cap for portfolio instead*/
FROM work.auxmf t1, work.own_basic t2, work.prices_historical t3, work.hmktcap t4
WHERE t1.fsym_ID = t2.fsym_ID
AND t1.fsym_ID = t3.fsym_ID
AND t1.month = t3.month
AND t2.factset_entity_id=t4.factset_entity_id
AND t1.month=t4.month;

SELECT count(*) from work.v0_holdingsmf
where (factset_fund_id !='04B9J7-E'
or fsym_id!='C7R70B-S')
and dollarholding is not null
and sec_mv is not null
and dollarholding<=sec_mv;

DELETE FROM work.v0_holdingsmf
WHERE factset_fund_id ='04B9J7-E'
AND fsym_id='C7R70B-S';

DELETE FROM work.v0_holdingsmf
WHERE  dollarholding is null
OR sec_mv is null;

DELETE FROM work.v0_holdingsmf
WHERE  dollarholding > sec_mv;

/*241942779, same as SAS*/
SELECT count(*) from work.v0_holdingsmf;

/*Rolling forward past report*/

CREATE TABLE work.sym_range as
SELECT fsym_ID, DATE_PART('year', termination_date)*100+DATE_PART('quarter', termination_date) as maxofqtr
FROM work.own_basic;

CREATE TABLE work.rangeofquarters AS
SELECT DISTINCT quarter
FROM work.own_inst_13f_detail
ORDER BY quarter;


--13F
CREATE TABLE work.inst_13f AS
SELECT DISTINCT factset_entity_id
FROM work.own_inst_13f_detail
ORDER BY factset_entity_id;



CREATE TABLE work.insts_13fdates as
SELECT factset_entity_id, quarter
FROM work.inst_13f, work.rangeofquarters
ORDER BY factset_entity_id, quarter;



CREATE TABLE work.pairs_13f AS
SELECT DISTINCT factset_entity_id, quarter, 1
AS has_report
FROM work.own_inst_13f_detail
ORDER BY factset_entity_id, quarter;


CREATE TABLE work.entity_minmax AS
SELECT factset_entity_id, min(quarter)
AS min_quarter, max(quarter) AS max_quarter
FROM work.own_inst_13f_detail
GROUP BY factset_entity_id;



CREATE TABLE work.roll113f AS
	SELECT a.*,
		   CASE
		   	  WHEN b.has_report IS NULL THEN 0
			  ELSE b.has_report
		   END AS has_report,
		   c.min_quarter,
		   c.max_quarter AS max_quarter_raw,
		   CASE
		   	  WHEN c.max_quarter >= quarter_add(202304,-3) THEN 202304
			  ELSE c.max_quarter
		   END AS max_quarter
	FROM work.insts_13fdates a
	LEFT JOIN work.pairs_13f b ON a.factset_entity_id = b.factset_entity_id AND a.quarter = b.quarter
	INNER JOIN work.entity_minmax c ON a.factset_entity_id = c.factset_entity_id
    ORDER BY factset_entity_id, quarter;


    DELETE FROM work.roll113f
    WHERE quarter < min_quarter
    OR quarter > max_quarter;

select count(*) from work.roll113f;

CREATE TABLE work.roll13f AS
SELECT a.*,
       b.quarter AS last_qtr,
       (CAST(a.quarter / 100 AS INTEGER) - CAST(b.quarter / 100 AS INTEGER)) * 4 + MOD(a.quarter, 100) - MOD(b.quarter, 100) AS dif_quarters,
       CASE
           WHEN (CAST(a.quarter / 100 AS INTEGER) - CAST(b.quarter / 100 AS INTEGER)) * 4 + MOD(a.quarter, 100) - MOD(b.quarter, 100) <= 7 THEN 1
           ELSE 0
       END AS valid
FROM work.roll113f a
JOIN work.pairs_13f b ON a.factset_entity_id = b.factset_entity_id AND b.quarter <= a.quarter
ORDER BY a.factset_entity_id, a.quarter, b.quarter DESC;


CREATE TABLE work.roll13f_sorted AS
SELECT DISTINCT ON (factset_entity_id, quarter) *
FROM work.roll13f;


SELECT count(*) FROM
(SELECT DISTINCT factset_entity_id, quarter FROM work.roll13f) AS a;

CREATE TABLE work.fill_13f AS
SELECT *
FROM work.roll13f_sorted
WHERE has_report = 0 AND valid = 1;

CREATE TABLE work.inserts_13f AS
SELECT b.factset_entity_id,
       a.quarter,
       b.fsym_id,
       b.adj_holding,
       b.io_sec,
       b.io_firm
FROM work.fill_13f a
JOIN work.v0_holdings13f b ON a.factset_entity_id = b.factset_entity_id AND a.last_qtr = b.quarter
JOIN work.sym_range c ON b.fsym_id = c.fsym_id AND a.quarter < c.maxofqtr;

CREATE TABLE work.v1_holdings13f AS
SELECT factset_entity_id, fsym_ID, quarter, io_sec, io_firm, adj_holding
FROM work.v0_holdings13f
UNION ALL
SELECT factset_entity_id, fsym_ID, quarter, io_sec, io_firm, adj_holding
FROM work.inserts_13f;

ALTER TABLE work.v1_holdings13f
ADD COLUMN dollarholding numeric;

ALTER TABLE work.v1_holdings13f
ADD COLUMN adj_price numeric;


UPDATE work.v1_holdings13f a
SET dollarholding=io_sec*b.own_mktcap,  /*implicit assumption: security io does not change for the rolledup holdings*/
adj_price=b.adj_price
/* 	calculated valueholding-calculated dollarholding as diff */
FROM work.sec_mktcap b
WHERE a.quarter=b.quarter
AND a.fsym_id=b.fsym_id;

DELETE FROM work.v1_holdings13f
WHERE dollarholding IS NULL OR dollarholding=0;



CREATE TABLE work.v2_holdings13f AS
	SELECT t2.factset_rollup_entity_id AS factset_entity_id, t1.fsym_id, t1.quarter, adj_price,
	sum(t1.io_sec) as io_sec,
	sum(t1.io_firm) as io_firm,
	sum(dollarholding) as dollarholding,
	sum(adj_holding) as adj_holding
	from work.v1_holdings13f t1,
		  factset.own_ent_13f_combined_inst t2
	where t1.factset_entity_id = t2.factset_filer_entity_id
	group by t2.factset_rollup_entity_id, t1.fsym_id, t1.quarter, adj_price;

/*Mutual funds rolling forward*/

CREATE TABLE work.insts_mf as
SELECT DISTINCT factset_fund_id
FROM work.own_fund_detail
ORDER BY factset_fund_id;

CREATE TABLE work.insts_mfdates AS
SELECT DISTINCT
factset_fund_id, quarter
FROM work.insts_mf, work.rangeofquarters
ORDER BY factset_fund_id, quarter;

CREATE TABLE work.pairs_mf AS
SELECT DISTINCT factset_fund_id, quarter, 1 AS has_report
FROM work.own_fund_detail
ORDER BY factset_fund_id, quarter;

CREATE TABLE work.fund_minmax AS
SELECT factset_fund_id, min(quarter) AS min_quarter, max(quarter) AS max_quarter
FROM work.own_fund_detail
GROUP BY factset_fund_id;

CREATE TABLE work.roll1mf AS
SELECT a.*,
	   CASE
	   	  WHEN b.has_report IS NULL THEN 0
		  ELSE b.has_report
	   END AS has_report,
	   c.min_quarter,
	   c.max_quarter AS max_quarter_raw,
	   CASE
	   	  WHEN c.max_quarter >= quarter_add(202304,-3) THEN 202304
		  ELSE c.max_quarter
	   END AS max_quarter
FROM work.insts_mfdates a
LEFT JOIN work.pairs_mf b ON a.factset_fund_id = b.factset_fund_id
AND a.quarter = b.quarter
INNER JOIN work.fund_minmax c ON a.factset_fund_id = c.factset_fund_id;


DELETE FROM work.roll1mf
WHERE quarter < min_quarter
OR quarter > max_quarter;

SELECT count(*) FROM work.roll1mf;


CREATE TABLE work.rollmf AS
SELECT a.*,
       b.quarter AS last_qtr,
       (CAST(a.quarter / 100 AS INTEGER) - CAST(b.quarter / 100 AS INTEGER)) * 4 + MOD(a.quarter, 100) - MOD(b.quarter, 100) AS dif_quarters,
       CASE
           WHEN (CAST(a.quarter / 100 AS INTEGER) - CAST(b.quarter / 100 AS INTEGER)) * 4 + MOD(a.quarter, 100) - MOD(b.quarter, 100) <= 7 THEN 1
           ELSE 0
       END AS valid
FROM work.roll1mf a, work.pairs_mf b
WHERE a.factset_fund_id = b.factset_fund_id AND b.quarter <= a.quarter
ORDER BY a.factset_fund_id, a.quarter, b.quarter DESC;

CREATE TABLE work.rollmf_sorted AS
SELECT DISTINCT ON (factset_fund_id, quarter) *
FROM work.rollmf;

select count(*) from work.rollmf_sorted;

select count(*) from work.rollmf_sorted WHERE has_report=0 AND valid=1;

CREATE TABLE work.fill_mf AS
SELECT *
FROM work.rollmf_sorted
WHERE has_report = 0 AND valid = 1;

CREATE TABLE work.inserts_mf AS
SELECT b.factset_fund_id, a.quarter, b.fsym_id,  b.adj_holding ,  b.io_sec, io_firm
FROM work.fill_mf a, work.v0_holdingsmf b, work.sym_range c
where a.factset_fund_id = b.factset_fund_id and a.last_qtr = b.quarter
and b.fsym_id = c.fsym_id and a.quarter < c.maxofqtr;


CREATE TABLE work.v1_holdingsmf AS
SELECT factset_fund_id, fsym_ID, quarter, io_sec, io_firm, adj_holding FROM work.v0_holdingsmf
UNION ALL
SELECT factset_fund_id, fsym_ID, quarter, io_sec, io_firm, adj_holding FROM work.inserts_mf;


ALTER TABLE work.v1_holdingsmf
ADD COLUMN dollarholding numeric;

ALTER TABLE work.v1_holdingsmf
ADD COLUMN adj_price numeric;


UPDATE work.v1_holdingsmf a
SET dollarholding=io_sec*b.own_mktcap,  /*implicit assumption: security io does not change for the rolledup holdings*/
adj_price=b.adj_price
/* 	calculated valueholding-calculated dollarholding as diff */
FROM work.sec_mktcap b
WHERE a.quarter=b.quarter
AND a.fsym_id=b.fsym_id;

VACUUM ANALYZE work.v1_holdingsmf;

SELECT count(*) FROM work.v1_holdingsmf WHERE dollarholding is not NULL and dollarholding !=0;

DELETE FROM work.v1_holdingsmf
WHERE dollarholding IS NULL OR dollarholding=0;

CREATE TABLE work.v2_holdingsmf as
	select factset_inst_entity_id as factset_entity_id, fsym_ID, quarter, adj_price,
	sum(adj_holding) as adj_holding,
	sum(io_sec) as io_sec,
	sum(io_firm) as io_firm,
	sum(dollarholding) as dollarholding
	from work.v1_holdingsmf t1,
			factset.own_ent_funds t2
	where t1.factset_fund_id = t2.factset_fund_id
	group by factset_entity_id, fsym_ID, quarter, adj_price;

/*DELETE NULL AND ZERO HOLDINGS*/

DELETE FROM work.v2_holdings13f
WHERE dollarholding is null or dollarholding=0;

DELETE FROM work.v2_holdingsmf
WHERE dollarholding is null or dollarholding=0;


/*Combine 13F and fund reports*/

CREATE TABLE work.inst_quarter_mf AS
SELECT DISTINCT factset_entity_id, quarter FROM work.v2_holdingsmf;

CREATE TABLE work.inst_quarter_13f AS
SELECT DISTINCT factset_entity_id, quarter FROM work.v2_holdings13f;

CREATE TABLE work.inst_quarter_mf_only AS
SELECT a.factset_entity_id, a.quarter
FROM work.inst_quarter_mf a
LEFT JOIN work.inst_quarter_13f b ON (a.factset_entity_id = b.factset_entity_id AND a.quarter = b.quarter)
WHERE b.factset_entity_id IS NULL AND b.quarter IS NULL;

CREATE TABLE work.inst_quarter_13f_only as
SELECT a.factset_entity_id, a.quarter
FROM work.inst_quarter_13f a
LEFT JOIN work.inst_quarter_mf b ON (a.factset_entity_id = b.factset_entity_id AND a.quarter = b.quarter)
WHERE b.factset_entity_id IS NULL AND b.quarter IS NULL;

CREATE TABLE work.inst_quarter_both AS
SELECT a.factset_entity_id, a.quarter
FROM work.inst_quarter_mf a, work.inst_quarter_13f b
WHERE a.factset_entity_id = b.factset_entity_id AND a.quarter = b.quarter;

CREATE TABLE work.v1_holdingsall AS
SELECT factset_entity_id, fsym_id, quarter, max(io_sec) as io_sec, max(io_firm) as io_firm, max(dollarholding) as dollarholding,
max(adj_holding) as adj_holding,
adj_price

FROM (
	SELECT factset_entity_id, fsym_id, quarter, io_sec,io_firm, dollarholding, adj_holding, adj_price
	FROM work.v2_holdings13f

	UNION ALL

	SELECT b.factset_entity_id, b.fsym_id, b.quarter, b.io_sec, b.io_firm, b.dollarholding, b.adj_holding, adj_price
	FROM work.inst_quarter_mf_only a, work.v2_holdingsmf b
	WHERE a.factset_entity_id = b.factset_entity_id
	AND a.quarter = b.quarter

	UNION ALL

	SELECT c.factset_entity_id, c.fsym_id, c.quarter, c.io_sec,c.io_firm, c.dollarholding, c.adj_holding, adj_price

	FROM work.inst_quarter_both a, work.own_basic b, work.v2_holdingsmf c
	WHERE b.iso_country != 'US' AND a.factset_entity_id = c.factset_entity_id
	AND a.quarter = c.quarter AND b.fsym_id = c.fsym_id
		) t1
GROUP BY factset_entity_id, fsym_id, quarter, adj_price;

/*Adjustment factors of security-level and firm-level IO such that aggregate IO le 1*/

/*security-level*/
CREATE TABLE work.adjfactor_sec AS
SELECT fsym_id, quarter, sum(io_sec) AS io_sec, GREATEST(SUM(io_sec), 1) AS adjf
FROM work.v1_holdingsall
GROUP BY fsym_id, quarter;

/*firm-level*/
CREATE TABLE work.adjfactor_firm AS
SELECT b.factset_entity_id AS company_id, quarter, sum(io_firm) as io_firm,  GREATEST(sum(io_firm), 1) AS adjf
FROM work.v1_holdingsall a, work.own_basic b
WHERE a.fsym_id=b.fsym_id
GROUP BY company_id, quarter;

/*Make adjustment to security-level ownership, add firm-information and factset market cap*/

CREATE TABLE work.v2_holdingsall_sec AS
SELECT
a.factset_entity_id, a.fsym_id,
d.factset_entity_id AS company_id,
a.quarter,
e.iso_country AS inst_country,
f.iso_country AS sec_country,
e.entity_sub_type,
 a.io_sec AS io_unadj,
 a.io_sec/adjf AS io,
 adj_holding/adjf AS adj_holding,
 dollarholding/adjf AS dollarholding
FROM work.v1_holdingsall a, work.adjfactor_sec b, work.sec_mktcap c, work.own_basic d,
factset.edm_standard_entity e, factset.edm_standard_entity f
WHERE a.fsym_id=b.fsym_id
AND a.quarter=b.quarter
AND a.fsym_id=c.fsym_id
AND a.quarter=c.quarter
AND a.fsym_id=d.fsym_id
AND a.factset_entity_id=e.factset_entity_id
AND d.factset_entity_id=f.factset_entity_id
AND a.io_sec IS NOT NULL
AND own_mktcap IS NOT NULL
AND own_mktcap != 0
AND d.factset_entity_id IS NOT NULL;

CREATE TABLE work.v1_holdingsall_firm AS
SELECT  a.factset_entity_id, b.factset_entity_id AS company_id, a.quarter,
		c.iso_country AS inst_country, d.iso_country AS sec_country, c.entity_sub_type,
		sum(a.io_firm) AS io, sum(dollarholding) AS dollarholding,
        cat_institution,
		CASE
	      WHEN c.iso_country = 'US' THEN 'US'
		  WHEN c.iso_country = 'GB' THEN 'UK'
		  WHEN f.region LIKE '%Europe%' AND d.iso_country != 'UK' THEN 'Europe'
		  ELSE 'Others'
    END AS inst_origin
FROM work.v1_holdingsall a, work.own_basic b,
 factset.edm_standard_entity c,
 factset.edm_standard_entity d,
 work.inst_type e,
 work.ctry f
WHERE a.fsym_ID = b.fsym_ID
AND   a.factset_entity_id = c.factset_entity_id
AND   b.factset_entity_id = d.factset_entity_id
AND   b.factset_entity_id IS NOT NULL
AND a.io_firm IS NOT NULL
AND c.entity_sub_type=e.entity_sub_type
AND
AND c.iso_country=f.iso
GROUP BY a.factset_entity_id, b.factset_entity_id,
a.quarter, c.iso_country, d.iso_country, c.entity_sub_type, cat_institution, inst_origin;

/*Adjustment*/


/*Apply firm-level adjustment factor */

CREATE TABLE work.v2_holdingsall_firm AS
SELECT a.factset_entity_id, a.company_id, a.quarter,a.inst_country, a.sec_country,
a.entity_sub_type,
a.io AS io_unadj,
adjf,
a.io/adjf as io,
a.dollarholding/adjf AS dollarholding, a.cat_institution, a.inst_origin
FROM work.v1_holdingsall_firm a, work.adjfactor_firm b
WHERE a.company_id=b.company_id
AND a.quarter=b.quarter;


CREATE TABLE work.principal_security AS
SELECT a.*
FROM factset.sym_coverage a
LEFT JOIN factset.own_sec_entity_eq b
ON a.fsym_id = b.fsym_id
WHERE b.factset_entity_id IN (SELECT DISTINCT company_id FROM work.v2_holdingsall_firm)
AND b.factset_entity_id IS NOT NULL
AND a.fsym_id = a.fsym_primary_equity_id
ORDER BY b.factset_entity_id;

