/*Aggregates institution-security level ownership to security-level and firm-level*/

/*Security-level Ownership*/

CREATE TEMP TABLE v3_holdingsall AS
SELECT t1.fsym_id,
t1.company_id,
t1.quarter, t1.factset_entity_id,
t1.sec_country,
t1.inst_country,
		CASE
		  WHEN t1.sec_country = t1.inst_country THEN 1
		   ELSE 0
		END AS is_dom,
		isglobal,
		CASE /*indicator for domestic country fund in security country*/
		  WHEN iscountry=1
		  AND t1.sec_country=t2.sec_country
		  AND t1.sec_country = t1.inst_country THEN 1
		  ELSE 0 END AS  is_ctry ,
		t1.io  /*already adjusted*/
/* 	t1.dollarholding as dollarholding */
FROM  work.v2_holdingsall_sec t1,
work.inst_isglobal t2
WHERE
t1.quarter = t2.quarter
AND t1.factset_entity_id=t2.factset_entity_id
ORDER BY t1.fsym_id, t1.quarter, io DESC;

CREATE TABLE work.holdings_by_security1 AS
SELECT 	a.fsym_id,
        a.company_id,
		a.quarter,
       (DATE_TRUNC('QUARTER', TO_DATE(a.quarter::text, 'YYYYQQ')) + INTERVAL '3 MONTH' - INTERVAL '1 day')::DATE AS quarterdate,
    sec_country,
		count(*) as nbr_firms,
		sum(io) as io,
	 	sum(io*is_dom) as io_dom,
		sum(io*(1-is_dom)) as io_for,
		sum(io*isglobal) as io_global,
		sum(io*is_ctry) as io_ctry, /*domestic country fund that focus on the security country*/
		own_mktcap

FROM v3_holdingsall a, work.sec_mktcap b
WHERE a.fsym_id=b.fsym_id
AND a.quarter=b.quarter
GROUP BY a.fsym_id,company_id, a.quarter, sec_country,own_mktcap;


CREATE TABLE work.holdings_by_securities AS
SELECT a.*
FROM work.holdings_by_security1 a, factset.edm_standard_entity b, ctry c
WHERE a.company_id=b.factset_entity_id
AND a.sec_country=c.iso
AND b.primary_sic_code != '6798';

/*Firm-level ownership*/
DROP TABLE IF EXISTS v3_holdingsall;

CREATE TABLE work.v3_holdingsall AS
SELECT  t1.company_id, t1.quarter, t1.factset_entity_id, t1.sec_country, t1.inst_country,
		CASE
		   WHEN t1.sec_country = t1.inst_country THEN 1
		   ELSE 0
		END AS is_dom,
		CASE
		   WHEN t1.inst_origin = 'US' THEN 1
		   ELSE 0
		END AS is_us_inst,
		CASE
		   WHEN t1.inst_origin = 'UK' THEN 1
		   ELSE 0
		END AS is_uk_inst,
		CASE
		   WHEN t1.inst_origin = 'Europe' THEN 1
		   ELSE 0
		END AS is_eu_inst,
		CASE
		   WHEN inst_country IN ('FR','DE','NL') THEN 1
		   ELSE 0
		END AS is_euro_inst,
		CASE
		   THEN t1.inst_origin = 'Others' THEN 1
		   ELSE 0
		END AS is_others_inst,
		CASE
		   WHEN cat_institution=1 THEN 1
		   ELSE 0
		END AS is_br,
		CASE
		   WHEN cat_institution=2 THEN 1
		   ELSE 0
		END AS is_pb,
		CASE
		   WHEN cat_institution=3 THEN 1
		   ELSE 0
		END AS is_hf,
		CASE
		   WHEN cat_institution=4 THEN 1
		   ELSE 0
		END AS is_ia,
		CASE
		   WHEN cat_institution=5 THEN 1
		   ELSE 0
		END AS is_lt,
		t1.io_unadj,
		t1.adjf,
		t1.io

FROM work.v2_holdingsall_firm  t1
ORDER BY t1.company_id, t1.quarter, io DESC;


CREATE TABLE work.holdings_by_firm1 AS
SELECT 	company_id,
		quarter,
		sec_country,
		count(*) AS nbr_firms,
		/*FM aggregation*/
		/*dometsic io*/
		sum(io) AS io,
		sum(io*is_dom) AS io_dom,
		sum(io*(1-is_dom)) AS io_for,

        /*us*/
        sum(io*is_us_inst) AS io_us,
        sum(io*is_us_inst*(1-is_dom)) AS io_for_us,

		/*uk*/
		sum(io*is_uk_inst) AS io_uk,
		sum(io*is_uk_inst*(1-is_dom)) AS io_for_uk,

		/*eu*/
		sum(io*is_eu_inst) AS io_eu,
		sum(io*is_eu_inst*(1-is_dom)) AS io_for_eu,

	    /*euro*/
	    sum(io*is_euro_inst) AS io_euro,
		sum(io*is_euro_inst*(1-is_dom)) AS io_for_euro,

	    /*foreign others*/
		sum(io*is_others_inst) AS io_others,
		sum(io*is_others_inst*(1-is_dom)) AS io_for_others,

		/*broker*/
		sum(io*is_br) AS io_br,

	    /*private banking*/
		sum(io*is_pb) AS io_pb,

		/*hedge fund*/
		sum(io*is_hf) AS io_hf,

		/*investment advisor*/
        sum(io*is_ia) AS io_ia,

        /*long-term*/
        sum(io*is_lt) AS io_lt

FROM v3_holdingsall
GROUP BY company_id, quarter, sec_country;

/*merge mktcap*/

CREATE TABLE work.holdings_by_firm2 AS
SELECT a.company_id, a.quarter, sec_country, nbr_firms, io, io_dom, io_for, io_us, io_for_us, io_uk, io_for_uk, io_eu, io_for_eu, io_euro, io_for_euro, io_others, io_for_others, io_br, io_pb, io_hf, io_ia, io_lt,
       c.entity_proper_name,
		b.mktcap_usd AS mktcap
FROM work.holdings_by_firm1 a, work.hmktcap b, factset.edm_standard_entity c
WHERE b.eoq = 1 AND a.company_id = b.factset_entity_id
AND a.quarter = b.quarter
AND a.company_id = c.factset_entity_id;

/*Remove FEIT*/
CREATE TABLE work.holdings_by_firm_all AS
SELECT a.company_id as factset_entity_id, a.quarter,
    (DATE_TRUNC('quarter', TO_DATE(a.quarter::text, 'YYYYQQ')) + INTERVAL '3 month' + INTERVAL '1 day' - INTERVAL '1 day')::date AS rquarter,
    sec_country, entity_proper_name, nbr_firms, io, io_dom, io_for, io_us, io_for_us, io_uk, io_for_uk, io_eu, io_for_eu, io_euro, io_for_euro, io_others, io_for_others, io_br, io_pb, io_hf, io_ia, io_lt, mktcap
FROM
    work.holdings_by_firm2 a
WHERE
    a.company_id NOT IN (
        SELECT factset_entity_id FROM factset.edm_standard_entity WHERE primary_sic_code = '6798'
    )
ORDER BY
    a.company_id,
    a.quarter;


CREATE TABLE work.holdings_by_firm_ftse AS
SELECT b.*
FROM work.ctry a, work.holdings_by_firm_all b
WHERE a.iso = b.sec_country;

/*Annual firm-level ownership*/
/*Keep last quarter of each year*/
CREATE TABLE work.holdings_by_firm_annual AS
WITH MaxQuarter AS (
    SELECT
        a.factset_entity_id,
        (a.quarter / 100)::int AS year, -- Calculated year
        MAX(a.quarter) AS maxqtr -- Maximum quarter for each year and factset_entity_id
    FROM
        work.holdings_by_firm_ftse a
    GROUP BY
        a.factset_entity_id,
        (a.quarter / 100)::int
)

    SELECT
    a.*,
    CASE
        WHEN b.iso = 'US' THEN 'US'
        ELSE b.isem
    END AS market,
    (a.quarter / 100)::int AS year,
    mq.maxqtr
FROM
    work.holdings_by_firm_ftse a
JOIN
    ctry b ON a.sec_country = b.iso
JOIN
    MaxQuarter mq ON a.factset_entity_id = mq.factset_entity_id AND (a.quarter / 100)::int = mq.year
WHERE
    a.quarter = mq.maxqtr -- keep max quarter
ORDER BY
    a.factset_entity_id, mq.year, mq.maxqtr;


