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

from work.v2_holdingsall_firm  t1
order by t1.company_id, t1.quarter, io desc;


