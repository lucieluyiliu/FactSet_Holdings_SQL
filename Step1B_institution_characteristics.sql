/*This script calcualtes institution-level portfolio characteristics*/
/*Variables include */
    --1. Total and subportfolio AUM by investment destination, total and sub number of securities/firms by investment destination
    --2. Country, region, global institution labels
    --3. Home bias and country bias
    --4. Portfolio HHI
    --5. Active share
    --6. Institution-security level portfolio concentration
    --7. Churn ratio
    --8. Institution-security level investment horizon

/*2024-03-17*/
/*Author: Lucie Lu, lucie.lu@unimelb.edu.au*/


--#0. Augment holdings table with institution origin

CREATE TABLE work.v1_holdingsall_aug AS
SELECT a.quarter, a.factset_entity_id, a.fsym_id, dollarholding,
adj_holding, adj_price,
c.iso_country AS sec_country,
d.iso_country AS inst_country,
e.isem,
CASE WHEN d.iso_country='US' THEN 'US' /*These labels can change*/
WHEN d.iso_country='GB' THEN 'UK' /*if GB then UK*/
when f.region LIKE '%Europe%' then 'EU' /*Others go to EU*/
else 'OT' end as inst_origin,
case when c.iso_country=d.iso_country then 1 else 0 end as is_dom,
case when e.isem='DM' then 1 else 0 end as is_dm,
case when e.isem='EM' then 1 else 0 end as is_em,
case when e.isem='FM' then 1 else 0 end as is_fm
from work.v1_holdingsall a,
work.own_basic b,
factset.edm_standard_entity c,
factset.edm_standard_entity d,
work.ctry e, work.ctry f
where a.fsym_id=b.fsym_id
and b.factset_entity_id=c.factset_entity_id
and a.factset_entity_id=d.factset_entity_id
and c.iso_country=e.iso
and d.iso_country=f.iso;


/*#1A. AUM*/

CREATE TABLE work.inst_aum AS
SELECT factset_entity_id, quarter, inst_origin,
sum(dollarholding) AS AUM,
sum(dollarholding*is_dom) AS AUM_dom,
sum(dollarholding*(1-is_dom)*is_dm) AS AUM_dm,
sum(dollarholding*(1-is_dom)*is_em) AS AUM_em,
sum(dollarholding*(1-is_dom)*is_fm) AS AUM_fm,
sum(dollarholding*(1-is_dom)) AS AUM_for
FROM work.v1_holdingsall_aug
GROUP BY factset_entity_id, quarter, inst_origin;

/*#1B. number of securities*/
CREATE TABLE work.inst_nsecurities AS
SELECT factset_entity_id, quarter, inst_origin,
sum(1) AS n,
sum(is_dom) AS n_dom,
sum((1-is_dom)*is_dm) AS n_dm,
sum((1-is_dom)*is_em) AS n_em,
sum((1-is_dom)*is_fm) AS n_fm,
sum((1-is_dom)) AS n_for
FROM work.v1_holdingsall_aug
GROUP BY factset_entity_id, quarter, inst_origin;

/*#1C. Number of firms*/
CREATE TABLE work.inst_nfirms AS
SELECT a.factset_entity_id, quarter, inst_origin,
sum(1) AS n,
sum(is_dom) AS n_dom,
sum((1-is_dom)*is_dm) AS n_dm,
sum((1-is_dom)*is_em) AS n_em,
sum((1-is_dom)*is_fm) AS n_fm,
sum((1-is_dom)) AS n_for
FROM work.v1_holdingsall_aug a, work.principal_security b
WHERE a.fsym_id=b.fsym_id
GROUP BY a.factset_entity_id, quarter, inst_origin;

/*#2. A classify institutions into country, regional and global institutions a la Bartram (2015)*/

CREATE TABLE work.inst_country_weight AS
SELECT
    a.factset_entity_id,
    a.quarter,
    EXTRACT(YEAR FROM to_date(a.quarter::text, 'YYYYQQ')) AS year, -- Converted to PostgreSQL's date extraction method
    SUM(a.dollarholding) / b.aum AS ctry_weight, -- Assuming 'aum' comes from table 'b', adjusted division
    a.inst_country,
    a.sec_country,
    a.is_dom
FROM
    work.v1_holdingsall_aug a
JOIN
    work.inst_aum b ON a.factset_entity_id = b.factset_entity_id AND a.quarter = b.quarter
WHERE
    b.aum != 0 -- 'ne' is changed to '!=' for PostgreSQL
GROUP BY
    a.factset_entity_id,
    a.quarter,
    b.aum, -- Ensure this is correct as 'aum' is used in aggregation, might need to adjust based on logic
    a.inst_country,
    a.sec_country,
    a.is_dom;

CREATE TABLE work.inst_region_weight AS
SELECT a.factset_entity_id, a.quarter, sum(a.dollarholding)/aum AS region_weight, region
FROM work.v1_holdingsall_aug a, work.inst_aum b, work.ctry c
WHERE aum != 0
AND a.factset_entity_id=b.factset_entity_id
AND a.quarter=b.quarter
AND a.sec_country=c.iso
GROUP BY a.factset_entity_id, a.quarter, aum,  region;

select count(*) from work.inst_region_weight;

/*classify entity and funds by their scope, according to Bartram et al (2015)*/

CREATE TABLE work.ctryinst AS
SELECT
a.factset_entity_id, a.quarter,a.sec_country, a.ctry_weight, b.maxweight AS maxctryweight,
maxweight >= 0.9 AS iscountry
FROM work.inst_country_weight a,
(SELECT max(ctry_weight) AS maxweight, quarter,factset_entity_id
 FROM WORK.inst_country_weight
 GROUP BY factset_entity_id, quarter ) b
WHERE a.ctry_weight=b.maxweight
AND a.factset_entity_id=b.factset_entity_id
AND a.quarter=b.quarter;

CREATE TABLE work.regioninst AS
SELECT
a.factset_entity_id, a.quarter,a.region, b.maxweight AS maxregionweight,
b.maxweight >= 0.8 AND NOT iscountry AS isregion
from work.inst_region_weight a,
     (SELECT max(region_weight) AS maxweight, quarter,factset_entity_id
                                 FROM work.inst_region_weight GROUP BY factset_entity_id, quarter ) b,
work.ctryinst c
WHERE a.region_weight=b.maxweight
AND a.factset_entity_id=b.factset_entity_id
AND a.factset_entity_id=c.factset_entity_id
AND a.quarter=b.quarter
AND a.quarter=c.quarter;

/*A table that contains global country indicator, its maxim country allocation and its maximum region allocation*/

CREATE TABLE work.inst_isglobal AS
SELECT a.quarter, a.factset_entity_id, aum, sec_country,
CASE WHEN iscountry IS NOT NULL THEN iscountry ELSE false END AS iscountry,
maxctryweight
FROM work.inst_aum a
    LEFT JOIN work.ctryinst b
    ON (a.factset_entity_id=b.factset_entity_id AND a.quarter=b.quarter);

ALTER TABLE work.inst_isglobal
ADD isregion BOOLEAN,
ADD region text,
ADD maxregionweight numeric;

UPDATE work.inst_isglobal a
SET isregion=CASE WHEN b.isregion is not null then b.isregion else false end,
maxregionweight=b.maxregionweight,
region=b.region
FROM work.regioninst b
WHERE a.factset_entity_id=b.factset_entity_id
AND a.quarter=b.quarter;

ALTER TABLE work.inst_isglobal
ADD isglobal integer,
ALTER COLUMN iscountry TYPE integer USING CASE WHEN iscountry THEN 1 ELSE 0 END,
ALTER COLUMN isregion TYPE integer USING CASE WHEN isregion THEN 1 ELSE 0 END;

UPDATE work.inst_isglobal
SET isglobal=1-iscountry-isregion;

/*Add investor country*/

ALTER TABLE work.inst_isglobal
ADD inst_country char(2);

UPDATE work.inst_isglobal a
SET inst_country=b.iso_country
FROM factset.edm_standard_entity b
WHERE a.factset_entity_id=b.factset_entity_id;

SELECT * FROM work.inst_isglobal WHERE factset_entity_id='000BJX-E' order by quarter;

/*Check distribution of country, regional and global instituions in 2023*/

CREATE TABLE work.institutiontype2023 AS
SELECT  a.factset_entity_id, b.entity_proper_name,
CASE WHEN iscountry=1 THEN 'ctry fund'
WHEN isregion=1 THEN 'region fund'
ELSE 'global fund' END AS insttype, inst_country, sec_country, maxctryweight, region, maxregionweight, aum
FROM work.inst_isglobal a, factset.edm_standard_entity b
WHERE a.factset_entity_id=b.factset_entity_id
AND a.quarter=202304
ORDER BY insttype, aum DESC;


/*What are the biggest country, region and global institution from each country, for sanity check*/

CREATE TABLE work.max_inst_iso_2023 AS
SELECT a.factset_entity_id, a.entity_proper_name, a.aum, a.inst_country, a.insttype, a.sec_country, a.maxctryweight, a.region, a.maxregionweight
FROM work.institutiontype2023 a,
(SELECT max(aum) AS maxaum, inst_country, insttype FROM work.institutiontype2023 GROUP BY inst_country, insttype) b
WHERE a.inst_country=b.inst_country
AND a.insttype=b.insttype
AND a.aum=b.maxaum
ORDER BY inst_country, insttype;

/*Check distribution of country, region, and global funds*/
/*Most are country funds*/
SELECT
  FLOOR(quarter / 100) AS year,
  AVG((iscountry::int)) AS ctryprop,
  AVG((isregion::int)) AS regionprop,
  AVG((isglobal::int)) AS globalprop
FROM
  work.inst_isglobal
WHERE
  quarter % 100 = 4
GROUP BY
  FLOOR(quarter / 100)
ORDER BY
  year DESC;

/*check distribution in AUM*/
/*comparable across groups*/
SELECT floor(quarter/100) AS year, sum(iscountry*aum)/sum(aum) AS countryfundprop,
sum(isregion*aum)/sum(aum) AS regionfundprop,
sum(isglobal*aum)/sum(aum) AS isglobalfundprop
/* sum(isnonglobalfund*aum)/sum(aum) as isnonglobalfundprop, */
FROM work.inst_isglobal
GROUP BY year
ORDER BY year DESC;

/*3A. Home bias*/

CREATE TABLE work.mktcap_share(
year integer,
iso char(3),
mv numeric,
close numeric,
weight numeric,
weight_float numeric
);

COPY work.mktcap_share (year, iso, mv, close, weight, weight_float)
FROM '/home/ubuntu/jmp/data/ctry_mktcap_weight.csv'
DELIMITER ','
CSV HEADER;

/*Home bias*/

CREATE TABLE work.inst_homebias AS
SELECT a.factset_entity_id,a.quarter, a.year, inst_country,
sum(ctry_weight*is_dom) AS homeweight,
sum(ctry_weight*is_dom)-weight AS homebias,
(sum(ctry_weight*is_dom)-weight)/(1-weight) AS homebias_norm,
sum(ctry_weight*is_dom)-weight_float AS homebias_float,
(sum(ctry_weight*is_dom)-weight_float)/(1-weight_float) AS homebias_floatnorm
FROM work.inst_country_weight a,
work.mktcap_share b,
ctry c
WHERE a.inst_country=c.iso
AND c.iso3=b.iso
AND a.year=b.year
GROUP BY factset_entity_id, a.quarter, a.year, inst_country, weight, weight_float;

/*3B. Bekaert and Wang normalized foreign bias, including normalized home bias*/

CREATE TABLE work.inst_foreignbias AS
SELECT a.factset_entity_id,a.quarter, a.year, inst_country, sec_country,
CASE WHEN ctry_weight>weight THEN (ctry_weight-weight)/(1-weight)
WHEN ctry_weight<weight THEN (ctry_weight-weight)/weight
END AS foreign_bias
FROM work.inst_country_weight a,
work.mktcap_share b,
ctry c
WHERE a.sec_country=c.iso
AND c.iso3=b.iso
AND a.year=b.year;


/*Active share and HHI*/
create table work.inst_totalmktcap as
select a.factset_entity_id,a.quarter,
sum(own_mktcap) as totalmktcap
from work.v1_holdingsall a, work.sec_mktcap b
where a.fsym_id=b.fsym_id
and a.quarter=b.quarter
and own_mktcap >= 0
group by a.factset_entity_id, a.quarter;

/*Institution portfolio weight*/

CREATE TABLE work.inst_weight AS
SELECT a.quarter, a.factset_entity_id, a.fsym_id,  a.dollarholding,
		a.dollarholding/AUM AS weight,
         own_mktcap/totalmktcap AS mktweight,
         aum, adj_holding, a.adj_price,
		inst_country, sec_country, e.entity_sub_type

FROM work.v1_holdingsall_aug a, work.inst_aum b, work.inst_totalmktcap c, work.sec_mktcap d,
factset.edm_standard_entity e
WHERE  a.factset_entity_id=b.factset_entity_id
AND a.quarter=b.quarter
AND a.factset_entity_id=c.factset_entity_id
AND a.quarter=c.quarter
AND a.fsym_id=d.fsym_id
AND a.quarter=d.quarter
AND a.factset_entity_id=e.factset_entity_id;

/*#4. HHI index*/

CREATE TABLE work.inst_hhi AS
SELECT quarter, factset_entity_id, sum(weight*weight) AS hhi
FROM work.inst_weight
GROUP BY quarter, factset_entity_id;

/*5. Active share*/

CREATE TABLE work.inst_activeness AS
SELECT  quarter,entity_sub_type, sum(abs(weight-mktweight))/2 AS activeshare,
factset_entity_id, inst_country
FROM work.inst_weight a
GROUP BY quarter,factset_entity_id,entity_sub_type, inst_country;



/*#6. Portfolio concentration institution-security level*/

CREATE TABLE work.inst_concentration AS
SELECT a.factset_entity_id, a.fsym_id, a.quarter, weight-avg_weight AS conc
FROM work.inst_weight a,
(SELECT factset_entity_id, quarter, avg(weight) AS avg_weight
FROM work.inst_weight
GROUP BY factset_entity_id, quarter)b
WHERE a.factset_entity_id=b.factset_entity_id
AND a.quarter=b.quarter;

--75,605 securities
SELECT COUNT(DISTINCT FSYM_ID) FROM WORK.INST_WEIGHT;

SELECT COUNT(DISTINCT FACTSET_ENTITY_ID) FROM WORK.INST_WEIGHT;


/*#7. Churn ratio*/

CREATE TABLE work.inst_churn AS
SELECT
  a.quarter,
  a.factset_entity_id,
  a.fsym_id,
  a.adj_holding AS nshares,
  a.adj_price AS price,
  b.adj_holding AS nshares_lag,
  b.adj_price AS price_lag,
  ABS((a.adj_holding - b.adj_holding) * a.adj_price) AS trade,
  ABS((CASE WHEN a.adj_holding > b.adj_holding THEN a.adj_holding - b.adj_holding ELSE 0 END) * a.adj_price) AS trade_buy,
  ABS((CASE WHEN a.adj_holding < b.adj_holding THEN a.adj_holding - b.adj_holding ELSE 0 END) * a.adj_price) AS trade_sell,
  (a.adj_holding * a.adj_price + b.adj_holding * b.adj_price) / 2 AS aum
FROM
  work.v1_holdingsall a
LEFT JOIN work.v1_holdingsall b ON
  (a.factset_entity_id = b.factset_entity_id AND
  a.fsym_id = b.fsym_id AND
  a.quarter = quarter_add(b.quarter,1))  -- Assuming quarter is of a date type
WHERE
  b.adj_holding IS NOT NULL AND
  b.adj_holding != 0 AND
  b.adj_price IS NOT NULL
ORDER BY
  a.factset_entity_id, a.fsym_id, a.quarter;

CREATE TABLE work.inst_churn_ratio AS
SELECT
  quarter,
  factset_entity_id,
  SUM(trade) / NULLIF(SUM(aum), 0) AS CR,
  LEAST(
    SUM(trade_buy) / NULLIF(SUM(aum), 0),
    SUM(trade_sell) / NULLIF(SUM(aum), 0)
  ) AS CR_adj
FROM
  work.inst_churn
GROUP BY
  quarter,
  factset_entity_id;

UPDATE work.inst_churn_ratio
SET cr=2 WHERE cr>2;

/*290 after factset updated 2023 Q4*/

/*4-quarter moving average churn ratio*/

CREATE TABLE work.inst_churn_ma AS
SELECT
  quarter,
  factset_entity_id,
  cr,
  cr_adj,
  AVG(cr) OVER (PARTITION BY factset_entity_id ORDER BY quarter ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cr_ma,
  AVG(cr_adj) OVER (PARTITION BY factset_entity_id ORDER BY quarter ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cr_adj_ma
FROM
  work.inst_churn_ratio
ORDER BY
  factset_entity_id,
  quarter;



CREATE TABLE work.inst_characteristics AS
SELECT a.factset_entity_id, a.quarter, a.aum,
b.homebias, b.homebias_norm, b.homebias_float, b.homebias_floatnorm,
c.activeshare, d.cr, d.cr_ma,
d.cr_adj, d.cr_adj_ma,
n, n_dom, n_for,
hhi, isglobal
FROM work.inst_aum a
LEFT JOIN work.inst_homebias b
ON (a.factset_entity_id=b.factset_entity_id AND a.quarter=b.quarter)
LEFT JOIN work.inst_activeness c
ON (a.factset_entity_id=c.factset_entity_id AND a.quarter=c.quarter)
LEFT JOIN work.inst_churn_ma d
ON (a.factset_entity_id=d.factset_entity_id
AND a.quarter=d.quarter)
LEFT JOIN work.inst_nsecurities e
ON (a.factset_entity_id=e.factset_entity_id
and a.quarter=e.quarter)
LEFT JOIN work.inst_hhi f
ON (a.factset_entity_id=f.factset_entity_id
AND a.quarter=f.quarter)
LEFT JOIN work.inst_isglobal g
ON (a.factset_entity_id=g.factset_entity_id
AND a.quarter=g.quarter);

/*Before filtering, calculate the number of consecutive reports at each level*/

CREATE TABLE work.inst_quarter AS
SELECT DISTINCT factset_entity_id, quarter
FROM work.v1_holdingsall
ORDER BY factset_entity_id, quarter;


/*Number of consecutive reports*/
CREATE TABLE work.consecutive_inst AS
WITH ranked_quarters AS (
    SELECT
        factset_entity_id,
        quarter,
        LAG(quarter) OVER (PARTITION BY factset_entity_id ORDER BY quarter) AS prev_quarter,
        ROW_NUMBER() OVER (PARTITION BY factset_entity_id ORDER BY quarter) AS rn
    FROM
        work.inst_quarter
),
quarters_with_consec AS (
    SELECT
        factset_entity_id,
        quarter,
        prev_quarter,
        rn,
        CASE
            WHEN quarter = quarter_add(prev_quarter , 1) THEN 1
            WHEN rn = 1 THEN 1
            ELSE 0
        END AS is_consecutive
    FROM ranked_quarters
),
discontinuity_flags AS (
    SELECT
        factset_entity_id,
        quarter,
        is_consecutive,
        SUM(CASE WHEN is_consecutive = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY factset_entity_id ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS discontinuity_group
    FROM quarters_with_consec
),
consecutive_counts AS (
    SELECT
        factset_entity_id,
        quarter,
        ROW_NUMBER() OVER (PARTITION BY factset_entity_id, discontinuity_group ORDER BY quarter) AS consecutive_count
    FROM discontinuity_flags
    --WHERE is_consecutive = 1
)

SELECT * FROM consecutive_counts;

/*Maximum consecutive reports per institution*/

CREATE TABLE work.consecutive_inst_max AS
SELECT factset_entity_id, max(consecutive_count) AS max_consecutive
FROM work.consecutive_inst
GROUP BY factset_entity_id
order by max_consecutive;

/*Pre-filter 583,801 instituion-year observation*/
SELECT count(*) FROM work.inst_characteristics;

CREATE TABLE work.inst_filtered AS
SELECT a.factset_entity_id, quarter, aum, homebias, homebias_norm, homebias_float, homebias_floatnorm, activeshare, cr, cr_ma, cr_adj, cr_adj_ma, n, n_dom, n_for, hhi, isglobal,
       b.entity_proper_name, iso_country, entity_type, entity_sub_type
FROM work.inst_characteristics a,
work.factset_entities b,
work.consecutive_inst_max c
WHERE a.factset_entity_id=b.factset_entity_id
AND a.factset_entity_id=c.factset_entity_id
/*filters*/
AND activeshare IS NOT NULL
AND n_dom>5
AND n_for>5
AND aum>10
AND hhi<0.2
AND max_consecutive >= 2;

SELECT COUNT(*) from work.inst_filtered;

/*#8 Investment horizon investor-security*/

CREATE TABLE work.investment_horizon AS
WITH ranked_quarters AS (
    SELECT
        factset_entity_id,
        fsym_id,
        quarter,
        LAG(quarter) OVER (PARTITION BY factset_entity_id, fsym_id ORDER BY quarter) AS prev_quarter,
        ROW_NUMBER() OVER (PARTITION BY factset_entity_id, fsym_id ORDER BY quarter) AS rn
    FROM
        work.v1_holdingsall
),
quarters_with_consec AS (
    SELECT
        factset_entity_id,
        fsym_id,
        quarter,
        prev_quarter,
        rn,
        CASE
            WHEN quarter = quarter_add(prev_quarter , 1) THEN 1
            WHEN rn = 1 THEN 1
            ELSE 0
        END AS is_consecutive
    FROM ranked_quarters
),
discontinuity_flags AS (
    SELECT
        factset_entity_id,
        fsym_id,
        quarter,
        is_consecutive,
        SUM(CASE WHEN is_consecutive = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY factset_entity_id, fsym_id ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS discontinuity_group
    FROM quarters_with_consec
),
consecutive_counts AS (
    SELECT
        factset_entity_id,
        fsym_id,
        quarter,
        ROW_NUMBER() OVER (PARTITION BY factset_entity_id, fsym_id, discontinuity_group ORDER BY quarter) AS consecutive_count
    FROM discontinuity_flags
    --WHERE is_consecutive = 1
)

SELECT * FROM consecutive_counts;



