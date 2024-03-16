--Augment holdings table with institution origin

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

