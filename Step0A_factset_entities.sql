
/*This script creates tables that contain fund and entity information in FactSet*/
--1. FactSet entity and funds information
--2. Security and company identifiers

CREATE TABLE work.funds AS
SELECT
    factset_fund_id, factset_inst_entity_id, fund_type, style, fund_family, etf_type, active_flag
    FROM factset.own_ent_funds;

ALTER TABLE work.funds
ADD COLUMN fs_ultimate_parent_entity_id CHAR(8),
ADD COLUMN factset_entity_id CHAR(8);

UPDATE work.funds a
SET fs_ultimate_parent_entity_id=b.fs_ultimate_parent_entity_id,
    factset_entity_id=b.factset_entity_id
FROM factset.edm_standard_entity_structure b
WHERE a.factset_inst_entity_id=b.factset_entity_id;

ALTER TABLE work.funds
ADD COLUMN fund_name TEXT;

UPDATE work.funds a
SET fund_name=b.entity_proper_name
FROM factset.edm_standard_entity b
WHERE a.factset_fund_id=b.factset_entity_id;

ALTER TABLE work.funds
ADD COLUMN entity_name TEXT,
add iso char(2);

UPDATE work.funds a
SET entity_name=b.entity_proper_name,
    iso=b.iso_country
FROM factset.edm_standard_entity b
WHERE a.factset_entity_id=b.factset_entity_id;

ALTER TABLE work.funds
ADD COLUMN parent_name TEXT;

UPDATE work.funds a
SET parent_name=b.entity_proper_name
FROM factset.edm_standard_entity b
WHERE a.fs_ultimate_parent_entity_id=b.factset_entity_id;

SELECT count(*) from work.funds where parent_name is not null;

CREATE TABLE work.factset_entities AS
SELECT factset_entity_id, entity_proper_name, iso_country,
entity_type, entity_sub_type
FROM factset.edm_standard_entity;


/*Security and company identifiers: after Step1A*/

CREATE TABLE sym_identifiers1 AS
SELECT DISTINCT fsym_ID FROM work.own_basic;

CREATE TABLE work.sym_identifiers AS
SELECT a.fsym_ID,
	   CASE
	      WHEN b.isin IS NULL THEN c.isin
		  ELSE b.isin
	   END AS isin,
	   d.cusip,
	   f.sedol,
	   g.ticker_region,
	   c1.entity_proper_name
FROM sym_identifiers1 a
LEFT JOIN factset.sym_isin b
		ON (a.fsym_ID = b.fsym_ID)
LEFT JOIN factset.sym_xc_isin c
		ON (a.fsym_ID = c.fsym_ID)
LEFT JOIN factset.sym_cusip d
		ON (a.fsym_ID = d.fsym_ID)
LEFT JOIN factset.sym_coverage e
		ON (a.fsym_ID = e.fsym_ID)
LEFT JOIN factset.sym_sedol f
		ON (e.fsym_primary_listing_id = f.fsym_ID)
LEFT JOIN factset.sym_ticker_region g
		ON (e.fsym_primary_listing_id = g.fsym_ID),
		factset.own_sec_entity_eq b1, factset.edm_standard_entity c1
WHERE a.fsym_id=b1.fsym_id
AND b1.factset_entity_id=c1.factset_entity_id
ORDER BY fsym_ID;


