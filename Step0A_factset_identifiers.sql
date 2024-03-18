
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


/*Company identifiers after Step2*/

CREATE TABLE work.principal_security AS
SELECT a.fsym_id, currency, proper_name, fsym_primary_equity_id, fsym_primary_listing_id, active_flag, fref_security_type, fref_listing_exchange, listing_flag, regional_flag, security_flag, fsym_regional_id, fsym_security_id, universe_type,
       b.factset_entity_id
FROM factset.sym_coverage a
LEFT JOIN factset.own_sec_entity_eq b ON a.fsym_id = b.fsym_id
WHERE b.factset_entity_id IN (SELECT DISTINCT company_id FROM work.v2_holdingsall_firm)
AND b.factset_entity_id IS NOT NULL
AND a.fsym_id = a.fsym_primary_equity_id
ORDER BY b.factset_entity_id;

CREATE TABLE work.remaining_securities AS
SELECT a.fsym_id, currency, proper_name, fsym_primary_equity_id, fsym_primary_listing_id, active_flag, fref_security_type, fref_listing_exchange, listing_flag, regional_flag, security_flag, fsym_regional_id, fsym_security_id, universe_type,
       b.factset_entity_id
FROM factset.sym_coverage a
LEFT JOIN factset.own_sec_entity_eq b ON a.fsym_id = b.fsym_id
WHERE b.factset_entity_id IN (SELECT DISTINCT company_id FROM work.v2_holdingsall_firm)
AND b.factset_entity_id NOT IN (SELECT factset_entity_id FROM work.principal_security)
AND b.factset_entity_id IS NOT NULL
AND a.fref_security_type IN ('SHARE','PREFEQ')
ORDER BY b.factset_entity_id, a.active_flag DESC, a.fref_security_type DESC;

CREATE TABLE work.security_entity1 AS
SELECT factset_entity_id, fsym_id FROM work.principal_security
UNION ALL
SELECT factset_entity_id, fsym_id FROM work.remaining_securities;

CREATE TABLE work.security_entity AS
SELECT a.*, b.fsym_primary_listing_id
FROM security_entity1 a
LEFT JOIN factset.sym_coverage b
ON (a.fsym_id = b.fsym_id);


CREATE TABLE work.entity_identifiers AS
SELECT a.*,
	   CASE
	      WHEN b.isin IS NULL THEN c.isin
		  ELSE b.isin
	   END AS isin,
	   d.cusip,
	   e.sedol,
	   f.ticker_region
FROM security_entity a
LEFT JOIN factset.sym_isin b ON a.fsym_id = b.fsym_id
LEFT JOIN factset.sym_xc_isin c ON a.fsym_id = c.fsym_id
LEFT JOIN factset.sym_cusip d ON a.fsym_id = d.fsym_id
LEFT JOIN factset.sym_sedol e ON a.fsym_primary_listing_id = e.fsym_id
LEFT JOIN factset.sym_ticker_region f ON a.fsym_primary_listing_id = f.fsym_id;
