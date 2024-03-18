
/*This script creates tables that contain fund and entity information in FactSet*/
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

