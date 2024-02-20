/*This SQL script calculates security-level institutional ownership*/

CREATE TABLE work.mic_exchange (
iso text,
mic_exchange_code text
);

COPY work.mic_exchange (iso, mic_exchange_code)
FROM '/home/ubuntu/jmp/data/mic_exchange.csv'
DELIMITER ','
CSV HEADER;

\copy work.mic_exchange1 FROM 'Users/yiliul2/Dropbox/JMP/Data/mic_exchange.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

/*#1: subset of qualifying securities*/

-- proxy for termination date from security prices table;
create table work.termination as
select fsym_ID, max(price_date) as termination_date
from factset.own_sec_prices_eq
group by fsym_ID;


/* securities that are defined as Equity or ADR in ownership, or Preferred if defined as PREFEQ in sym_coverage;*/
/*add exchange information to security table*/


create table work.equity_secs as
select t1.fsym_id, t1.issue_type, t1.iso_country, t1.fref_security_type,
t2.iso as ex_country, /*legacy code for security listing information*/
security_name
from (select a.fsym_id, a.issue_type, a.iso_country, a.mic_exchange_code, a.security_name, b.fref_security_type
      from factset.own_sec_coverage_eq a left join factset.sym_coverage b
on a.fsym_id = b.fsym_id
where a.fsym_id in (select distinct c.fsym_id from factset.own_sec_prices_eq c)
and(a.issue_type in ('EQ','AD') or (issue_type = 'PF' and b.fref_security_type = 'PREFEQ')))  t1,
work.mic_exchange t2
where t1.mic_exchange_code=t2.mic_exchange_code;

/*add dummies for local stock and depository receipts*/

create table work.own_basic as
select a.*, b.factset_entity_id, c.termination_date,
case when iso_country=ex_country then 1 else 0 end as islocal,
case when issue_type='AD' then 1 else 0 end as isdr
from work.equity_secs a, factset.own_sec_entity_eq b, work.termination c
where a.fsym_id = b.fsym_id and a.fsym_id = c.fsym_id;

