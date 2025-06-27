

/* 


---------------------------------------------------------------
--STUDY TITLES
---------------------------------------------------------------


-- all studies appear to have a 'brief title'

insert into ad.study_titles (sd_sid, title_type_id, title_text, is_default, comments)
select nct_id, 15, brief_title, true, 'brief title in clinicaltrials.gov'
from ctgov.studies;

insert into ad.study_titles (sd_sid, title_type_id, title_text, is_default, comments)
select nct_id, 16, official_title, false, 'official title in clinicaltrials.gov'
from ctgov.studies s
where s.official_title is not null and s.official_title <> s.brief_title;

insert into ad.study_titles (sd_sid, title_type_id, title_text, is_default)
select nct_id, 14, acronym, false
from ctgov.studies
where acronym is not null;
        
SELECT pg_size_pretty(pg_total_relation_size('ad.study_titles'));
VACUUM (FULL, VERBOSE, ANALYZE) ad.study_titles;
SELECT pg_size_pretty(pg_total_relation_size('ad.study_titles'));


DROP TABLE IF EXISTS ad.study_identifiers;
CREATE TABLE ad.study_identifiers(
    id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , identifier_value       VARCHAR         NULL
  , identifier_type_id     INT             NULL
  , source_id              INT             NULL
  , source                 VARCHAR         NULL
  , source_ror_id          VARCHAR         NULL
  , identifier_date        VARCHAR         NULL
  , identifier_link        VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
  , coded_on               TIMESTAMPTZ     NULL                                     
);
CREATE INDEX study_identifiers_sid ON ad.study_identifiers(sd_sid);


DROP TABLE IF EXISTS ad.study_titles;
REATE TABLE ad.study_titles(
    id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , title_type_id          INT
  , title_text             VARCHAR
  , lang_code              VARCHAR         NOT NULL default 'en'
  , is_default             BOOL
  , comments               VARCHAR
  , added_on               TIMESTAMPTZ     NOT NULL default now()
);
CREATE INDEX study_titles_sid ON ad.study_titles(sd_sid);


-- a LOT of cleaning required. Best done manually, in turn.
-- in the source data initially

update ctgov.id_information
set id_value = trim(BOTH '"' from id_value)
where id_value like '"%';

update ctgov.id_information
set id_value = trim(BOTH '''' from id_value)
where id_value like '''%';

update ctgov.id_information
set id_value = trim(BOTH '#' from id_value)
where id_value like '#%';

update ctgov.id_information
set id_value = trim(BOTH '-' from id_value)
where id_value like '-%';

update ctgov.id_information
set id_value = trim(BOTH '.' from id_value)
where id_value like '.%';

update ctgov.id_information
set id_value = trim(LEADING '!' from id_value)
where id_value like '!%';

update ctgov.id_information
set id_value = trim(LEADING '+' from id_value)
where id_value like '+%';

update ctgov.id_information
set id_value = trim(LEADING '&' from id_value)
where id_value like '&%';

update ctgov.id_information
set id_value = trim(LEADING '*' from id_value)
where id_value like '*%';

update ctgov.id_information
set id_value = trim(LEADING ':' from id_value)
where id_value like ':%';

update ctgov.id_information
set id_value = trim(LEADING '/' from id_value)
where id_value like '/%';

update ctgov.id_information
set id_value = trim(LEADING '_' from id_value)
where id_value like '_%';

update ctgov.id_information
set id_value = trim(LEADING '|' from id_value)
where id_value like '_%';

update ctgov.id_information
set id_value = trim(LEADING '´' from id_value)
where id_value like '´%';

update ctgov.id_information
set id_value = trim(LEADING '(' from id_value)
where id_value like '(%';

update ctgov.id_information
set id_value = trim(TRAILING ')' from id_value)
where id_value like '%)';


update ctgov.id_information
set id_value = replace(id_value, ')(', ' ');

update ctgov.id_information
set id_value = replace(id_value, '(', '');

update ctgov.id_information
set id_value = replace(id_value, ')', ' ');

update ctgov.id_information
set id_value = replace(id_value, '（', '');

update ctgov.id_information
set id_value = replace(id_value, '）', ' ');

update ctgov.id_information
set id_value = replace(id_value, '【', '');

update ctgov.id_information
set id_value = replace(id_value, '】', ' ');

update ctgov.id_information
set id_value = replace(id_value, '[', '');

update ctgov.id_information
set id_value = replace(id_value, ']', ' ');

update ctgov.id_information
set id_value = replace(id_value, '  ', ' ');

update ctgov.id_information
set id_value = replace(id_value, '--', '-');

update ctgov.id_information
set id_value = trim(id_value);



delete from ctgov.id_information
where length(id_value) < 3;
-- 1554 go

delete from ctgov.id_information
where length(id_value) = 3
and id_value like '00%';
-- 546 go


delete from ctgov.id_information
where length(id_value) = 4
and id_value like '000%';
-- 166

delete from ctgov.id_information
where id_value = nct_id;
-- 214 go

delete from ctgov.id_information
where id_value in ('00-000', '00000', '000000', '0000000', 
'00000000', '000000000', '0000000000', '000000000000');
--34 go




select * from ctgov.id_information
order by id_value


-- deletions below still necessary?

update ctgov.id_information
set id_value = replace(id_value, 'number: ', '');

update ctgov.id_information
set id_value = replace(id_value, 'Number: ', '');

update ctgov.id_information
set id_value = replace(id_value, 'number ', '');

update ctgov.id_information
set id_value = replace(id_value, 'Number ', '');

update ctgov.id_information
set id_value = replace(id_value, 'N°', '');

update ctgov.id_information
set id_value = replace(id_value, 'n°', '');





select * from ctgov.id_information
where id_type is null and id_type_description is not null 
order by id_type, id_type_description

select * from ctgov.id_information
where id_type_description is not null 
order by id_type_description

select * from ctgov.id_information
where id_type not in ('AHRQ', 'CTIS', 'EUDRACT_NUMBER', 'FDA', 'NIH')
order by id_type, id_type_description

select * from ctgov.id_information
where (id_value ilike '%EU CT%' or id_value ilike '%Eudract%' or id_value ilike '%EU CTIS%')
--and id_type not in ('CTIS', 'EUDRACT_NUMBER')
order by id_value


-- use regex on these, and put the id_types, type_description accordingly...
--*********************************************************************
-- see below for corrections
--*********************************************************************

update ctgov.id_information i
set id_value = array_to_string(s.m, ','),
id_type = 'Trial Registry',
id_type_description = 'EMA Number'
from (
	select id, id_type_description, REGEXP_MATCHES(id_type_description,'[0-9]{4}-[0-9]{6}-[0-9]{2}') as m
	from ctgov.id_information
    where id_type_description ~ '[0-9]{4}-[0-9]{6}-[0-9]{2}') s
where i.id = s.id;
--49
 
update ctgov.id_information i
set id_value = array_to_string(s.m, ','),
id_type = 'Regulator Id',
id_type_description = 'ANSM Number'
from (
	select id, id_type_description, REGEXP_MATCHES(id_type_description,'[0-9]{4}-A[0-9]{5}-[0-9]{2}') as m
	from ctgov.id_information
    where id_type_description ~ '[0-9]{4}-A[0-9]{5}-[0-9]{2}') s
where i.id = s.id;
--365


update ctgov.id_information i
set id_value = array_to_string(s.m, ','),
id_type = 'Trial Registry',
id_type_description = 'EMA Number'
from (
	select id, REGEXP_MATCHES(id_value,'[0-9]{4}-[0-9]{6}-[0-9]{2}') as m
	from ctgov.id_information
    where id_value ~ '[0-9]{4}-[0-9]{6}-[0-9]{2}' 
    and id_value ilike '%eu%') s
where i.id = s.id;
--1808


update ctgov.id_information i
set id_value = array_to_string(s.m, ','),
id_type = 'Trial Registry',
id_type_description = 'EMA Number'
from (
	select id, REGEXP_MATCHES(id_value,'[0-9]{4}-[0-9]{6}-[0-9]{2}') as m
	from ctgov.id_information
    where id_value ~ '[0-9]{4}-[0-9]{6}-[0-9]{2}' 
    and (length(id_value) = 14 or length(id_value) = 17)) s
where i.id = s.id;


update ctgov.id_information i
set id_value = array_to_string(s.m, ','),
id_type = 'Regulator Id',
id_type_description = 'ANSM Number'
from (
	select id, REGEXP_MATCHES(id_value,'[0-9]{4}-A[0-9]{5}-[0-9]{2}') as m
	from ctgov.id_information
    where id_value ~ '[0-9]{4}-A[0-9]{5}-[0-9]{2}' ) s
where i.id = s.id;
-- 6791


update ctgov.id_information
set id_value = trim(LEADING '{' from id_value)
where id_value like '{%';

update ctgov.id_information
set id_value = trim(TRAILING '}' from id_value)
where id_value like '%}';

--************************************************************************
-- watch for 'substudy', 'followup', 'related', esp with NCT numbers
-- remember existing NCT and Dutch old / new tables....

--use '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'      euctr numbers
--use '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'    ctis numbers  (will using 5 be permanent?)
--use array_to_string()
--? -- anchor to start in some cases

--************************************************************************

select *
	from ctgov.id_information
    where id_value ~ '[0-9]{4}-[0-9]{6}-[0-9]{2}' 
    and length(id_value) <> 14
    order by id_value
    


insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source, identifier_date)
select nct_id, 11, nct_id, 100120, 'clinicaltrials.gov', 
from ctgov.studies;


insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source, identifier_date, identifier_link)
select nct_id, 100, id_value, , 
from ctgov.id_information;




select REGEXP_MATCHES(id_value,'[0-9]{4}-A[0-9]{5}-[0-9]{2}') as m, *
	from ctgov.id_information
    where id_value ~ '[0-9]{4}-A[0-9]{5}-[0-9]{2}' 
    

select * from ctgov.id_information
where id_type is null and id_type_description is not null
order by id_value


select id_type, count(id)
from ctgov.id_information
group by  id_type
order by  count(id) desc

select id_type_description, count(id)
from ctgov.id_information
group by  id_type_description
order by  count(id) desc
    


*/