

DROP TABLE IF EXISTS ad.studies;
CREATE TABLE ad.studies(
    id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY  (start with 1000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , display_title          VARCHAR         NULL
  , title_lang_code        VARCHAR         NOT NULL default 'en'
  , brief_description      VARCHAR         NULL
  , reg_year         	   INT             NULL
  , reg_month        	   INT             NULL
  , start_year      	   INT             NULL
  , start_month      	   INT             NULL
  , comp_year      		   INT             NULL
  , comp_month      	   INT             NULL
  , res_year      		   INT             NULL
  , res_month      		   INT             NULL
  , type_id                INT             NOT NULL default 0
  , status_id        	   INT             NOT NULL default 0
  , enrolment              VARCHAR         NULL
  , enrolment_type_id      INT             NULL
  , gender_flag            INT             NULL
  , min_age                INT             NULL
  , min_age_units_id       INT             NULL
  , max_age                INT             NULL
  , max_age_units_id       INT             NULL
  , age_group_flag         INT             NULL
  , iec_flag               INT             NOT NULL default 0 
  , ipd_sharing			   VARCHAR         NULL
  , dt_of_data_fetch	   TIMESTAMPTZ     NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
);
CREATE INDEX studies_sid ON ad.studies(sd_sid);



-- basic study information (to be augmented)
-- datetime of data fetch taken to be the most recent update time in the studies table

insert into ad.studies (sd_sid, display_title, reg_year, reg_month, 
start_year, start_month, comp_year, comp_month, 
res_year, res_month, type_id, enrolment, enrolment_type_id)
select nct_id, brief_title, 
substring(study_first_posted_date::varchar, 1, 4)::int, substring(study_first_posted_date::varchar, 6, 2)::int,
substring(start_month_year, 1, 4)::int, substring(start_month_year, 6, 2)::int,
substring(completion_month_year, 1, 4)::int, substring(completion_month_year, 6, 2)::int,
substring(results_first_posted_date::varchar, 1, 4)::int, substring(results_first_posted_date::varchar, 6, 2)::int,
case 
	when study_type = 'INTERVENTIONAL' then 11
	when study_type = 'OBSERVATIONAL' then 12
	when study_type = 'EXPANDED_ACCESS' then 14
	else 0
end,
enrollment, 
case 
	when enrollment_type = 'ACTUAL' then 1
	when enrollment_type = 'ESTIMATED' then 2
end
from ctgov.studies;

Update ad.studies s
set type_id = 13    -- patient registry
from ctgov.studies c
where s.sd_sid = c.nct_id
and c.patient_registry = true;


/*
changes to study type

11	Interventional
12	Observational 
14	Expanded access
15	Funded programme
16	Other
13	Observational patient registry
0	Not provided

to ...

11	Interventional* / BA/BE
12	Observational*/ observation... / epidem... / PMS / Relative factors research / Cause / Cause/Relative factors study / Health Services Research / Health services reaserch
13	Patient registry / Observational patient registry
14	Expanded access*
15	Funded programme
16	Diagnostic test*
99	Other*/ Others,meta-analysis etc / basic science / Prevention / Screening / Treatment study
NULL unknown, Not provided / Not provided / Not Specified / N/A
                     
*/

Update ad.studies s
set status_id = case
	when c.overall_status = 'COMPLETED' then 30
	when c.overall_status = 'NOT_YET_RECRUITING' then 10
	when c.overall_status = 'WITHDRAWN' then 12
	when c.overall_status = 'RECRUITING' then 14
	when c.overall_status = 'APPROVED_FOR_MARKETING' then 30
	when c.overall_status = 'ENROLLING_BY_INVITATION' then 16
	when c.overall_status = 'ACTIVE_NOT_RECRUITING' then 18
	when c.overall_status = 'AVAILABLE' then 20
    when c.overall_status = 'SUSPENDED' then 25
	when c.overall_status = 'TERMINATED' then 32
	else 0
end
from ctgov.studies c
where s.sd_sid = c.nct_id
and c.overall_status <> 'UNKNOWN';

Update ad.studies s
set status_id = case
	when c.last_known_status = 'NOT_YET_RECRUITING' then 10
	when c.last_known_status = 'RECRUITING' then 14
	when c.last_known_status = 'ENROLLING_BY_INVITATION' then 16
	when c.last_known_status = 'ACTIVE_NOT_RECRUITING' then 18
end
from ctgov.studies c
where s.sd_sid = c.nct_id
and c.overall_status = 'UNKNOWN'
and c.last_known_status is not null;


/*
changes to study status

16	Not yet recruiting
18	Suspended
19	Enrolling by invitation
20	Approved for marketing
25	Ongoing
24	Other
15	Active, not recruiting
21	Completed
11	Withdrawn
12	Available
13	Withheld
17	No longer available
22	Terminated
0	Not provided

to ...

10	Not yet recruiting* / pending / without startig enrollment (sic) / preinitiation
12	Withdrawn*
14  Recruiting* / open public recruiting / open to recruitment / in enrollment
16  Enrolling by invitation*
18  Ongoing, no longer recruiting / Active, not recruiting / ongoing / authorised-recruitment may be ongoing or finished / available
20  Ongoing, recruitment status unclear / Available / ongoing / authorised-recruitment may be ongoing or finished 
25	Suspended* / temporarily closed / temporary halt 
30  Completed* / Approved for marketing
32  Terminated* / stopped early / stopped

98  Recorded as not applicable / not applicable
0	Withheld* / No longer available / temporarily not available
NULL Not provided
*/


select type_id, count(id)
from ad.studies 
group by type_id
order by count(id) desc;

select status_id, count(id)
from ad.studies 
group by status_id
order by count(id) desc;

update ad.studies s
set brief_description = c.description
from ctgov.brief_summaries c
where s.sd_sid = c.nct_id;

update ad.studies s
set min_age = c.minimum_age_num,
min_age_units_id = case
	when minimum_age_unit = 'year' then 17
	when minimum_age_unit = 'month' then 16
	when minimum_age_unit = 'week' then 15
	when minimum_age_unit = 'day' then 14
	when minimum_age_unit = 'hour' then 13
	when minimum_age_unit = 'minute' then 12
end,
max_age = c.maximum_age_num,
max_age_units_id = case
	when maximum_age_unit = 'year' then 17
	when maximum_age_unit = 'month' then 16
	when maximum_age_unit = 'week' then 15
	when maximum_age_unit = 'day' then 14
	when maximum_age_unit = 'hour' then 13
	when maximum_age_unit = 'minute' then 12
end
from ctgov.calculated_values c
where s.sd_sid = c.nct_id;


update ad.studies s
set age_group_flag = 0;

update ad.studies s
set age_group_flag = 1
from ctgov.eligibilities c
where s.sd_sid = c.nct_id
and c.child = true;


update ad.studies s
set age_group_flag = age_group_flag + 2
from ctgov.eligibilities c
where s.sd_sid = c.nct_id
and c.adult = true;


update ad.studies s
set age_group_flag = age_group_flag + 4
from ctgov.eligibilities c
where s.sd_sid = c.nct_id
and c.older_adult = true;


update ad.studies s
set age_group_flag = null
where min_age is null
and max_age is null;


select nct_id, min_age, min_age_units_id, max_age, max_age_units_id, 
child, adult, older_adult, age_group_flag
from ad.studies s inner join ctgov.eligibilities c
on s.sd_sid = c.nct_id;


update ad.studies s
set gender_flag = 
case 
	when c.gender = 'ALL' then 3
	when c.gender = 'FEMALE' then 1
	when c.gender = 'MALE' then 2
	else 0
end
from ctgov.eligibilities c
where s.sd_sid = c.nct_id;



update ad.studies s
set dt_of_data_fetch = dt.max
from
(select max(updated_at) as max from ctgov.studies ) as dt


select * from ad.studies

-- update study status to 'complete' if a results date present
-- if not already 'complete'or 'terminated'
-- (leaving 'withdrawn', if any, as they are)

update ad.studies
set status_id = 30
where res_year is not null
and status_id < 30 
and status_id <> 12 

--1231

-- update study status to 'complete' if a 'actual' 
-- complete date present. If not already 'complete' or 'terminated'
-- (leaving 'withdrawn', if any, as they are)

update ad.studies s
set status_id = 30
from ctgov.studies c
where s.sd_sid = c.nct_id
and s.status_id < 30
and s.status_id <> 12 
and c.completion_date_type = 'ACTUAL'

-- 16

-- update studies to 'complete' if a 'estimated' 
-- complete date present before 2020 - 5 years ago at least
-- If not already 'complete' or 'terminated'
-- (leaving 'withdrawn' and 'suspended', if any, as they are)

update ad.studies s
set status_id = 30
from ctgov.studies c
where s.sd_sid = c.nct_id
and s.comp_year < 2020
and s.status_id < 30
and s.status_id <> 12 
and s.status_id <> 25
and c.completion_date_type = 'ESTIMATED'



update ad.studies a
set ipd_sharing = ds.sharing
from
(select nct_id, plan_to_share_ipd||
case 
	when plan_to_share_ipd_description is not null then E'\n'||plan_to_share_ipd_description
	else ''
end as sharing
from ctgov.studies s
where plan_to_share_ipd = 'NO' or plan_to_share_ipd = 'UNDECIDED') ds
where a.sd_sid = ds.nct_id;


drop table if exists ctgov.assoc_ipd_docs;
create table ctgov.assoc_ipd_docs 
(
   nct_id varchar primary key,
   docs  varchar 
);


insert into ctgov.assoc_ipd_docs 
select nct_id, 
string_agg (
		replace(replace(name, 'STUDY_PROTOCOL' ,'Study protocol'), 'ANALYTIC_CODE' , 'Analytic code'), ', '
		)
from ctgov.ipd_information_types
group by nct_id;


update ad.studies a
set ipd_sharing = ds.sharing
from
(select s.nct_id, plan_to_share_ipd||
case 
	when plan_to_share_ipd_description is not null then E'\n'||plan_to_share_ipd_description
	else ''
end
|| case 
	when ipd_time_frame is not null then E'\nTime frame: '||ipd_time_frame
	else ''
end
|| case 
	when ipd_access_criteria is not null then E'\nAccess criteria: '||ipd_access_criteria
	else ''
end
||case 
	when ipd_url is not null then E'\nURL: '||ipd_url
	else ''
end
||case 
	when docs is not null then E'\nSupporting documents: '|| docs
	else ''
end
|| E'\n(as of '||s.last_update_posted_date||')' as sharing
from ctgov.studies s
left join ctgov.assoc_ipd_docs t
on s.nct_id = t.nct_id
where s.plan_to_share_ipd = 'YES') ds
where a.sd_sid = ds.nct_id;


drop table if exists ctgov.assoc_ipd_docs;


-- normally leave until final updates...after iec_flag calculated

SELECT pg_size_pretty(pg_total_relation_size('ad.studies'));
VACUUM (FULL, VERBOSE, ANALYZE) ad.studies;
SELECT pg_size_pretty(pg_total_relation_size('ad.studies'));


--select * from ad.studies

--select plan_to_share_ipd, count(nct_id)
--from ctgov.studies s group by plan_to_share_ipd

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



