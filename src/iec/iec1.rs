

/*


DROP TABLE IF EXISTS ad.iec_base;
create table ad.iec_base(
    id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 1000001 increment by 1)
  , sd_sid                 VARCHAR         NOT null
  , criteria			   VARCHAR         NULL
  , elig_crit			   VARCHAR         NULL
  , inc_crit               VARCHAR         NULL
  , exc_crit               VARCHAR         null
  , has_incl			   bool            NOT null default false
  , has_excl			   bool            NOT null default false
  , examined               bool            NOT null default false

);
CREATE INDEX iec_base_sid ON ad.iec_base(sd_sid);


-- try and differentiate IEC

insert into ad.iec_base(sd_sid, criteria)
select nct_id, criteria
from ctgov.eligibilities;

-- tidy up the beginning of the criteria statements

update ad.iec_base
set criteria = trim(LEADING '*' from criteria)
where criteria like '*%'

update ad.iec_base
set criteria = trim(criteria)
where criteria like ' %'

-- identify the records with no criteria

update ad.iec_base
set examined = true
where criteria is null
or criteria ilike 'No eligibility criteria%';

update ad.iec_base
set examined = true
where criteria ilike 'data ana%'
or criteria ilike 'please contact site%';

update ad.iec_base
set examined = true
where criteria ilike 'no%' and
(criteria ilike 'none %'
or criteria like 'No patient will be included%'
or criteria like 'No in- or exclusion criteria%'
or criteria = 'Non applicable');


update ad.iec_base
set examined = true
where criteria ilike 'no %' and
(criteria = 'No formal inclusion or exclusion criteria'
or criteria like 'No inclusion or exclusion criteria%'
or criteria = 'No particular inclusion or exclusion criteria'
or criteria ilike 'No patients enrolled%'
or criteria = 'No inclusion or exclusion criteria');


-- flag 'inclusion' and  'exclusion' in the criteria text

update ad.iec_base
set has_incl = true
where criteria ilike '%inclusion%'
and examined = false;

update ad.iec_base
set has_excl = true
where criteria ilike '%exclusion%'
and examined = false;

-- records with no explicit inclusion or exclusion criteria

update ad.iec_base
set examined = true,
elig_crit = criteria
where has_incl = false
and has_excl = false
and examined = false

-- records with no explicit exclusion criteria

update ad.iec_base
set examined = true,
inc_crit = criteria
where has_incl = true
and has_excl = false
and examined = false

-- records with no explicit inclusion criteria

update ad.iec_base
set examined = true,
exc_crit = criteria
where has_incl = false
and has_excl = true
and examined = false

-- records with both explicit inclusion and exclusion criteria

update ad.iec_base
set examined = true,
inc_crit = (REGEXP_SPLIT_TO_ARRAY(criteria, 'exclusion', 'i'))[1],
exc_crit = (REGEXP_SPLIT_TO_ARRAY(criteria, 'exclusion', 'i'))[2]
where has_incl = true
and has_excl = true
and examined = false

-- takes about 30 seconds

-- process inclusion criteria 1

update ad.iec_base
set inc_crit = replace(inc_crit, 'Key Inclusion Criteria', 'Inclusion Criteria')
where inc_crit like 'Key Inclusion Criteria%';

update ad.iec_base
set inc_crit = replace(inc_crit, 'INCLUSION CRITERIA', 'Inclusion Criteria')
where inc_crit like 'INCLUSION CRITERIA%';

update ad.iec_base
set inc_crit = trim(leading 'Inclusion Criteria' from inc_crit)
where inc_crit like 'Inclusion Criteria%';

update ad.iec_base
set inc_crit = trim(leading ':' from inc_crit)
where inc_crit like ':%';

update ad.iec_base
set inc_crit = trim(inc_crit);

update ad.iec_base
set inc_crit =  trim(leading '\$£' from regexp_replace(inc_crit, '^\n\n', '$£'));

update ad.iec_base
set inc_crit =  trim(trailing '\$£' from regexp_replace(inc_crit, '\n\n$', '$£'));

update ad.iec_base
set inc_crit =  trim(inc_crit);

update ad.iec_base
set inc_crit = null,
has_incl = false
where length(inc_crit) < 3;

-- process exclusion criteria 1

update ad.iec_base
set exc_crit = trim(exc_crit)
where exc_crit like ' %';

update ad.iec_base
set exc_crit = replace(exc_crit, 'CRITERIA', 'Criteria')
where exc_crit like 'CRITERIA%';

update ad.iec_base
set exc_crit = trim(leading 'Criteria' from exc_crit)
where exc_crit like 'Criteria%';

update ad.iec_base
set exc_crit = trim(leading ':' from exc_crit)
where exc_crit like ':%';

update ad.iec_base
set exc_crit =  trim(exc_crit)
where exc_crit like ' %';

update ad.iec_base
set exc_crit =  trim(leading '\$£' from regexp_replace(exc_crit, '^\n\n', '$£'));

update ad.iec_base
set exc_crit =  trim(trailing '\$£' from regexp_replace(exc_crit, '\n\n$', '$£'));

update ad.iec_base
set exc_crit =  trim(exc_crit)
where exc_crit like ' %';

update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 3;

-- iec base table -- needs a lot more work, 
-- but very roughly can be split betwen those 
-- with internal carriage returns and those without
-- but some of the latter also a listing

SELECT pg_size_pretty(pg_total_relation_size('ad.iec_base'));
VACUUM (FULL, VERBOSE, ANALYZE) ad.iec_base;
SELECT pg_size_pretty(pg_total_relation_size('ad.iec_base'));

select count(elig_crit)
from ad.iec_base
where elig_crit is not null

select count(inc_crit)
from ad.iec_base
where inc_crit is not null

select count(exc_crit)
from ad.iec_base
where exc_crit is not null


update ad.studies s
set iec_flag = 1
from ad.iec_base e
where s.sd_sid = e.sd_sid
and elig_crit is not null
and not (elig_crit ~ '\n')
and elig_crit not like '(-)%'
and elig_crit not like '(1%'
and elig_crit not like '（1%'
and elig_crit not like '(a)%'
and elig_crit not like '(A)%'
and elig_crit not like '(i)%'
and elig_crit not like '(I)%'
and elig_crit not like '1.%'
and elig_crit not like '1)%';


update ad.studies s
set iec_flag = 2
from ad.iec_base e
where s.sd_sid = e.sd_sid
and elig_crit is not null
and (elig_crit ~ '\n'
or elig_crit like '(-)%'
or elig_crit like '(1%'
or elig_crit like '（1%'
or elig_crit like '(a)%'
or elig_crit like '(A)%'
or elig_crit like '(i)%'
or elig_crit like '(I)%'
or elig_crit like '1.%'
or elig_crit like '1)%');


update ad.studies s
set iec_flag = iec_flag + 4
from ad.iec_base e
where s.sd_sid = e.sd_sid
and has_incl = true
and not (inc_crit ~ '\n')
and inc_crit not like '(-)%'
and inc_crit not like '(1%'
and inc_crit not like '（1%'
and inc_crit not like '(a)%'
and inc_crit not like '(A)%'
and inc_crit not like '(i)%'
and inc_crit not like '(I)%'
and inc_crit not like '1.%'
and inc_crit not like '1)%';


update ad.studies s
set iec_flag = iec_flag + 8
from ad.iec_base e
where s.sd_sid = e.sd_sid
and has_incl = true
and (inc_crit ~ '\n'
or inc_crit like '(-)%'
or inc_crit like '(1%'
or inc_crit like '（1%'
or inc_crit like '(a)%'
or inc_crit like '(A)%'
or inc_crit like '(i)%'
or inc_crit like '(I)%'
or inc_crit like '1.%'
or inc_crit like '1)%');


update ad.studies s
set iec_flag = iec_flag + 16
from ad.iec_base e
where s.sd_sid = e.sd_sid
and has_excl = true
and not (exc_crit ~ '\n')
and exc_crit not like '(-)%'
and exc_crit not like '(1%'
and exc_crit not like '（1%'
and exc_crit not like '(a)%'
and exc_crit not like '(A)%'
and exc_crit not like '(i)%'
and exc_crit not like '(I)%'
and exc_crit not like '1.%'
and exc_crit not like '1)%';


update ad.studies s
set iec_flag = iec_flag + 32
from ad.iec_base e
where s.sd_sid = e.sd_sid
and has_excl = true
and (exc_crit ~ '\n'
or exc_crit like '(-)%'
or exc_crit like '(1%'
or exc_crit like '（1%'
or exc_crit like '(a)%'
or exc_crit like '(A)%'
or exc_crit like '(i)%'
or exc_crit like '(I)%'
or exc_crit like '1.%'
or exc_crit like '1)%');


SELECT pg_size_pretty(pg_total_relation_size('ad.studies'));
VACUUM (FULL, VERBOSE, ANALYZE) ad.studies;
SELECT pg_size_pretty(pg_total_relation_size('ad.studies'));


select iec_flag, count(id)
from ad.studies 
group by iec_flag

select * from ad.iec_base



*/