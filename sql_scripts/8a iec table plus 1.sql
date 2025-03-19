
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
set exc_crit =  trim(leading '\$£' from regexp_replace(exc_crit, '^\n\n', '$£'));

update ad.iec_base
set exc_crit =  trim(trailing '\$£' from regexp_replace(exc_crit, '\n\n$', '$£'));

update ad.iec_base
set exc_crit =  trim(exc_crit);

update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 3;

-- focus now on statements without carriage returns, that include all the shorter ones
-- including those that effectively mean 'no meaningful criteria supplied'

-- process inclusion criteria 2




select inc_crit
from ad.iec_base
where not (inc_crit ~ '\n')
and inc_crit not like '(-)%'
and inc_crit not like '(1%'
and inc_crit not like '（1%'
and inc_crit not like '(a)%'
and inc_crit not like '(A)%'
and inc_crit not like '(i)%'
and inc_crit not like '(I)%'
and inc_crit not like '1.%'
and inc_crit not like '1)%'
order by inc_crit

--etc.

update ad.iec_base
set inc_crit = trim(leading '-' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '-%';

update ad.iec_base
set inc_crit = trim(leading '*' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '*%';

update ad.iec_base
set inc_crit = trim(inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like ' %';

update ad.iec_base
set inc_crit = trim(leading '\\' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '\\%';

update ad.iec_base
set inc_crit = trim(leading '●' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '●%';

update ad.iec_base
set inc_crit = trim(leading '°' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '°%';

update ad.iec_base
set inc_crit = trim(leading '·' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '·%';

update ad.iec_base
set inc_crit = trim(leading '•' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '•%';

update ad.iec_base
set inc_crit = trim(leading '-' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '-%';

update ad.iec_base
set inc_crit = trim(inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like ' %';

update ad.iec_base
set inc_crit = trim(leading '.' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '.%';

update ad.iec_base
set inc_crit = trim(leading ':' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like ':%';

update ad.iec_base
set inc_crit = trim(leading ',' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like ',%';

update ad.iec_base
set inc_crit = trim(leading '_' from inc_crit)
where not (inc_crit ~ '\n')
and inc_crit like '_%';

update ad.iec_base
set inc_crit = trim(leading '-' from inc_crit)
where length(inc_crit) < 10 and inc_crit like '-%';

update ad.iec_base
set inc_crit = trim(leading '*' from inc_crit)
where length(inc_crit) < 10 and inc_crit like '*%';

update ad.iec_base
set inc_crit = trim(leading '•' from inc_crit)
where length(inc_crit) < 10 and inc_crit like '•%';

update ad.iec_base
set inc_crit =  trim(inc_crit)
where length(inc_crit) < 10; 

update ad.iec_base
set inc_crit = null,
has_incl = false
where length(inc_crit) < 3;

update ad.iec_base
set inc_crit = replace(inc_crit, '\', '')
where length(inc_crit) < 10;

update ad.iec_base
set inc_crit = null,
has_incl = false
where length(inc_crit) < 10
and (
	inc_crit = '???'
	or lower(inc_crit) = 'all'
	or lower(inc_crit) = 'general'
	or lower(inc_crit) = 'in- and'
	or lower(inc_crit) = 'major'
	or lower(inc_crit) = 'nil'
	or inc_crit ilike 'n/a%'
    or inc_crit ilike 'non%'
    or lower(inc_crit) = 'see above'
    or lower(inc_crit) = 'to_add'
);


select inc_crit
from ad.iec_base
where not (inc_crit ~ '\n')
order by inc_crit

-- process exclusion criteria 2

-- start with elimination of blanks and very short entries

-- still needs a large amount of trimming and tidying

update ad.iec_base
set exc_crit = replace(exc_crit, '\', '')
where length(exc_crit) < 10;

update ad.iec_base
set exc_crit = trim(leading '-' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '-%';

update ad.iec_base
set exc_crit = trim(leading '*' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '*%';

update ad.iec_base
set exc_crit = trim(both '.' from exc_crit)
where length(exc_crit) < 10;

-- again, elimination of blanks and very short entries
-- after changes above

update ad.iec_base
set exc_crit =  trim(exc_crit)
where length(exc_crit) < 10;

update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 3;

update ad.iec_base
set exc_crit = trim(leading ':' from exc_crit)
where length(exc_crit) < 10 and exc_crit like ':%';

update ad.iec_base
set exc_crit = trim(leading ';' from exc_crit)
where length(exc_crit) < 10 and exc_crit like ';%';

update ad.iec_base
set exc_crit = trim(both '\$£' from regexp_replace(exc_crit, '\n', '$£', 'g'))
where length(exc_crit) < 10;

update ad.iec_base
set exc_crit = trim(leading '•' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '•%';

update ad.iec_base
set exc_crit = trim(leading '1)' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '1)%';

update ad.iec_base
set exc_crit = trim(leading '1.' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '1.%';

update ad.iec_base
set exc_crit = trim(leading '-' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '-%';  -- need to do again

-- again, elimination of blanks and very short entries
-- after changes above

update ad.iec_base
set exc_crit =  trim(exc_crit)
where length(exc_crit) < 10;

update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 3;

update ad.iec_base
set exc_crit = trim(both '"' from exc_crit)
where length(exc_crit) < 10;

update ad.iec_base
set exc_crit = trim(leading '*' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '*%';   -- need to do again

update ad.iec_base
set exc_crit = trim(leading ',' from exc_crit)
where length(exc_crit) < 10 and exc_crit like ',%'; 

update ad.iec_base
set exc_crit = trim(leading '(' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '(%'; 

update ad.iec_base
set exc_crit = trim(trailing ')' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '%)'; 

update ad.iec_base
set exc_crit = trim(leading '●' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '●%'; 

update ad.iec_base
set exc_crit = trim(leading 'a)' from exc_crit)
where length(exc_crit) < 10 and exc_crit like 'a)%';

-- again, elimination of blanks and very short entries
-- after changes above

update ad.iec_base
set exc_crit =  trim(exc_crit)
where length(exc_crit) < 10;

update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 3;

update ad.iec_base
set exc_crit = trim(leading '#' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '#%'; 

update ad.iec_base
set exc_crit = trim(leading '.' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '.%'; 

update ad.iec_base
set exc_crit = trim(leading '[' from exc_crit)
where length(exc_crit) < 10 and exc_crit like '[%'; 

update ad.iec_base
set exc_crit = trim(leading ']' from exc_crit)
where length(exc_crit) < 10 and exc_crit like ']%'; 

-- final elimination of blanks and very short entries
-- after changes above

update ad.iec_base
set exc_crit =  trim(exc_crit)
where length(exc_crit) < 10;

update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 3;





