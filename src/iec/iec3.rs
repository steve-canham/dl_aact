

/*

-- Can finally move to deleting spurious entries

update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 10
and (
	exc_crit = '???'
	or exc_crit = '<Clinical'
	or exc_crit = '<Common'
	or exc_crit = '1-2'
	or exc_crit like '12.%'
	or exc_crit like '13.%'
	or exc_crit like '2.%'
	or exc_crit like '3.%'
	or exc_crit like '4.%'
	or exc_crit like '5.%'
	or exc_crit like '6.%'
	or exc_crit like '7.%'
	or exc_crit like '8.%'   
);


update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 10
and (
	lower(exc_crit) = 'age'
	or lower(exc_crit) = 'all'
	or lower(exc_crit) = 'and'
	or lower(exc_crit) = 'any'
	or exc_crit ilike 'ary%'
	or lower(exc_crit) = 'baseline'
	or lower(exc_crit) = 'basic'
	or exc_crit ilike 'case%'
	or exc_crit ilike 'clinic%'
	or lower(exc_crit) = 'common'
);


update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 10
and (
	exc_crit ilike 'criter%' 
	or lower(exc_crit) = 'critical'
	or lower(exc_crit) = 'current'
	or lower(exc_crit) = 'few'
	or lower(exc_crit) = 'for'
	or lower(exc_crit) = 'general'
	or lower(exc_crit) = 'initial'
	or lower(exc_crit) = 'key'
	or lower(exc_crit) = 'known'
	or lower(exc_crit) = 'list'
	or lower(exc_crit) = 'main'
);


update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 10
and (
	lower(exc_crit) = 'major'
	or lower(exc_crit) = 'medical'
	or exc_crit ilike 'meet%'
	or lower(exc_crit) = 'n. a'
	or lower(exc_crit) = 'n.a'
	or exc_crit ilike 'n/a%'
	or exc_crit ilike 'nil%'
	or lower(exc_crit) = 'no any'
	or lower(exc_crit) = 'no extra'
	or lower(exc_crit) = 'no formal'
);


update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 10
and (
	lower(exc_crit) = 'n?a'
	or lower(exc_crit) = 'multiple'
	or exc_crit ilike 'na$%'
	or lower(exc_crit) = 'neither'
	or lower(exc_crit) = 'no one'
	or exc_crit ilike 'no$%'
	or exc_crit ilike 'nil%'
	or lower(exc_crit) = 'no other'
	or lower(exc_crit) = 'nobody'
	or lower(exc_crit) = 'non'
	or exc_crit ilike 'non$%'
	or lower(exc_crit) = 'none'
);


update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 10
and (
	exc_crit ilike 'none%'
	or lower(exc_crit) = 'not'
	or lower(exc_crit) = 'not any'
	or lower(exc_crit) = 'not have'
	or lower(exc_crit) = 'nothing'
	or lower(exc_crit) = 'null'
	or lower(exc_crit) = 'obvious'
	or exc_crit ilike 'other%'
	or lower(exc_crit) = 'our'
	or lower(exc_crit) = 'overall'
);


update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 10
and (
	exc_crit ilike 'part%'
	or exc_crit ilike 'patient%'
	or exc_crit ilike 'phase%'
	or lower(exc_crit) = 'primary'
	or lower(exc_crit) = 'principal'
	or lower(exc_crit) = 'recipient'
	or lower(exc_crit) = 'refusal'
	or lower(exc_crit) = 'registry'
	or lower(exc_crit) = 'regular'
	or lower(exc_crit) = 's below'
	or lower(exc_crit) = 's exist'
	or lower(exc_crit) = 'same'
	or lower(exc_crit) = 'see'
	or lower(exc_crit) = 'see above'
);


update ad.iec_base
set exc_crit = null,
has_excl = false
where length(exc_crit) < 10
and (
	exc_crit ilike 'stage%'
	or exc_crit ilike 'step%'
	or exc_crit ilike 'study%'
	or lower(exc_crit) = 'the'
	or exc_crit ilike 'the %'
	or exc_crit ilike 'usual%'
	or lower(exc_crit) = 'subject'
	or lower(exc_crit) = 'systemic'
	or lower(exc_crit) = 'tbc'
	or lower(exc_crit) = 'temporary'
	or lower(exc_crit) = 'these'
	or lower(exc_crit) = 'to add'
	or lower(exc_crit) = 'trial'
	or lower(exc_crit) = 'us sites:'
);


select inc_crit
from ad.iec_base
where length(inc_crit) < 100
and not (inc_crit ~ '\n')
order by inc_crit

select inc_crit
from ad.iec_base
where not (inc_crit ~ '\n')
order by inc_crit


SELECT pg_size_pretty(pg_total_relation_size('ad.iec_bases'));
VACUUM (FULL, VERBOSE, ANALYZE) ad.iec_bases;
SELECT pg_size_pretty(pg_total_relation_size('ad.iec_bases'));


update ad.studies s
set iec_flag = 0;

update ad.studies s
set iec_flag = 1
from ad.iec_base e
where s.sd_sid = e.sd_sid
and elig_crit is not null
and not (elig_crit ~ '\n');

update ad.studies s
set iec_flag = 2
from ad.iec_base e
where s.sd_sid = e.sd_sid
and elig_crit is not null
and elig_crit ~ '\n';

update ad.studies s
set iec_flag = iec_flag + 4
from ad.iec_base e
where s.sd_sid = e.sd_sid
and has_incl = true
and not (inc_crit ~ '\n');

update ad.studies s
set iec_flag = iec_flag + 8
from ad.iec_base e
where s.sd_sid = e.sd_sid
and has_incl = true
and inc_crit ~ '\n';

update ad.studies s
set iec_flag = iec_flag + 16
from ad.iec_base e
where s.sd_sid = e.sd_sid
and has_excl = true
and not (exc_crit ~ '\n');

update ad.studies s
set iec_flag = iec_flag + 32
from ad.iec_base e
where s.sd_sid = e.sd_sid
and has_excl = true
and exc_crit ~ '\n';


SELECT pg_size_pretty(pg_total_relation_size('ad.studies'));
VACUUM (FULL, VERBOSE, ANALYZE) ad.studies;
SELECT pg_size_pretty(pg_total_relation_size('ad.studies'));


select iec_flag, count(id)
from ad.studies 
group by iec_flag


*/