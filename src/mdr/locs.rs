

/*


DROP TABLE IF EXISTS ad.study_locations;
CREATE TABLE ad.study_locations(
	id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , facility_org_id        INT             NULL
  , facility               VARCHAR         NULL
  , facility_ror_id        VARCHAR         NULL
  , city_id                INT             NULL
  , city_name              VARCHAR         NULL
  , disamb_id              INT             NULL
  , disamb_name            VARCHAR         NULL
  , country_id             INT             NULL
  , country_name           VARCHAR         NULL
  , status_id              INT             NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
  , coded_on               TIMESTAMPTZ     NULL          
);
CREATE INDEX study_locations_sid ON ad.study_locations(sd_sid);


insert into ad.study_locations(sd_sid, facility, city_name, 
disamb_name, country_name, status_id)
select nct_id, 
coalesce(name, '(Unnamed Site)') as fac, city, state, country,
case
	when status = 'RECRUITING' then 14
	when status = 'NOT_YET_RECRUITING' then 10
	when status = 'ENROLLING_BY_INVITATION' then 16
	when status = 'ACTIVE_NOT_RECRUITING' then 18
	when status = 'COMPLETED' then 30
	when status = 'AVAILABLE' then 20
    when status = 'SUSPENDED' then 25
	when status = 'TERMINATED' then 32
end as status
from ctgov.facilities
where status <> 'WITHDRAWN'
or status is null
order by nct_id


update ad.study_locations
set facility = '(Unnamed Site)'
where facility like '%For additional information%';

update ad.study_locations
set facility = 'Boehringer Ingelheim Investigational Site'
where facility like '%Boehringer Ingelheim Investigational Site%';

update ad.study_locations
set facility = replace(facility, '"', '')
where facility like '%"%';

update ad.study_locations
set facility = replace(facility, '.', '')
where facility like '%.%';

update ad.study_locations
set facility = replace(facility, '''''', '')
where facility like '%''''%'

update ad.study_locations
set facility = replace(facility, '*', '')
where facility like '%*%';

update ad.study_locations
set facility = replace(facility, '?', '')
where facility like '%?%';

update ad.study_locations
set facility = replace(facility, '¿', '')
where facility like '%¿%';


update ad.study_locations
set facility = trim(leading '-' from facility)
where facility like '-%';

update ad.study_locations
set facility = trim(leading '''' from facility)
where facility like '''%';

update ad.study_locations
set facility = trim(leading '!' from facility)
where facility like '!%';

update ad.study_locations
set facility = trim(leading '&quot;' from facility)
where facility like '&quot;%';

update ad.study_locations
set facility = trim(leading '&' from facility)
where facility like '&%';

update ad.study_locations
set facility = trim(leading '•' from facility)
where facility like '•%';

update ad.study_locations
set facility = trim(leading '・' from facility)
where facility like '・%';

update ad.study_locations
set facility = trim(leading ',' from facility)
where facility like ',%';

update ad.study_locations
set facility = trim(leading ';' from facility)
where facility like ';%'

update ad.study_locations
set facility = trim(leading '/' from facility)
where facility like '/%';

update ad.study_locations
set facility = trim(leading ':' from facility)
where facility like ':%';

update ad.study_locations
set facility = trim(leading '~' from facility)
where facility like '~%';

update ad.study_locations
set facility = trim(leading '=' from facility)
where facility like '=%';

update ad.study_locations
set facility = trim(facility)
where facility like ' %';

update ad.study_locations
set facility = replace(facility, '''', '’')
where facility like '%''%';

update ad.study_locations
set facility = replace(facility, ' Med ', ' Medical ')
where facility like '% Med %';

update ad.study_locations
set facility = replace(facility, 'Gen ', 'General ')
where facility like '%Gen %'

update ad.study_locations
set facility = replace(facility, 'Univ', 'University')
where facility like '% Univ %'
or facility like 'Univ %'
or facility like '% Univ';

update ad.study_locations
set facility = replace(facility, 'Ctr', 'Center')  -- N.B. US spelling
where facility like '% Ctr%'
or facility like '%Ctr %';

update ad.study_locations
set facility = replace(facility, 'Hosp', 'Hospital')  -- N.B. US spelling
where facility like '%Hosp'
or facility like '%Hosp %';

update ad.study_locations
set facility = replace(facility, 'Natl', 'National')  -- N.B. US spelling
where facility like 'Natl %'
or facility like '% Natl %';
                        
update ad.study_locations
set facility = replace(facility, 'Inst', 'Institute')  -- N.B. US spelling
where facility like 'Inst %'
or facility like '% Inst %';                       


select facility
from ad.study_locations
where facility not like '(%'
and facility not like '#%'
order by facility

select * from ad.study_locations
order by sd_sid


-- try and match against geonames

-- set up foreign table wrapper
-- in this case mdr schema from geo db

CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA ad;  -- WITH SCHEMA <schema> required the first time in DB

CREATE SERVER IF NOT EXISTS geo
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'geo', port '5433')

CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
SERVER geo
OPTIONS (user 'postgres', password 'WinterIsComing!')
                
CREATE SCHEMA geo_mdr;
IMPORT FOREIGN SCHEMA mdr
FROM SERVER geo
INTO geo_mdr;


update ad.study_locations
set country_name = replace(country_name, '''', '’')
where country_name like '%''%';


update ad.study_locations c
set country_id = n.country_id
from 
geo_mdr.country_names n
where lower(c.country_name) = n.comp_name

-- 31 seconds

DROP TABLE IF EXISTS ad.study_countries;
CREATE TABLE ad.study_countries(
	id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , country_id             INT             NULL
  , country_name           VARCHAR         NULL
  , status_id              INT             NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
  , coded_on               TIMESTAMPTZ     NULL  default now()       -- already coded when added                                   
);
CREATE INDEX study_countries_sid ON ad.study_countries(sd_sid);


Insert into ad.study_countries(sd_sid, country_id, country_name)
select distinct sd_sid, country_id, country_name
from ad.study_locations c
order by sd_sid

-- try and update cities

update ad.study_locations
set city_name = replace(city_name, '''', '’')
where city_name like '%''%';

update ad.study_locations
set city_name = replace(city_name, '.', '')
where city_name like '%.%';

update ad.study_locations
set city_name = replace(city_name, '"', '')
where city_name like '%"%';

update ad.study_locations
set city_name = replace(city_name, ':', '')
where city_name like '%:%';

update ad.study_locations
set city_name = replace(city_name, '*', '')
where city_name like '%*%';

update ad.study_locations
set city_name = trim(trailing ',' from city_name)
where city_name like '%,';

update ad.study_locations
set city_name = trim(leading '’' from city_name)
where city_name like '’%';

update ad.study_locations
set city_name = 'Łódź' 
where city_name = '?ód?'
or city_name = '¿ód¿'
or city_name = '?Od?'
or city_name = '£ód?';

update ad.study_locations
set city_name = 'Istanbul' 
where city_name = '?stanbul'

update ad.study_locations
set city_name = 'Izmir' 
where city_name = '?zmir'

update ad.study_locations
set city_name = replace(city_name, '-cedex', ' cedex')
where city_name like '%-cedex%';

update ad.study_locations
set city_name = replace(city_name, ',cedex', ' cedex')
where city_name like '%,cedex%';

update ad.study_locations
set city_name = trim(substring(city_name, 1, strpos(city_name, ' cedex')))
where city_name like '% cedex%';

update ad.study_locations
set city_name = replace(city_name, ' Locations)', ' locations)')
where city_name like '% Locations)'

update ad.study_locations
set facility = '(Unnamed Sites, 2 locations)',
city_name = replace(city_name, ' (2 locations)', '')
where city_name like '% (2 locations)'
and facility = '(Unnamed Site)'

update ad.study_locations
set facility = '(Unnamed Sites, 3 locations)',
city_name = replace(city_name, ' (3 locations)', '')
where city_name like '% (3 locations)'
and facility = '(Unnamed Site)'

update ad.study_locations
set facility = '(Unnamed Sites, 6 locations)',
city_name = replace(city_name, ' (6  locations)', '')
where city_name like '% (6 locations)'
and facility = '(Unnamed Site)'

update ad.study_locations
set city_name = 'multiple locations'
where city_name ilike 'many locations%'
or city_name ilike 'multiple locations%'
or city_name ilike 'cities in%'

update ad.study_locations
set city_name = replace(city_name, 'St ', 'Saint ')
where city_name like 'St %'
and country_name in ('United States', 'United Kingdom');

update ad.study_locations
set city_name = replace(city_name, 'Ft ', 'Fort ')
where city_name like 'Ft %'
and country_name  = 'United States';

update ad.study_locations
set city_name = replace(city_name, 'N ', 'North ')
where city_name like 'N %'
and country_name  = 'United States';

update ad.study_locations
set city_name = replace(city_name, 'E ', 'East ')
where city_name like 'E %';

update ad.study_locations
set city_name = replace(city_name, 'W ', 'West ')
where city_name like 'W %';

update ad.study_locations
set city_name = replace(city_name, 'S ', 'South ')
where city_name like 'S %'
and country_name  = 'United States';


select * from ad.study_locations
where city_name like 'N %'
and city_name <> 'N Novgorod'
and city_name <> 'N Ionia'
and city_name not like 'N Efkarpia%'


select * from ad.study_locations
where city_name like 'N Ionia'
or city_name like 'Nea Ionia%'

N Efkarpia

select * from ad.study_locations
where city_name like 'N Efkarpia'
or city_name like 'Nea Efkarpia%'
or city_name like 'Efkarpia%'


select * from ad.study_locations
where  city_name like '%Nea Smyrni%'
add to Smyrni, also N Smirni

-- try and update disamb division

update ad.study_locations
set disamb_name = replace(disamb_name, '''', '’')
where disamb_name like '%''%';

update ad.study_locations
set disamb_name = replace(disamb_name, '.', '')
where disamb_name like '%.%';

update ad.study_locations
set disamb_name = replace(disamb_name, '"', '')
where disamb_name like '%"%';

update ad.study_locations
set disamb_name = trim(trailing ',' from disamb_name)
where disamb_name like '%,';

update ad.study_locations
set disamb_name = 'Andhra Pradesh'
where disamb_name = 'Andh Prad'
or disamb_name = 'Andhara Pradesh'
or disamb_name = 'Andharapradesh'
or disamb_name = 'Andhera Pradesh'
or disamb_name = 'Andhhra Pradesh'
or disamb_name = 'Andprad'
or disamb_name = 'AndhPrad'
or disamb_name = 'Andhra'
or disamb_name = 'Andhra-Pradesh'
or disamb_name = 'Andhra Pardesh'
or disamb_name = 'Andhra Pra'
or disamb_name = 'Andhra Prades'
or disamb_name = 'Andhra pradesh'
or disamb_name = 'Andhar Pradesh'
or disamb_name = 'Andrapradesh'
or disamb_name = 'Andrha Pradesh'
or disamb_name = 'Andhrapradesh'
or disamb_name = 'Andra Pradesh'
or disamb_name = 'AP';

update ad.study_locations
set disamb_name = 'Gujarat'
where disamb_name = 'GJ'
or disamb_name = 'Gjuarat'
or disamb_name = 'Guajrat'
or disamb_name = 'Guijarat'
or disamb_name = 'Guj'
or disamb_name = 'Gujar?t'
or disamb_name = 'gUJARAT'
or disamb_name = 'Gujarat, India'
or disamb_name = 'Gujaratc'
or disamb_name = 'Gujrat'
or disamb_name = 'Gujrat-India'
or disamb_name = 'Gujrata'
or disamb_name = 'Gujurat';

update ad.study_locations
set disamb_name = 'West Bengal'
where disamb_name = 'W Bengal'
or disamb_name = 'WB'
or disamb_name = 'West Bangal'
or disamb_name = 'west Bangol'
or disamb_name = 'West Benagal'
or disamb_name = 'West Benga'
or disamb_name = 'West Bengali'
or disamb_name = 'WEST Bengal'
or disamb_name = 'WestBengal';

update ad.study_locations
set disamb_name = 'Tamil Nadu'
where disamb_name = 'Tamal Nadu'
or disamb_name = 'Tami Nadu'
or disamb_name = 'Tamil-Nadu'
or disamb_name = 'Tamil N?du'
or disamb_name = 'Tamil nadu'
or disamb_name = 'Tamil NADU'
or disamb_name = 'Tamil Nadu State'
or disamb_name = 'Tamil Nadu, India'
or disamb_name = 'Tamil Naidu'
or disamb_name = 'Tamil, Nadu'
or disamb_name = 'Tamiladu'
or disamb_name = 'Tamilna'
or disamb_name = 'Tamilnadu'
or disamb_name = 'Taminadu'
or disamb_name = 'TN';

update ad.study_locations
set disamb_name = 'Uttar Pradesh'
where disamb_name = 'UP'
or disamb_name = 'U P'
or disamb_name = 'Uttar Pardesh'
or disamb_name = 'Uttar Prad'
or disamb_name = 'Uttar pradesh'
or disamb_name = 'Uttar Pradish'
or disamb_name = 'Uttar Prandesh'
or disamb_name = 'Uttar Prsdesh'
or disamb_name = 'Uttarpradesh'
or disamb_name = 'UttarPradesh'
or disamb_name = 'Uttart Pradesh'
or disamb_name = 'Utter Prad'
or disamb_name = 'Utter Pradesh'
or disamb_name = 'Uttra Pradesh';


-- update using city name, disamb name and country name

update ad.study_locations c
set city_id = n.city_id,
disamb_id = n.disamb_id
from 
geo_mdr.city_names n
where lower(c.city_name) = n.comp_name
and c.disamb_name = n.disamb_name
and c.country_id = n.country_id


select c.*, n.city_id, n.disamb_id, n.disamb_name
from ad.study_locations c
inner join geo_mdr.city_names n
on lower(c.city_name) = n.comp_name
and c.country_id = n.country_id
and c.city_id is null
and c.disamb_name is not null
and c.country_name <> 'United States'

-- match on city and country names only
-- But do NOT do for US, as multiple states have th same name town
-- and state is inidcated in almost every case 

update ad.study_locations c
set city_id = n.city_id,
disamb_id = n.disamb_id
from 
geo_mdr.city_names n
where lower(c.city_name) = n.comp_name
and c.country_id = n.country_id
and c.city_id is null
and c.country_name <> 'United States'

'Newcastle upon Tyne needs to be sorted'
city_names 1024483
comp_name = 'newcastle', city_name = 'Newcastle upon Tyne'
(from Newry based town)


-- DROP schema geo_mdr
-- DROP USER MAPPING FOR postgres SERVER geo;
-- DROP SERVER geo;


select city_name, disamb_name, country_name, count(id) from ad.study_locations c
where city_id is null
group by  city_name, disamb_name, country_name
having count(id) > 5
order by count(id) desc


select * from ad.study_locations c
where facility ilike 'the %'
order by city_name  

select city_name, trim(substring(city_name, 1, strpos(city_name, ' cedex') ))
from ad.study_locations c
where city_name like '%cedex%'
order by length(substring(city_name, 1, strpos(city_name, ' cedex') ))


--Need to add 
-- Duarte California 	United States id 5344147
-- Decatur Georgia 	United States 4191124
-- Coral Gables Florida 	United States 4151871
-- Kettering Ohio 	United States 4515843
-- Danville	Pennsylvania	United States	5186327
-- Westmead	New South Wales	Australia 2143973
-- Murray	Utah	United States	5778755
-- Cedar Rapids	Iowa	United States 4850751
-- Germantown	Tennessee	United States 4624601
-- Herlev		Capital Region Denmark 2620431
-- Saint Joseph	Michigan	United States  5008327
-- Plantation	Florida	United States 4168782
-- Saitama	Saitama	Japan  6940394
-- Creteil		Ile-de-France France  3022530 (also Crétail)
-- Clayton	Victoria	Australia 2171400
-- Peru	Illinois	United States 4905770
-- Miami Lakes	Florida	United States 4164186
-- Lake Success	New York	United States 5123853
-- Littleton	Colorado	United States 5429032
-- Duncansville	Pennsylvania	United States 5187508
-- Ypsilanti	Michigan	United States 5015688
-- Pekin	Illinois	United States 4905599
-- Taipei City		Taiwan	  Add to Taipei (also Taipei, Taiwan / taipei County)
-- Camperdown	New South Wales	Australia 2172563









*/