use super::utils;

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;

/*
pub async fn build_titles_table (pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"SET client_min_messages TO WARNING; 
    DROP TABLE IF EXISTS ad.study_titles;
    CREATE TABLE ad.study_titles(
      id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
    , sd_sid                 VARCHAR         NOT NULL
    , title_type_id          INT
    , title_text             VARCHAR
    , lang_code              VARCHAR         NOT NULL default 'en'
    , is_default             BOOL
    , comments               VARCHAR
    , added_on               TIMESTAMPTZ     NOT NULL default now()
    );
    CREATE INDEX study_titles_sid ON ad.study_titles(sd_sid);"#;

	utils::execute_sql(sql, pool).await?;
    info!("study titles table (re)created");
    Ok(())

}
 */

pub async fn build_idents_table (pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"SET client_min_messages TO WARNING; 
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
    CREATE INDEX study_identifiers_sid ON ad.study_identifiers(sd_sid);"#;

	utils::execute_sql(sql, pool).await?;
    info!("study identifiers table (re)created");
    Ok(())

}

/* 
pub async fn load_titles_data (max_id: u64, pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let chunk_size = 2000000;

    // All studies appear to have a 'brief title'.

    let sql = r#"insert into ad.study_titles (sd_sid, title_type_id, title_text, is_default, comments)
        select nct_id, 15, brief_title, true, 'brief title in clinicaltrials.gov'
        from ctgov.studies c "#;

    utils::execute_phased_transfer(sql, max_id, chunk_size, " where ", "brief titles added", pool).await?;

    let sql = r#"insert into ad.study_titles (sd_sid, title_type_id, title_text, is_default, comments)
        select nct_id, 16, official_title, false, 'official title in clinicaltrials.gov'
        from ctgov.studies c
        where c.official_title is not null and c.official_title <> c.brief_title "#;

    utils::execute_phased_transfer(sql, max_id, chunk_size, " and ", "oficial titles added", pool).await?;

    let sql = r#"insert into ad.study_titles (sd_sid, title_type_id, title_text, is_default)
        select nct_id, 14, acronym, false
        from ctgov.studies c 
        where acronym is not null "#;

    utils::execute_phased_transfer(sql, max_id, chunk_size, " and ", "acronyms added", pool).await?;

    utils::vacuum_table("study_titles", pool).await?;

    Ok(())

}
*/


pub async fn load_idents_data (max_id: u64, pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let chunk_size = 2000000;

    // insert the nct ids themselves 
    
    let sql = r#"insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source, identifier_date)
                             select nct_id, nct_id, 11, 100120, 'clinicaltrials.gov', study_first_posted_date from ctgov.studies c "#;

    utils::execute_phased_transfer(sql, max_id, chunk_size, " where ", "nct ids added as identifiers", pool).await?;

    // insert the old 'alias' ids that were initially used for 3285 studies

    let sql = r#"insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source)
                             select nct_id, id_value, 44, 100120, 'clinicaltrials.gov' from ctgov.id_information c
                             where id_source = 'nct_alias' "#;
    utils::execute_phased_transfer(sql, max_id, chunk_size, " and ", "obsolete nct ids added as identifiers", pool).await?;

    // insert ids with url links to /reporter.nih... as funder / grant ids (agency may vary) 

    let sql = r#"insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source)
                             select nct_id, id_value, 13, null, id_type from ctgov.id_information c
                             where ((id_link is not null and id_link ilike '%reporter.nih%') 
                             or id_type = 'NIH') "#;
    utils::execute_phased_transfer(sql, max_id, chunk_size, " and ", "grant ids added as identifiers", pool).await?;

    // insert the remaining id data into a temp table for further processing
    
    let sql = r#"SET client_min_messages TO WARNING; 
	drop table if exists ad.temp_idents;
	create table ad.temp_idents
	(
		  sd_sid                 VARCHAR         NOT NULL
        , id_value               VARCHAR         NULL
        , id_source              VARCHAR         NULL
        , id_type_id             VARCHAR         NULL
        , id_type                VARCHAR         NULL
        , id_type_desc           VARCHAR         NULL
        , source_id              INT             NULL
        , source                 VARCHAR         NULL
        , done                   BOOL            NULL default False
        , flagged                BOOL            NULL default False
	);
    CREATE INDEX temp_idents_sid ON ad.temp_idents(sd_sid);
    SET client_min_messages TO WARNING;"#;
    
    utils::execute_sql(sql, pool).await?;

	let sql = r#"insert into ad.temp_idents (sd_sid, id_value, id_source, id_type, id_type_desc)
	select nct_id, id_value, id_source, id_type, id_type_description
	from ctgov.id_information c
    where (c.id_source <> 'nct_alias' 
    and (c.id_link is null or c.id_link not ilike '%reporter.nih%')
    and (c.id_type is null or c.id_type <> 'NIH')) "#;

    utils::execute_phased_transfer(sql, max_id, chunk_size, " and ", "", pool).await?;

	let sql = r#"SET client_min_messages TO NOTICE;"#;
    utils::execute_sql(sql, pool).await?;
   
    // Do some basic processing on the temp table values
    // First remove leading or trailing semi-colons
    // then split entries on any internal semi-colons (as most of these ppear to be compound)
    // replace the 'semi-colon' values with their split versions

    remove_both_ldtr_char_from_ident(';', pool).await?;

    let sql = r#"insert into ad.temp_idents (sd_sid, id_value, id_source, id_type, id_type_desc)
        select sd_sid, trim(unnest(string_to_array(id_value, ';'))) as new_value, 
        id_source, id_type, id_type_desc
        from ad.temp_idents
        where id_value ilike '%;%';"#;
    utils::execute_sql(&sql, pool).await?;

    let sql = r#"delete from ad.temp_idents 
        where id_value ilike '%;%'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} records with semi-colons split and deleted", res.rows_affected());
   
    // Remove those records where the identifier is the same as trhe NCT id

    let sql = r#"delete from ad.temp_idents 
        where id_value = sd_sid"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} records deleted where identifier = NCT Id", res.rows_affected());

    // Remove single quotes from the beginning and end of identifiers

    let sql = r#"update ad.temp_idents
        set id_value = trim(BOTH '''' from id_value)
        where id_value like '%''' or id_value like '''%'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} single quote characters removed from start or end of lines", res.rows_affected());

    // Tidy up the spurious characters to be found in the identifiers

    remove_both_ldtr_char_from_ident('"', pool).await?;
    remove_both_ldtr_char_from_ident('#', pool).await?;
    remove_both_ldtr_char_from_ident('-', pool).await?;
    remove_both_ldtr_char_from_ident(',', pool).await?;
    remove_both_ldtr_char_from_ident('.', pool).await?;

    remove_leading_char_from_ident('!', pool).await?;
    remove_leading_char_from_ident('+', pool).await?;
    remove_leading_char_from_ident('&', pool).await?;
    remove_leading_char_from_ident('*', pool).await?;
    remove_leading_char_from_ident(':', pool).await?;
    remove_leading_char_from_ident('/', pool).await?;
    remove_leading_char_from_ident('|', pool).await?;
    remove_leading_char_from_ident('´', pool).await?;

    replace_string_in_ident(")(", "", pool).await?;
    replace_string_in_ident(" (", " ", pool).await?;
    replace_string_in_ident(") ", " ", pool).await?;
    replace_string_in_ident("(", " ", pool).await?;
    replace_string_in_ident(")", " ", pool).await?;
    replace_string_in_ident("【", " ", pool).await?;
    replace_string_in_ident("】", " ", pool).await?;
    replace_string_in_ident("[", " ", pool).await?;
    replace_string_in_ident("]", " ", pool).await?;
    replace_string_in_ident("--", "-", pool).await?;
    replace_string_in_ident("  ", " ", pool).await?;
    replace_string_in_ident("  ", " ", pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = trim(id_value);"#;
    utils::execute_sql(&sql, pool).await?;

    // Remove those that are clearly dummies.

    let sql = r#"delete from ad.temp_idents 
        where 
        id_value in ('00-000', '00000', '000000', '0000000', 
        '00000000', '000000000', '0000000000', '000000000000')
        or id_value ilike '%12345678%' 
        or id_value ilike '%87654321%';"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} dummy identifiers, e.g. all 0s, or obvious sequences, removed", res.rows_affected());

    // remove those with '@' - e.g. email addresses, odd formulations

    let sql = r#"delete from ad.temp_idents 
        where id_value like '%@%';"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} email addresses and other odd identifier values with '@' removed", res.rows_affected());
    
    // Get rid of all identifiers that are just letters, spaces and hyphens.
    // Though a few of these may be sponsor Ids the vast bulk are acronyms, the name of the 
    // sponsor, a short formn of the study name, or something undecipherable. They are not useful identifiers.
    
    let sql = r#"delete from ad.temp_idents
        where id_value ~ '^[A-Za-z\-\s]+$';"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} identifiers consisting only of letters, spaces and hyphens removed", res.rows_affected());		

    // Can now start matching against regular expresions representing trial registry formats
    info!("");

    // EU CTR number

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'),
        id_source = 'secondary_id',
        id_type_id = 11,
        source_id = 100123
        where id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}' 
        and (
        length(id_value) = 14
        or id_value ilike '%eu%'
        or id_value ilike '%udract%'
        or id_value ilike '%edract%')"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} EU CTR identifiers found and labelled", res.rows_affected());	
info!("");

    // CTIS number

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'),
        id_source = 'secondary_id',
        id_type_id = 11,
        source_id = 110428
        where id_value ~ '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} CTIS identifiers found and labelled", res.rows_affected());	
    info!("");

    // WHO number 

    let sql = r#"update ad.temp_idents
        set id_value = 'U'||substring(id_value from '1111-[0-9]{4}-[0-9]{4}'),
        id_source = 'secondary_id',
        id_type_id = 11,
        source_id = 100115
        where id_value ~ '1111-[0-9]{4}-[0-9]{4}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} WHO U identifiers found and labelled", res.rows_affected());	
    info!("");

    // ACTRN

    replace_string_in_ident("ACTRN0", "ACTRN", pool).await?;  // preliminary tidying
    replace_string_in_ident("ACTRNO", "ACTRN", pool).await?;  // of a few records

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ACTRN[0-9]{14}'),
        id_source = 'secondary_id',
        id_type_id = 11,
        source_id = 100116
        where id_value ~ 'ACTRN[0-9]{14}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} ACTRN identifiers found and labelled", res.rows_affected());	
    info!("");

    // DRKS

    replace_string_in_ident("DRKS ID 0", "DRKS0", pool).await?;  // preliminary tidying (1 rec)
 
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'DRKS[0-9]{8}'),
        id_source = 'secondary_id',
        id_type_id = 11,
        source_id = 100124
        where id_value ~ 'DRKS[0-9]{8}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} DRKS identifiers found and labelled", res.rows_affected());	
    info!("");

    // ISRCTN

    replace_string_in_ident("ISRCTN : ", "ISRCTN", pool).await?;   // preliminary tidying (few recs)
    replace_string_in_ident("ISRCTN: ", "ISRCTN", pool).await?;  
    replace_string_in_ident("ISRCTN:", "ISRCTN", pool).await?;  
    replace_string_in_ident("ISRCTN No. ", "ISRCTN", pool).await?;  
    replace_string_in_ident("ISRCTN ", "ISRCTN", pool).await?; 
    replace_string_in_ident("ISRCTN-", "ISRCTN", pool).await?;  

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ISRCTN[0-9]{8}'),
        id_source = 'secondary_id',
        id_type_id = 11,
        source_id = 100126
        where id_value ~ 'ISRCTN[0-9]{8}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} ISRCTN identifiers found and labelled", res.rows_affected());	
    info!("");






    let _sql = r#"drop table if exists ad.temp_idents;"#;
    //utils::execute_sql(sql, pool).await?;

    utils::vacuum_table("study_identifiers", pool).await?;

    Ok(())

}


async fn replace_string_in_ident(s1: &str, s2: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = format!(r#"update ad.temp_idents
        set id_value = replace(id_value, '{}', '{}')
        where id_value like '%{}%'"#, s1, s2, s1);
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} '{}' replaced by '{}' in identifiers", res.rows_affected(), s1, s2);
    Ok(())
}

async fn remove_leading_char_from_ident(s: char, pool: &Pool<Postgres>) -> Result<(), AppError> {  

        let sql = format!(r#"update ad.temp_idents
        set id_value = trim(LEADING '{}' from id_value)
        where id_value like '{}%'"#, s, s);
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} '{}' characters removed from start of identifiers", res.rows_affected(), s);
    Ok(())
}


async fn remove_both_ldtr_char_from_ident(s: char, pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = format!(r#"update ad.temp_idents
        set id_value = trim(BOTH '{}' from id_value)
        where id_value like '%{}' or id_value like '{}%'"#, s, s, s);
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} '{}' characters removed from start or end of identifiers", res.rows_affected(), s);
    Ok(())
}

/* 



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
where id_value in ('00-000', '00000', '000000', '0000000', 
'00000000', '000000000', '0000000000', '000000000000',
);
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
order by count(id) desc

select id_type_description, count(id)
from ctgov.id_information
group by  id_type_description
order by  count(id) desc
    


*/