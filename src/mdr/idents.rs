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

    // Insert the nct ids themselves.
    
    let sql = r#"insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source, identifier_date)
                             select nct_id, nct_id, 120, 100133, 'National Library of Medicine', study_first_posted_date from ctgov.studies c "#;

    utils::execute_phased_transfer(sql, max_id, chunk_size, " where ", "nct ids added as identifiers", pool).await?;

    // Insert the old 'alias' ids that were initially used for 3285 studies.

    let sql = r#"insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source)
                             select nct_id, id_value, 180, 100133, 'National Library of Medicine' from ctgov.id_information c
                             where id_source = 'nct_alias' "#;
    utils::execute_phased_transfer(sql, max_id, chunk_size, " and ", "obsolete nct ids added as identifiers", pool).await?;

    // Insert ids with url links to /reporter.nih... as funder / grant ids (agency may vary).

    let sql = r#"insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source, identifier_link)
                             select nct_id, id_value, 401, 100134, 'National Institutes of Health', id_link from ctgov.id_information c
                             where ((id_link is not null and id_link ilike '%reporter.nih%') 
                             or id_type = 'NIH') "#;
    utils::execute_phased_transfer(sql, max_id, chunk_size, " and ", "grant ids added as identifiers", pool).await?;

    // Insert the remaining id data into a temp table for further processing.
    
    let sql = r#"SET client_min_messages TO WARNING; 
	drop table if exists ad.temp_idents;
	create table ad.temp_idents
	(
		  sd_sid                 VARCHAR         NOT NULL
        , id_value               VARCHAR         NULL
        , id_source              VARCHAR         NULL
        , id_type_id             INT             NULL
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
    // then split entries on any internal semi-colons (as most of these appear to be compound)
    // replace the 'semi-colon' values with their split versions.

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
   
    // Remove those records where the identifier is the same as trhe NCT id.

    let sql = r#"delete from ad.temp_idents 
        where id_value = sd_sid"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} records deleted where identifier = NCT Id", res.rows_affected());

    // Remove single quotes from the beginning and end of identifiers.

    let sql = r#"update ad.temp_idents
        set id_value = trim(BOTH '''' from id_value)
        where id_value like '%''' or id_value like '''%'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} single quote characters removed from start or end of lines", res.rows_affected());

    // Tidy up the spurious characters to be found in the identifiers.

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

    // Seems to need to be done twice, albeit for a very small number of records.

    replace_string_in_ident("  ", " ", pool).await?;
    replace_string_in_ident("  ", " ", pool).await?;  

    // Final trim.

    let sql = r#"update ad.temp_idents
        set id_value = trim(id_value);"#;
    utils::execute_sql(&sql, pool).await?;
    info!("");

    // Remove those that are clearly dummies.

    let sql = r#"delete from ad.temp_idents 
        where 
        id_value in ('0', '00', '000', '0000', '00-000', '00000', '000000', '0000000', 
        '00000000', '000000000', '0000000000', '000000000000', '0000-00000')
        or id_value ilike '%12345678%' 
        or id_value ilike '%87654321%';"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} dummy identifiers, e.g. all 0s, or obvious sequences, removed", res.rows_affected());

    // Remove those that are too small to be useful (unless type is given).

    let sql = r#"delete from ad.temp_idents 
        where length(id_value) < 3
        and id_type_desc is not null"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} very short  identifiers (1 or 2 characters) removed", res.rows_affected());

    // Remove those with '@' - e.g. email addresses, odd formulations

    let sql = r#"delete from ad.temp_idents 
        where id_value like '%@%';"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} email addresses and other odd identifier values with '@' removed", res.rows_affected());

    // Remove identifiers that are study acronyms.

    let sql = r#"delete from ad.temp_idents i
        using ad.study_titles t
        where i.sd_sid = t.sd_sid
        and t.title_type_id = 14
        and upper(i.id_value) = upper(t.title_text);"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} identifiers removed as having the same value as the study acronym", res.rows_affected());

    // Remove identifiers that are study brief titles.

    let sql = r#"delete from ad.temp_idents i
        using ad.study_titles t
        where i.sd_sid = t.sd_sid
        and t.title_type_id in (15, 16)
        and upper(i.id_value) = upper(t.title_text);"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} identifiers removed as having the same value as a study title", res.rows_affected());

    // Remove identifiers that refer (mostly) to a person.

    let sql = r#"delete from ad.temp_idents i
            where id_value ilike '%dr.%'
        or id_value like '%Dr %'
        or id_value ilike '%prof.%'
        or id_value like '%prof %'
        or id_value like '%Prof %'
        or id_value ilike '%professor%'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} identifiers removed as referring to a person", res.rows_affected());
    
    // Get rid of all remaining identifiers that include only letters, spaces, and periods.
    // Though a few of these may be sponsor Ids the vast bulk are acronyms, the name of the 
    // sponsor or hospital, a short formn of the study name, or something undecipherable. 
    // They are not useful identifiers!  
    // Similar terms that include hyphens are retained as a much higher percentage of these
    // are possible sponsor ids.
    
    let sql = r#"delete from ad.temp_idents
        where id_value ~ '^[A-Za-z\s\.]+$'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} identifiers consisting only of letters, spaces and periods removed", res.rows_affected());		


    // Can now start matching against regular expresions representing trial registry formats.

    info!("");

    // EU CTR number

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'),
        id_source = 'secondary_id',
        id_type_id = 123,
        source_id = 100159,
        source = 'European Medicines Agency'
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
        id_type_id = 135,
        source_id = 100159,
        source = 'European Medicines Agency'
        where id_value ~ '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} EU CTIS identifiers found and labelled", res.rows_affected());	
    info!("");

    // WHO number 

    let sql = r#"update ad.temp_idents
        set id_value = 'U'||substring(id_value from '1111-[0-9]{4}-[0-9]{4}'),
        id_source = 'secondary_id',
        id_type_id = 115,
        source_id = 100114,
        source = 'World Health Organisation'
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
        id_type_id = 116,
        source_id = 100690,
        source = 'National Health and Medical Research Council, Australia'
        where id_value ~ 'ACTRN[0-9]{14}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} ACTRN Australian / NZ identifiers found and labelled", res.rows_affected());	
    info!("");

    // DRKS

    replace_string_in_ident("DRKS ID 0", "DRKS0", pool).await?;  // preliminary tidying (1 rec)
 
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'DRKS[0-9]{8}'),
        id_source = 'secondary_id',
        id_type_id = 124,
        source_id = 105875,
        source = 'Federal Institute for Drugs and Medical Devices'
        where id_value ~ 'DRKS[0-9]{8}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} DRKS German identifiers found and labelled", res.rows_affected());	
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
        id_type_id = 126,
        source_id = 101421,
        source = 'Springer Nature'
        where id_value ~ 'ISRCTN[0-9]{8}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} ISRCTN UK identifiers found and labelled", res.rows_affected());	
    info!("");


    // CTRI

    replace_string_in_ident("CTRI No. ", "", pool).await?;  

    let sql = r#"update ad.temp_idents
        set id_value = replace (substring(id_value from 'CTRI/20[0-9]{2}/[0-9]{2,3}/[0-9]{6}'), '/', '-'),
        id_source = 'secondary_id',
        id_type_id = 121,
        source_id = 102044,
        source = 'Indian Council of Medical Research'
        where id_value ~ 'CTRI/20[0-9]{2}/[0-9]{2,3}/[0-9]{6}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} CTRI Indian identifiers found and labelled", res.rows_affected());	
    info!("");


    // ChiCTR

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ChiCTR[0-9]{10}'),
        id_source = 'secondary_id',
        id_type_id = 118,
        source_id = 100494,
        source = 'West China Hospital"'
        where id_value ~ 'ChiCTR[0-9]{10}'"#;
    let res1 = utils::execute_sql(&sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ChiCTR-([A-Z]{3}|[A-Z]{4})-[0-9]{8}'),
        id_source = 'secondary_id',
        id_type_id = 118,
        source_id = 100494,
        source = 'West China Hospital"'
        where id_value ~ 'ChiCTR-([A-Z]{3}|[A-Z]{4})-[0-9]{8}'"#;
    let res2 = utils::execute_sql(&sql, pool).await?;
    info!("{} ChiCTR Chinese identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	
    info!("");


    // KCT 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'KCT[0-9]{7}'),
        id_source = 'secondary_id',
        id_type_id = 119,
        source_id = 0,
        source = 'Korea Disease Control and Prevention Agency '
        where id_value ~ 'KCT[0-9]{7}'
        and id_value !~ 'MKKCT[0-9]{7}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} KCT Korean identifiers found and labelled", res.rows_affected());	
    info!("");

    
    // RBR

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'RBR-[0-9a-z]{6,8}'),
        id_source = 'secondary_id',
        id_type_id = 117,
        source_id = 109251,
        source = 'Instituto Oswaldo Cruz'
        where id_value ~ 'RBR-[0-9a-z]{6,8}'"#;

    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} RBR Brazilian identifiers found and labelled", res.rows_affected());	
    info!("");


    // IRCT

    replace_string_in_ident("IRCT2020-", "IRCT2020", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'IRCT[0-9]{11,14}N[0-9]{1,2}'),
        id_source = 'secondary_id',
        id_type_id = 125,
        source_id = 0,
        source = 'Iranian Ministry of Health and Medical Education'
        where id_value ~ 'IRCT[0-9]{11,14}N[0-9]{1,2}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} IRCT Iranian identifiers found and labelled", res.rows_affected());	
    info!("");


    // JAPAN

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'C000[0-9]{6}'),
        id_source = 'secondary_id',
        id_type_id = 141,
        source_id = 100156,
        source = 'University Hospital Medical Information Network'
        where id_value ~ '^C000[0-9]{6}'
        or (id_value ~ 'C000[0-9]{6}' and id_value like '%UMIN%')"#;
    let res1 = utils::execute_sql(&sql, pool).await?;


    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'UMIN[0-9]{9}'),
        id_source = 'secondary_id',
        id_type_id = 141,
        source_id = 100156,
        source = 'University Hospital Medical Information Network'
        where id_value ~ 'UMIN[0-9]{9}'"#;
    let res2 = utils::execute_sql(&sql, pool).await?;


    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_type_desc from 'UMIN[0-9]{9}'),
        id_source = 'secondary_id',
        id_type_id = 141,
        source_id = 100156,
        source = 'University Hospital Medical Information Network'
        where id_type_desc ~ 'UMIN[0-9]{9}'"#;
    let res3 = utils::execute_sql(&sql, pool).await?;

    info!("{} UMIN japanese identifiers found and labelled", res1.rows_affected() + res2.rows_affected() + res3.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'jRCT[0-9]{10}'),
        id_source = 'secondary_id',
        id_type_id = 140,
        source_id = 0,
        source = 'Japan Registry of Clinical Trials'
        where id_value ~ 'jRCT[0-9]{10}'"#;
    let res1 = utils::execute_sql(&sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'jRCTs[0-9]{9}'),
        id_source = 'secondary_id',
        id_type_id = 140,
        source_id = 0,
        source = 'Japan Registry of Clinical Trials'
        where id_value ~ 'jRCTs[0-9]{9}'"#;
    let res2 = utils::execute_sql(&sql, pool).await?;

    info!("{} jCRT japanese identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

    let sql = r#"update ad.temp_idents
    set id_value = id_value||'-'||id_type_desc
    where id_value = 'JAPIC-CTI'"#;
    utils::execute_sql(&sql, pool).await?;

    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, 'JAPIC', 'Japic')
    where id_value like '%JAPIC%'"#;
    utils::execute_sql(&sql, pool).await?;

    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, 'JapicCTI- ', 'JapicCTI-')
    where id_value like '%JapicCTI- %'"#;
    utils::execute_sql(&sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'JapicCTI\-[0-9]{6}'),
        id_source = 'secondary_id',
        id_type_id = 139,
        source_id = 100157,
        source = 'Japan Pharmaceutical Information Center'
        where id_value ~ 'JapicCTI\-[0-9]{6}'"#;
    let res1 = utils::execute_sql(&sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'JapicCTI\-R[0-9]{6}'),
        id_source = 'secondary_id',
        id_type_id = 139,
        source_id = 100157,
        source = 'Japan Pharmaceutical Information Center'
        where id_value ~ 'JapicCTI\-R[0-9]{6}'"#;
    let res2 = utils::execute_sql(&sql, pool).await?;

    info!("{} JAPIC japanese identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'JMA-IIA[0-9]{5}'),
        id_source = 'secondary_id',
        id_type_id = 138,
        source_id = 100158,
        source = 'Japan Medical Association Center for Clinical Trials'
        where id_value ~ 'JMA-IIA[0-9]{5}'"#;
    let res = utils::execute_sql(&sql, pool).await?;

    info!("{} JMA japanese identifiers found and labelled", res.rows_affected());	

    
    info!("");

    
    // PACTR
    
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'PACTR[0-9]{15,16}'),
        id_source = 'secondary_id',
        id_type_id = 128,
        source_id = 0,
        source = 'Cochrane South Africa'
        where id_value ~ 'PACTR[0-9]{15,16}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} PACTR Pan African identifiers found and labelled", res.rows_affected());	
    info!("");


    // PERU

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'PER-[0-9]{3}-[0-9]{2}'),
        id_source = 'secondary_id',
        id_type_id = 129,
        source_id = 0,
        source = 'National Institute of Health, Peru'
        where id_value ~ '^PER-[0-9]{3}-[0-9]{2}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} PER Peruvian identifiers found and labelled", res.rows_affected());	
    info!("");


    // CUBA

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'RPCEC[0-9]{8}'),
        id_source = 'secondary_id',
        id_type_id = 122,
        source_id = 0,
        source = 'The National Coordinating Center of Clinical Trials, Cuba'
        where id_value ~ 'RPCEC[0-9]{8}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} RPCEC Cuban identifiers found and labelled", res.rows_affected());	
    info!("");


    // SRi-LANKA

    let sql = r#"update ad.temp_idents
        set id_value = replace(substring(id_value from 'SLCTR/20[0-9]{2}/[0-9]{3}'),  '/', '-'),
        id_source = 'secondary_id',
        id_type_id = 130,
        source_id = 0,
        source = 'Sri Lanka Medical Association'
        where id_value ~ 'SLCTR/20[0-9]{2}/[0-9]{3}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} SLCTR Sri Lankan identifiers found and labelled", res.rows_affected());	
    info!("");


    // THAI

    // TCTR20[0-9]{9}

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'TCTR20[0-9]{9}'),
        id_source = 'secondary_id',
        id_type_id = 131,
        source_id = 0,
        source = 'Central Research Ethics Committee, Thailand'
        where id_value ~ 'TCTR20[0-9]{9}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} TCTR Thai identifiers found and labelled", res.rows_affected());	
    info!("");

    // DUTCH

    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, ' ', '')
    where id_value ~ '^NTR [0-9]{1,4}'
    and id_source = 'secondary_id'"#;
    utils::execute_sql(&sql, pool).await?;

    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, '-', '')
    where id_value ~ '^NTR\-[0-9]{1,4}'
    and id_source = 'secondary_id'"#;
    utils::execute_sql(&sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NTR[0-9]{1,4}'),
        id_source = 'secondary_id',
        id_type_id = 132,
        source_id = 0,
        source = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NTR[0-9]{1,4}'
        and id_source = 'secondary_id'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} NTR Dutch identifiers found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL-OMON[0-9]{1,5}'),
        id_source = 'secondary_id',
        id_type_id = 132,
        source_id = 0,
        source = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ 'NL-OMON[0-9]{1,5}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} NL-OMON Dutch identifiers found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL[0-9]{4}'),
        id_source = 'secondary_id',
        id_type_id = 132,
        source_id = 0,
        source = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NL[0-9]{4}'
        and length(id_value) < 7
        and id_source = 'secondary_id'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} NL Dutch identifiers found and labelled", res.rows_affected());	
    info!("");


    // LEBANESE

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'LBCTR20[0-9]{8}'),
        id_source = 'secondary_id',
        id_type_id = 133,
        source_id = 0,
        source = 'Lebanese Ministry of Public Health'
        where id_value ~ 'LBCTR20[0-9]{8}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} LBCTR Lebanese identifiers found and labelled", res.rows_affected());	
    info!("");

    // ITMC 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ITMCTR[0-9]{10}'),
        id_source = 'secondary_id',
        id_type_id = 133,
        source_id = 0,
        source = 'Lebanese Ministry of Public Health'
        where id_value ~ 'ITMCTR[0-9]{10}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} ITMCTR Trad Medicine identifiers found and labelled", res.rows_affected());	
    info!("");


    // Hong Kong
    
    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, ' ', '')
    where id_value ilike '%HKUCTR%'"#;
    utils::execute_sql(&sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'HKUCTR-[0-9]{1,4}'),
        id_source = 'secondary_id',
        id_type_id = 156,
        source_id = 0,
        source = 'The University of Hong Kong'
        where id_value ~ 'HKUCTR-[0-9]{1,4}'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} HKUCTR Hong Kong identifiers found and labelled", res.rows_affected());	
    info!("");


   // Insert secondary ids found above

    let sql = r#"insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source)
                             select sd_sid, id_value, id_type_id, source_id, source
                             from ad.temp_idents 
                             where id_type_id is not null "#;   
    utils::execute_sql(&sql, pool).await?;

    let sql = r#"delete from ad.temp_idents 
                             where id_type_id is not null "#;   
    utils::execute_sql(&sql, pool).await?;

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