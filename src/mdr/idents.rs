
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

    // Create a copy of the ctgov identifier table (ad.temp_idents) - 
    // Working with this makes it much easier to develop and test routines 
    // to characterise the identifiers.

    create_copy_of_identifiers(max_id, chunk_size, pool).await?;

    // Using the temp_idents table, tidy up the identifier
    // text and remove duplicates and obvious non identifiers
    
    split_doubled_values(pool).await?;
    tidy_identifier_text(pool).await?;
    remove_obvious_non_identifiers(pool).await?;   // Doing this first simplifies the step below
    make_identifier_prefixes_more_consistent(pool).await?;
    make_identifier_text_more_consistent(pool).await?;
    remove_duplicate_identifiers(pool).await?;  // The data source contains about 11000 duplicated identifiers

    code_very_short_unidentifiable_ids(pool).await?; 
    transfer_coded_identifiers(pool).await?;

    /* 
    // identify old NCT aliases and NIH and other us agency identifiers

    find_nct_aliases(pool).await?;
    find_us_nci_identifiers(pool).await?;
    find_us_cdc_identifiers(pool).await?;
    find_nih_grant_identifiers(pool).await?;
    find_other_us_grant_identifiers(pool).await?;
    transfer_coded_identifiers(pool).await?;

    // Can now start matching against regular expresions representing trial registry formats.

    find_european_registry_identities(pool).await?;
    find_japanese_registry_identities(pool).await?;
    find_chinese_registry_identities(pool).await?;
    find_other_asian_registry_identities(pool).await?;
    find_middle_eastern_registry_identities(pool).await?;
    find_latin_american_registry_identities(pool).await?;
    find_other_registry_identities(pool).await?;
    transfer_coded_identifiers(pool).await?;

    // Find non registry Ids, e.g. from funders, regulators, registries

    //find_us_repository_identities(pool).await?;
    find_eu_regulators_identities(pool).await?;
    find_ethics_oversight_identities(pool).await?;
    find_other_identities(pool).await?;
    transfer_coded_identifiers(pool).await?;

    let _sql = r#"drop table if exists ad.temp_idents;"#;
    //utils::execute_sql(sql, pool).await?;

    utils::vacuum_table("study_identifiers", pool).await?;
*/
    Ok(())

}

    
async fn create_copy_of_identifiers(max_id: u64, chunk_size: u64, pool: &Pool<Postgres>) -> Result<(), AppError> { 

    let sql = r#"SET client_min_messages TO WARNING; 
	drop table if exists ad.temp_idents;
	create table ad.temp_idents
	(
          id                     INT             NOT NULL
		, sd_sid                 VARCHAR         NOT NULL
        , id_value               VARCHAR         NULL
        , id_source              VARCHAR         NULL
        , id_type_id             INT             NULL
        , id_type                VARCHAR         NULL
        , id_type_desc           VARCHAR         NULL
        , source_id              INT             NULL
        , source                 VARCHAR         NULL
        , link                   VARCHAR         NULL
	);
    CREATE INDEX temp_idents_id ON ad.temp_idents(id);
    CREATE INDEX temp_idents_sid ON ad.temp_idents(sd_sid);"#;
    utils::execute_sql(sql, pool).await?;

	let sql = r#"insert into ad.temp_idents (id, sd_sid, id_value, id_source, id_type, id_type_desc, link)
	select id, nct_id, id_value, id_source, id_type, id_type_description, id_link
	from ctgov.id_information c "#;
    utils::execute_phased_transfer(sql, max_id, chunk_size, " where ", "", pool).await?;

    let sql = r#"SET client_min_messages TO NOTICE;"#;
    utils::execute_sql(sql, pool).await?;
    
    Ok(())
}


async fn split_doubled_values(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // There seem to be about 720 doubled identifier records, with each
    // pair split by a semi-colon. Not all records with semi-colons are doubles,
    // and some doubled identifiers use a different delimiter, but the semi-colon seems
    // to be a reasonable starting point.

    // As an initial step, remove semi-colons from the start and end of identifiers.

    remove_both_ldtr_char_from_ident(';', pool).await?;

    // First though, a small group of identifiers including '(V/v)ersion' or 'v' followed by a number have a semi-colon 
    // before the date. In these case the semi-colon shopuld just be removed.

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, ';', '')
        where (id_value ilike '%version%'
        or id_value ~ 'v [0-9]' or id_value ~ '^v[0-9]'
        or id_value ~ 'v.[0-9]')
        and id_value ilike '%;%';"#;
    utils::execute_sql(sql, pool).await?;

    // There is a particular group of doubled identifiers that come from the 
    // Clinical Trials Unit in Basel. In each case the first id is a Swiss
    // BASEC (ethics system) number, and the second is an internal CTU
    // identifier. These should be botrh split and identified.
    
    let sql = r#"insert into ad.temp_idents
        (id, sd_sid, id_value, id_source, id_type, id_type_desc)
        select id, sd_sid, trim(substring(id_value, position(';' in id_value) + 1)), id_source, id_type, 'Basel CTU ID'
        from ad.temp_idents
        where id_value ~ '; ?[a-z]{2}(1|2)[0-9]{1}[A-Za-z1-5]+$'
        and id_value ~ '[0-9]{4}-[0-9]{5}';"#;
    utils::execute_sql(sql, pool).await?;

    let sql = r#"insert into ad.temp_idents
        (id, sd_sid, id_value, id_source, id_type, id_type_desc)
        select id, sd_sid, trim(substring(id_value, 1, position(';' in id_value) - 1)), id_source, id_type, 'BASEC ID'
        from ad.temp_idents
        where id_value ~ '; ?[a-z]{2}(1|2)[0-9]{1}[A-Za-z1-5]+$'
        and id_value ~ '[0-9]{4}-[0-9]{5}';"#;
    utils::execute_sql(sql, pool).await?;

    let sql = r#"delete from ad.temp_idents
        where id_value ~ '; ?[a-z]{2}(1|2)[0-9]{1}[A-Za-z1-5]+$'
        and id_value ~ '[0-9]{4}-[0-9]{5}';"#;
    let res1 = utils::execute_sql(sql, pool).await?;

    // There is a much smaller but similar group of paired records linked by a comma

    let sql = r#"insert into ad.temp_idents
        (id, sd_sid, id_value, id_source, id_type, id_type_desc)
        select id, sd_sid, trim(substring(id_value, position(',' in id_value) + 1)), id_source, id_type, 'Basel CTU ID'
        from ad.temp_idents
        where id_value ~ ', ?[a-z]{2}(1|2)[0-9]{1}[A-Za-z1-5]+$'
        and id_value ~ '[0-9]{4}-[0-9]{5}';"#;
    utils::execute_sql(sql, pool).await?;

    let sql = r#"insert into ad.temp_idents
        (id, sd_sid, id_value, id_source, id_type, id_type_desc)
        select id, sd_sid, trim(substring(id_value, 1, position(',' in id_value) - 1)), id_source, id_type, 'BASEC ID'
        from ad.temp_idents
        where id_value ~ ', ?[a-z]{2}(1|2)[0-9]{1}[A-Za-z1-5]+$'
        and id_value ~ '[0-9]{4}-[0-9]{5}';"#;
    utils::execute_sql(sql, pool).await?;

    let sql = r#"delete from ad.temp_idents
        where id_value ~ ', ?[a-z]{2}(1|2)[0-9]{1}[A-Za-z1-5]+$'
        and id_value ~ '[0-9]{4}-[0-9]{5}';"#;
    let res2 = utils::execute_sql(sql, pool).await?;

    info!("{} Basel CTU records with semi-colons or commas split", res1.rows_affected() + res2.rows_affected());

    // The remaining semi-colon containing identifiers can then be split and added
    // to the table.

    let sql = r#"insert into ad.temp_idents (id, sd_sid, id_value, id_source, id_type, id_type_desc)
        select id, sd_sid, trim(unnest(string_to_array(id_value, ';'))) as new_value, 
        id_source, id_type, id_type_desc
        from ad.temp_idents
        where id_value ilike '%;%';"#;
    utils::execute_sql(sql, pool).await?;

    let sql = r#"delete from ad.temp_idents 
        where id_value ilike '%;%'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} remaining records with semi-colons split", res.rows_affected());

    info!("");
    Ok(())
}


async fn tidy_identifier_text(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    // Remove single quotes from the beginning and end of identifiers.

    let sql = r#"update ad.temp_idents
        set id_value = trim(BOTH '''' from id_value)
        where id_value like '%''' or id_value like '''%'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} single quote characters removed from start or end of identifiers", res.rows_affected());

    // Tidy up the spurious characters to be found in the identifiers.

    remove_both_ldtr_char_from_ident('"', pool).await?;
    remove_both_ldtr_char_from_ident('-', pool).await?;
    remove_both_ldtr_char_from_ident(',', pool).await?;
    remove_both_ldtr_char_from_ident('.', pool).await?;
    remove_both_ldtr_char_from_ident('#', pool).await?;

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

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value, 2)
        where id_value ~ '^_';"#;
    utils::execute_sql(sql, pool).await?;
    info!("{} underscore characters removed from start of identifiers", res.rows_affected());
    info!("");

    // Seems to need to be done twice, albeit for a very small number of records.

    replace_string_in_ident("  ", " ", pool).await?;
    replace_string_in_ident("  ", " ", pool).await?;  

    // Final trim.

    let sql = r#"update ad.temp_idents
        set id_value = trim(id_value);"#;
    utils::execute_sql(sql, pool).await?;
    info!("");

    Ok(())
}


async fn remove_obvious_non_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // Remove those that are almost certainly dummies.

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
    info!("{} very short identifiers (1 or 2 characters) removed", res.rows_affected());

    // Remove those including '@' - e.g. mostly email addresses, plus some odd formulations

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
    
    // Some records have a digit in the id_type_desc field and only text (inc. spaces and punctuation)
    // in the id_value field. These are generally where the data in the two fields have been inverted
    // on data entry. These need to be swapped back.

    let sql = r#"update ad.temp_idents
        set id_value = id_type_desc,
        id_type_desc = id_value
        where id_value ~ '^[A-Za-z\s\.&,_/#-]+$'
        and id_type_desc ~ '[0-9]'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} identifiers and identifier descriptions reversed", res.rows_affected());

    // Some of the swapped group have a date in the id_value field and the type 'No NIH funding'.
    // These need to be removed as not an actual identifier.

    let sql = r#"delete from ad.temp_idents
        where id_type_desc = 'No NIH funding'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{}'No NIH funding' records deleted", res.rows_affected());

    // Get rid of all remaining identifiers that include only letters, spaces, and punctuation other than hyphens.
    // Though a few of these may be sponsor Ids the vast bulk are acronyms, the name of the 
    // sponsor or hospital, a short form of the study name, or something undecipherable. 
    // They are not useful identifiers!  
    // Similar terms that include hyphens are retained as a higher percentage of these
    // are possible sponsor ids.
    
    let sql = r#"delete from ad.temp_idents
        where id_value ~ '^[A-Za-z\s\.&,_/]+$'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} identifiers consisting only of letters, spaces, and certain punctuation removed", res.rows_affected());		

    // For future clarity remove id_type_desc where it is simply 
    // the same as the id_value

    let sql = r#"update ad.temp_idents
        set id_type_desc = null
        where id_type_desc = id_value"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} id types removed when they are identical to the id value", res.rows_affected());		
 
    // Some id_values are simply the trial acronym followed by ' trial', or ' Trial',
    // though there a few exceptions to this general rule, as shown by the sql below.
    // They were not deleted above because the trial name contains a hyphen.

    let sql = r#"delete from ad.temp_idents
        where (id_value ~ ' trial$' or  id_value ~ ' Trial$')
            and id_value !~ '^[0-9]'
            and id_value !~ '^U1111'
            and id_value !~ '^AGO'
            and id_value !~ '^WUCC'"#;
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} id types removed when they are simply the trial name followed by ' trial'", res.rows_affected());		
    info!("");


    Ok(())

}


async fn make_identifier_prefixes_more_consistent(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // Tries to make the use of the terms like 'number' consistent,
    // Replaces ID, no:, grant no, ref., etc. with the single term 'number'.
    // This is to make it easier to characterise some IDs later, but
    // further processing after these initial steps is done after somne of the 
    // registry IDs have been identiffie and removed (which significantly reduces the numbers involved).

    // Park the bus on these - will restore them later as they are nort easily interpretable

    let sql = r#"update ad.temp_idents
        set source = 'foo',
        link = id_value
        where id_value in ('No Grant-2', 'no Grant 4', 'No Grant 5', 'no Grant 6')"#;
    utils::execute_sql(sql, pool).await?;
   
    replace_string_in_ident("2024GRANT089", "2024 GRANT 089", pool).await?;
    replace_string_in_ident("BMAChartiableGrant2016", "BMA Charitable Grant 2016", pool).await?;
    info!("");

    replace_string_in_ident("Grant Fund", "grant ", pool).await?;
    replace_string_in_ident("Grant Agreement n.", "grant number ", pool).await?;
    replace_string_in_ident("Grant Agreement Nº:", "grant number ", pool).await?;
    replace_string_in_ident("Grant Agreement", "grant number ", pool).await?;
    replace_string_in_ident("Grant N.", "grant number ", pool).await?;
    replace_string_in_ident("Grant:", "grant ", pool).await?;
    replace_string_in_ident("Grant ID", "grant ", pool).await?;
    replace_string_in_ident("Grant-", "grant ", pool).await?;
    replace_string_in_ident("Grant ", "grant ", pool).await?;
    replace_string_in_ident("GRANT", "grant ", pool).await?;
    replace_string_in_ident("grant:", "grant number ", pool).await?;
    replace_string_in_ident("grant no", "grant number ", pool).await?;
    replace_string_in_ident("grant ", "grant number ", pool).await?;
    replace_string_in_ident("grant,", "grant number ", pool).await?;
    replace_string_in_ident("grant/", "grant number ", pool).await?;
    info!("");

    replace_string_in_ident("PROJECT NUMBER", "project number ", pool).await?;
    replace_string_in_ident("Project Number:", "project number ", pool).await?;
    replace_string_in_ident("Project Number", "project number ", pool).await?;
    replace_string_in_ident("PROJECT NO. ", "project number ", pool).await?;
    replace_string_in_ident("Project n.", "project number ", pool).await?;
    replace_string_in_ident("Project Nr.", "project number ", pool).await?;
    replace_string_in_ident("Project No-", "project number ", pool).await?;
    replace_string_in_ident("Project nº", "project number ", pool).await?;
    replace_string_in_ident("PROJECT NO ", "project number ", pool).await?;
    replace_string_in_ident("Project nr.", "project number ", pool).await?;
    replace_string_in_ident("Project nr", "project number ", pool).await?;
    replace_string_in_ident("project no", "project number ", pool).await?;
    replace_string_in_ident("projectnummer", "project number ", pool).await?;
    replace_string_in_ident("projectnr.", "project number ", pool).await?;
    replace_string_in_ident("projectnr", "project number ", pool).await?;
    replace_string_in_ident("project nr", "project number ", pool).await?;

    replace_string_in_ident("PROJECT00", "project number 00", pool).await?;
    replace_string_in_ident("Project Code:", "project number ", pool).await?;
    replace_string_in_ident("Project ID162820", "project number 162820", pool).await?;
    replace_string_in_ident("ProjectID ", "project number ", pool).await?;
    replace_string_in_ident("Project-ID ", "project number ", pool).await?;
    replace_string_in_ident("Project ID-", "project number ", pool).await?;
    replace_string_in_ident("PROJECT ID:", "project number ", pool).await?;
    replace_string_in_ident("Project id:", "project number ", pool).await?;
    replace_string_in_ident("Project ID:", "project number ", pool).await?;
    replace_string_in_ident("project ID:", "project number ", pool).await?;
    replace_string_in_ident("project-ID", "project number ", pool).await?;

    replace_string_in_ident("Study No", "study number", pool).await?;
    info!("");
    replace_string_in_ident("N°", "number ", pool).await?;
    replace_string_in_ident("n°", "number ", pool).await?;
    replace_string_in_ident(" id ", " number ", pool).await?;
    replace_string_in_ident(" ID ", " number ", pool).await?;

    replace_string_in_ident("Ref. No.", "number ", pool).await?;
    replace_string_in_ident("Ref. No ", "number ", pool).await?;

    replace_string_in_ident("No.", " number ", pool).await?;
    replace_string_in_ident("No:", " number ", pool).await?;
    replace_string_in_ident("No :", " number ", pool).await?;
    replace_string_in_ident("no.", " number ", pool).await?;
    replace_string_in_ident("no:", " number ", pool).await?;
    replace_string_in_ident("no :", " number ", pool).await?;
    replace_string_in_ident("NO :", " number ", pool).await?;
    replace_string_in_ident("NO:", " number ", pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, 'no', ' number ')
        where id_value ~ ' no [0-9]'
        or id_value ~ '^no [0-9]';"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} 'no 's folowed by a number replaced with 'number'", res.rows_affected());

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, 'No', ' number ')
        where id_value ~ ' No [0-9]'
        or id_value ~ '^No [0-9]';"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} 'No 's folowed by a number replaced with 'number'", res.rows_affected());
    info!("");

    replace_string_in_ident("#:", " number ", pool).await?;
    replace_string_in_ident("# :", " number ", pool).await?;
   
    // Almost all #s can be replaced by a 'number' unless that generates 
    // a split in a number, hi.e. hashes immediately preceded and followed by [0-9] 
    // can cause problems (there are 40-50 examples). It is not clear in these 
    // cases what the # signifies.

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, '#', ' number ')
        where id_value like '%#%'
        and id_value !~ '[0-9]#[0-9]';"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} '#'s not embedded in digits replaced with 'number'", res.rows_affected());
    info!("");
     
    replace_string_in_ident("Reference Number", "number ", pool).await?;
    replace_string_in_ident("Reference number", "number ", pool).await?;
    replace_string_in_ident("reference number", "number ", pool).await?;

    replace_string_in_ident("Reference", "number ", pool).await?;
    replace_string_in_ident("REFERENCE", "number ", pool).await?;
    replace_string_in_ident("reference", "number ", pool).await?;
    
    replace_string_in_ident("Ref Nº:", "number ", pool).await?;
    replace_string_in_ident("Ref nº", "number ", pool).await?;
    replace_string_in_ident("Ref No ", "number ", pool).await?;
    replace_string_in_ident("Ref.Nr.", "number ", pool).await?;
    replace_string_in_ident("Ref,", "number ", pool).await?;
    replace_string_in_ident("Ref-Nr:", "number ", pool).await?;
    replace_string_in_ident("Ref Code:", "number ", pool).await?;
    replace_string_in_ident("ref. number ", "number ", pool).await?;
    replace_string_in_ident("Ref. number ", "number ", pool).await?;
    replace_string_in_ident("Ref_ No_ ", "number ", pool).await?;

    replace_string_in_ident("Ref:", "number ", pool).await?;
    replace_string_in_ident("ref:", "number ", pool).await?;
    replace_string_in_ident("Ref.", "number ", pool).await?;
    replace_string_in_ident("ref.", "number ", pool).await?;
    replace_string_in_ident("REF:", "number ", pool).await?;
    replace_string_in_ident(" REF ", "number ", pool).await?;
    replace_string_in_ident(" ref ", "number ", pool).await?;
    replace_string_in_ident(" Ref ", "number ", pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, 'Ref ', '')
        where id_value ~ '^Ref ';"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} 'Initial 'Ref's removed", res.rows_affected());
    info!("");

    replace_string_in_ident("agreement", "", pool).await?;
    replace_string_in_ident("  ", " ", pool).await?;

    replace_string_in_ident("number -", " number ", pool).await?;
    replace_string_in_ident("number :", "number ", pool).await?;
    replace_string_in_ident("number:", "number ", pool).await?;
    replace_string_in_ident("number  number", " number ", pool).await?;
    replace_string_in_ident("number number", " number ", pool).await?;
    replace_string_in_ident("number Number", " number ", pool).await?;
    replace_string_in_ident("number NUMBER", " number ", pool).await?;

    // Need to repeat this...

    replace_string_in_ident("  ", " ", pool).await?;
    replace_string_in_ident("  ", " ", pool).await?;

    // repair from the top of the routine
    let sql = r#"update ad.temp_idents
        set source = null,
        id_value = link,
        link = null
        where source= 'foo'"#;
    utils::execute_sql(sql, pool).await?;

    // Final trim.

    let sql = r#"update ad.temp_idents
        set id_value = trim(id_value);"#;
    utils::execute_sql(sql, pool).await?;
    info!("");

    Ok(())
}


async fn make_identifier_text_more_consistent(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // Remove spurious 'number 's that simply prefix an identifier

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, 'number ', '')
        where id_value ~ '^number ';"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} Initial 'number's removed", res.rows_affected());
    
    // Create a table with the 'XXX type number' description, and the identifiers themselves, 
    // in separate fields, using those entries that now contain 'number' but which do not have
    // 'number' as the final word.

    let sql = r#"SET client_min_messages TO WARNING; 
            drop table if exists ad.temp_split_numbers;
            create table ad.temp_split_numbers as
            select id, id_value, 
            trim(substring(id_value, 1, POSITION('number' in id_value) +5 )) as value_type, 
            trim(substring(id_value, POSITION('number' in id_value) + 6)) as new_value
            from ad.temp_idents
            where id_value like '%number%'
            and id_value not like '%number'
            and id_value !~ '^[0-9]'
            and id_value !~ '^A[0-9]'
            order by id_value;"#;
    utils::execute_sql(sql, pool).await?;

    // Use that split_numbers table to update the temp_idents table.

    let sql = r#"update ad.temp_idents a
            set id_type_desc = case 
                when id_type_desc is null then s.value_type
                else id_type_desc||', '||s.value_type
                end,
            id_type = case
            when s.value_type ilike '% grant%' then 'OTHER_GRANT'
            when s.value_type ilike '%award%' then 'OTHER_GRANT'
            else 'OTHER' end,
            id_value = s.new_value
            from ad.temp_split_numbers s
            where a.id = s.id;
            drop table if exists ad.temp_split_numbers;"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} Identifier types transferred to id description from value", res.rows_affected());

    // Need to deal with those id values that end with 'number' - no easy way
    // but can simplify a little by first assuring they are all lower case.

    let sql: &'static str = r#"update ad.temp_idents
        set id_value = replace(id_value, 'Number', 'number')
        where id_value ~ 'Number$'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} Final 'Number's changed to lower case", res.rows_affected());


    switch_number_suffix_to_desc("UPenn IRB Protocol number", pool).await?;
    switch_number_suffix_to_desc("UPittsburgh IRB number", pool).await?;
    switch_number_suffix_to_desc("NIH grant number", pool).await?;
    switch_number_suffix_to_desc("Contract number", pool).await?;
    switch_number_suffix_to_desc("IDRCB number", pool).await?;
    switch_number_suffix_to_desc("UCSD number", pool).await?;
    switch_number_suffix_to_desc("REC number", pool).await?;
    switch_number_suffix_to_desc("REK number", pool).await?;
    switch_number_suffix_to_desc("Station number", pool).await?;
    switch_number_suffix_to_desc("EUDRACT number", pool).await?;
    switch_number_suffix_to_desc("EU CT number", pool).await?;
    switch_number_suffix_to_desc("EudraCT-number", pool).await?;
    switch_number_suffix_to_desc("EudraCT number", pool).await?;
    switch_number_suffix_to_desc("Eudra CT number", pool).await?;
    switch_number_suffix_to_desc("Scripps SOPRS number", pool).await?;
    switch_number_suffix_to_desc("Study number", pool).await?;
    switch_number_suffix_to_desc("Logan IRB number", pool).await?;
    switch_number_suffix_to_desc("Award number", pool).await?;
    switch_number_suffix_to_desc("Sponsor number", pool).await?;
    switch_number_suffix_to_desc("Institution number", pool).await?;
    switch_number_suffix_to_desc("VA IRB number", pool).await?;
    switch_number_suffix_to_desc("Chilean number", pool).await?;
    switch_number_suffix_to_desc("Clinical Trial number", pool).await?;
    switch_number_suffix_to_desc("IMI WP8A number", pool).await?;
    switch_number_suffix_to_desc("UCSF RAS number", pool).await?;
    switch_number_suffix_to_desc("no UCI HS number", pool).await?;
    switch_number_suffix_to_desc("UHN REB number", pool).await?;
    switch_number_suffix_to_desc("CR number", pool).await?;
    switch_number_suffix_to_desc("IRB number", pool).await?;
    switch_number_suffix_to_desc("Hospice number", pool).await?;
    switch_number_suffix_to_desc("Protocol number", pool).await?;
    switch_number_suffix_to_desc("protocol number", pool).await?;
    switch_number_suffix_to_desc("grant number", pool).await?;

    switch_number_suffix_to_desc("MBW-number", pool).await?;
    switch_number_suffix_to_desc("NIAID CRMS ID number", pool).await?;
    switch_number_suffix_to_desc("DDX exemption from MRHA number", pool).await?;
    switch_number_suffix_to_desc("number", pool).await?;
    
    // the last two are odd in that they have an empty Version number suffix, which can be removed

    let sql: &'static str = r#"update ad.temp_idents
        set id_value = replace(id_value, ', Version', '')
        where id_value ~ ', Version$'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} Empty version numbers removed", res.rows_affected());

    // Repeat removal of spurious 'number 's that simply prefix an identifier

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, 'number ', '')
        where id_value ~ '^number ';"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} Initial 'number's removed", res.rows_affected());
   
    // Remove a few additional records that now don't have values (or meaningful values).

    let sql = r#"delete from ad.temp_idents i
        where id_value is null
        or id_value = ''
        or id_value ~ '^[0-]+$';"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} Additional empty or meaningless identifier records removed", res.rows_affected());
    
    // clean up beginnings

    remove_leading_char_from_ident('.', pool).await?;
    remove_leading_char_from_ident(':', pool).await?;
    

    // Final trim.

    let sql = r#"update ad.temp_idents
        set id_value = trim(id_value);"#;
    utils::execute_sql(sql, pool).await?;

    info!("");
    Ok(())
}


async fn remove_duplicate_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 

    // Identify duplicated (study id + identifier)s, and link them to the 
    // full identifier records in temp_idents, creating temp_id_dups.
    
    let sql = r#"SET client_min_messages TO WARNING; 
     drop table if exists ad.temp_id_dups;
     create table ad.temp_id_dups as 
        select a.* 
        from ad.temp_idents a
        inner join
            (select sd_sid, id_value
            from ad.temp_idents c
            group by sd_sid, id_value
            having count(id) > 1) d
        on a.sd_sid = d.sd_sid
        and a.id_value = d.id_value
        order by sd_sid, id_value;"#;
    utils::execute_sql(sql, pool).await?;

    // Create a table of the minimum id records for each duplicate record
    // (Some are duplicated more than once).

    let sql = r#"SET client_min_messages TO WARNING; 
        drop table if exists ad.temp_id_dup_mins;
        create table ad.temp_id_dup_mins as 
        select sd_sid, id_value, min(id) as min_id 
        from ad.temp_id_dups 
        group by sd_sid, id_value;"#;
    utils::execute_sql(sql, pool).await?;

    // Use the minimum table to remove the minimum id version of each 
    // duplicated record from temp_id_dups.

    let sql = r#"delete from ad.temp_id_dups d
        using ad.temp_id_dup_mins m
        where d.id = m.min_id;"#;
    utils::execute_sql(sql, pool).await?;

    // Use the remaining temp_id_dups records to delete the 
    // spurious records from temp_idents, leaving only the 
    // version with the minimum id in the table.

    let sql = r#"delete from ad.temp_idents c
        using ad.temp_id_dups d
        where c.id = d.id"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} duplicated identifiers removed", res.rows_affected());

    let sql = r#"drop table if exists ad.temp_id_dup_mins;
        drop table if exists ad.temp_id_dups;
        SET client_min_messages TO NOTICE;"#;
    utils::execute_sql(sql, pool).await?;

    // Also remove those records where the identifier is the same as the NCT id.

    let sql = r#"delete from ad.temp_idents 
        where id_value = sd_sid"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} records deleted where identifier = NCT Id", res.rows_affected());

    info!("");
    Ok(())
}


async fn code_very_short_unidentifiable_ids(pool: &Pool<Postgres>) -> Result<(), AppError> { 

    // There are many small identifiers, that if given without any further information,
    // are impossible to characterise as they could have a wide variety of sources.
    // These are categorised as 'unknown' so that can thwm be removed from the temp_idents table,
    // though they ae transferred to study_identifiers. This is just to make later
    // development and processing easier and quicker.
   
    let sql = r#"update ad.temp_idents
        set id_type_id = 990,
        source_id = null,
        source = null
        where id_value ~ '^[0-9]+$'
        and length (id_value) < 7
        and id_type_desc is null"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} IDs with just numbers (length < 7) labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_type_id = 990,
        source_id = null,
        source = null
        where id_value ~ '^[0-9\-]+$'
        and id_value !~ '^[0-9]+$'
        and length (id_value) < 6
         and id_type_desc is null"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} IDs with just numbers and hyphens (length < 6) labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
    
        set id_type_id = 990,
        source_id = null,
        source = null
        where id_value ~ '^[A-Za-z0-9\-]+$'
        and id_value !~ '^[0-9\-]+$'
        and length (id_value) < 5
        and id_type_desc is null"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} IDs with with length < 5 labelled", res.rows_affected());	

    info!("");
    Ok(())
}

async fn find_nct_aliases(pool: &Pool<Postgres>) -> Result<(), AppError> { 

    // Insert the old 'alias' ids that were initially used for 3285 studies.
   
    let sql = r#"update ad.temp_idents
        set id_source = 'nct_alias',
        id_type_id = 180,
        source_id = 100133,
        source = 'National Library of Medicine'
        where id_source = 'nct_alias' "#;

    let res = utils::execute_sql(sql, pool).await?;
    info!("{} old NCT aliases found and labelled", res.rows_affected());	

    info!("");
    Ok(())
}


async fn find_us_nci_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 
       
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NCI-20[0-9]{2}-[0-9]{2}'),
        id_source = 'system_id',
        id_type_id = 170,
        source_id = 100162,
        source = 'National Cancer Institute'
        where id_value ~ 'NCI-20[0-9]{2}-[0-9]{2}'"#;

    let res = utils::execute_sql(sql, pool).await?;
    info!("{} NCI CTRP identifiers found and labelled", res.rows_affected());	
   
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'CDR[0-9]{10}'),
        id_source = 'system_id',
        id_type_id = 174,
        source_id = 100162,
        source = 'National Cancer Institute'
        where id_value ~ 'CDR[0-9]{10}'"#;

    let res = utils::execute_sql(sql, pool).await?;
    info!("{} CDR NCI PDQ identifiers found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_source = 'system_id',
        id_type_id = 175,
        source_id = 100162,
        source = 'National Cancer Institute'
        where id_type_desc ~ 'CTEP'"#;

    let res = utils::execute_sql(sql, pool).await?;
    info!("{} NCI CTEP identifiers found and labelled", res.rows_affected());	


    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 407,
        source_id = 100162,
        source = 'National Cancer Institute'
        where (id_type_desc in ('NCI', 'National Cancer Institute')
            or id_value ~ 'NCI-[0-9A-Z]{3}-[0-9A-Z]{3,5}'
            or id_value ~ '^REBACCC')
            and id_type_id is null
            and id_type= 'OTHER_GRANT'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} NCI grant Ids found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_source = 'other_id',
        id_type_id = 178,
        source_id = 100162,
        source = 'National Cancer Institute'
        where (id_type_desc in ('NCI', 'National Cancer Institute')
            or id_value ~ 'NCI-[0-9A-Z]{3}-[0-9A-Z]{3,5}'
            or id_value ~ '^REBACCC')
            and id_type_id is null
            and (id_type is null or id_type <> 'OTHER_GRANT')"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} Other NCI Ids found and labelled", res.rows_affected());	
    
    info!("");
    Ok(())
}


async fn find_us_cdc_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 
  
    replace_string_in_ident("CDC - N", "CDC-N", pool).await?;  
    replace_string_in_ident("CDC N", "CDC-N", pool).await?;  
    replace_string_in_ident("CDC IRB", "CDC-IRB", pool).await?;  

    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 406,
        source_id = 100245,
        source = 'Centers for Disease Control and Prevention'
        where (id_type_desc in ('CDC', 'CDC, USA', 'CDC grant number')
            or (id_type_desc ~ 'Centers for Disease Control' and id_type_desc not ilike '%Taiwan%')
            or id_type_desc ilike 'Center for Disease Control%'
            or id_type_desc ilike 'CDC Cooperative Agreement%'
            or id_value ilike 'CDC-G%'
            or id_value ilike 'CDC-N%'
            or id_value ilike 'CDC-O%'
            or id_value ilike 'CDC %')
            and id_type= 'OTHER_GRANT'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} CDC grant Ids found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_source = 'other_id',
        id_type_id = 177,
        source_id = 100245,
        source = 'Centers for Disease Control and Prevention'
        where ((id_type_desc ~ 'Centers for Disease Control' and id_type_desc not ilike '%Taiwan%')
            or id_type_desc in ('CDC', 'CDC, USA', 'CDC number', 'CDC Protocol number',
            'CDC, USA, CDC IRB Protocol number')
            or id_value ilike 'CDC-G%'
            or id_value ilike 'CDC-N%'
            or id_value ilike 'CDC-O%'
            or id_value ilike 'CDC %')
            and (id_type is null or id_type <> 'OTHER_GRANT')"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} Other CDC Ids found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}

/*
async fn find_nih_grant_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 

    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 401,
        source_id = 100134,
        source = 'National Institutes of Health'
        where id_type = 'NIH'"#;

    let res1 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 401,
        source_id = 100134,
        source = 'National Institutes of Health'
        where id_type_desc in ('Federal Funding, NIH', 'NIH Contract', 'NIH Contract Number', 'US NIH Contract Number',
        'NIH grant number', 'NIH contract')
        or id_type_desc ilike 'US NIH Grant%'
        or id_type_desc ilike 'U.S. NIH Grant%'
        or (id_type = 'OTHER_GRANT' 
        and id_type_desc in ('nih', 'NIH', 'National Institutes of Health (NIH)', 'US NIH'))"#;

    let res2 = utils::execute_sql(sql, pool).await?;

    // attempts to get similar ids to the NIH ones created immediately above,
    // where they do not have type_id_descriptions indicating NIH

    let sql = r#"update ad.temp_idents a
        set id_source = 'grant_id',
        id_type_id = 401,
        source_id = 100134,
        source = 'National Institutes of Health'
    from
        (select substring(id_value, 1, 4) as pref, count(id) from ad.temp_idents
            where id_type_id = 401
            group by substring(id_value, 1, 4)
            having count(id) >= 40) p
    where substring(a.id_value, 1, 4) = p.pref
    and a.id_type_id is null; "#;

    let res3 = utils::execute_sql(sql, pool).await?;
    info!("{} NIH grant identifiers found and labelled", res1.rows_affected() + res2.rows_affected() + res3.rows_affected());	
    info!("");

    Ok(())
}


async fn find_other_us_grant_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 
      
    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 402,
        source_id = 100407,
        source = 'Agency for Health Research and Quality'
        where id_type = 'AHRQ'
        or id_type_desc ilike '%AHRQ%'
        or id_type_desc ilike '%Research Quality%'"#;

    let res = utils::execute_sql(sql, pool).await?;
    info!("{} AHRQ grant identifiers found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 403,
        source_id = 108548,
        source = 'Food and Drug Administration'
        where id_type = 'FDA'"#;

    let res1 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 403,
        source_id = 108548,
        source = 'Food and Drug Administration'
        where id_type = 'OTHER_GRANT' 
        and id_type_desc in ('fda', 'FDA', 'Food and Drug Administration', 
        'US FDA', 'USFDA', 'United States FDA', 'U.S. Food and Drug Administration',
        'US Food and Drug Administration', 'US FOOD AND DRUG ADMN')
        and id_value not ilike '%nih%'"#;

    let res2 = utils::execute_sql(sql, pool).await?;
    info!("{} FDA grant identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 404,
        source_id = 108270,
        source = 'Substance Abuse and Mental Health Services Administration'
        where id_type = 'SAMHSA'"#;
    let res1 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 404,
        source_id = 108270,
        source = 'Substance Abuse and Mental Health Services Administration'
        where id_type = 'OTHER_GRANT' 
        and (id_type_desc ilike '%SAMHSA%'
        or id_type_desc ilike '%substance abuse and mental health%')"#;

    let res2 = utils::execute_sql(sql, pool).await?;
    info!("{} SAMHSA grant identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	
    
    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 405,
        source_id = 101872,
        source = 'US Department of Defense'
        where id_value ilike 'W81XWH%'
        or id_value ilike 'CDMRP%'
        or id_value ilike 'HT9425%'"#;

    let res1 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_source = 'grant_id',
        id_type_id = 405,
        source_id = 101872,
        source = 'US Department of Defense'
        where (id_type_desc ilike '%department of defense%'
        or id_type_desc ilike '%dept of defense%'
        or id_type_desc ilike '%dod%')
        and id_type_id is null"#;

    let res2 = utils::execute_sql(sql, pool).await?;
    info!("{} Department of Defense grant identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

    info!("");
    Ok(())
}


async fn find_european_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // EU CTR number

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'),
        id_source = 'registry_id',
        id_type_id = 123,
        source_id = 100159,
        source = 'European Medicines Agency'
        where id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}' 
        and (
        length(id_value) = 14
        or id_value ilike '%eu%'
        or id_value ilike '%udract%'
        or id_value ilike '%edract%')"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} EU CTR identifiers found and labelled", res.rows_affected());	

    // CTIS number

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'),
        id_source = 'registry_id',
        id_type_id = 135,
        source_id = 100159,
        source = 'European Medicines Agency'
        where id_value ~ '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} EU CTIS identifiers found and labelled", res.rows_affected());	
    info!("");

    // DRKS

    replace_string_in_ident("DRKS ID 0", "DRKS0", pool).await?;  // preliminary tidying (1 rec)
 
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'DRKS[0-9]{8}'),
        id_source = 'registry_id',
        id_type_id = 124,
        source_id = 105875,
        source = 'Federal Institute for Drugs and Medical Devices'
        where id_value ~ 'DRKS[0-9]{8}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} DRKS German identifiers found and labelled", res.rows_affected());	
    info!("");

    // DUTCH

    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, ' ', '')
    where id_value ~ '^NTR [0-9]{1,4}'
    and id_source = 'secondary_id'"#;
    utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, '-', '')
    where id_value ~ '^NTR\-[0-9]{1,4}'
    and id_source = 'secondary_id'"#;
    utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NTR[0-9]{1,4}'),
        id_source = 'registry_id',
        id_type_id = 181,
        source_id = 0,
        source = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NTR[0-9]{1,4}'
        and id_source = 'secondary_id'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} NTR Dutch identifiers found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL[0-9]{4}'),
        id_source = 'registry_id',
        id_type_id = 182,
        source_id = 0,
        source = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NL[0-9]{4}'
        and length(id_value) < 7
        and id_source = 'secondary_id'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} NL Dutch identifiers found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL-OMON[0-9]{1,5}'),
        id_source = 'registry_id',
        id_type_id = 132,
        source_id = 0,
        source = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ 'NL-OMON[0-9]{1,5}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} NL-OMON Dutch identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


async fn find_japanese_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'C000[0-9]{6}'),
        id_source = 'registry_id',
        id_type_id = 141,
        source_id = 100156,
        source = 'University Hospital Medical Information Network'
        where id_value ~ '^C000[0-9]{6}'
        or (id_value ~ 'C000[0-9]{6}' and id_value like '%UMIN%')"#;
    let res1 = utils::execute_sql(sql, pool).await?;

    replace_string_in_ident("UMIN 0", "UMIN0", pool).await?;

    // UMIN Ids present in both the id_value and the id_type_desc fields

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'UMIN[0-9]{9}'),
        id_source = 'registry_id',
        id_type_id = 141,
        source_id = 100156,
        source = 'University Hospital Medical Information Network'
        where id_value ~ 'UMIN[0-9]{9}'"#;
    let res2 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_type_desc from 'UMIN[0-9]{9}'),
        id_source = 'registry_id',
        id_type_id = 141,
        source_id = 100156,
        source = 'University Hospital Medical Information Network'
        where id_type_desc ~ 'UMIN[0-9]{9}'"#;
    let res3 = utils::execute_sql(sql, pool).await?;

    info!("{} UMIN japanese identifiers found and labelled", res1.rows_affected() + res2.rows_affected() + res3.rows_affected());	

    replace_string_in_ident("jRCT ", "jRCT", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'jRCT[0-9]{10}'),
        id_source = 'registry_id',
        id_type_id = 140,
        source_id = 0,
        source = 'Japan Registry of Clinical Trials'
        where id_value ~ 'jRCT[0-9]{10}'"#;
    let res1 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'jRCTs[0-9]{9}'),
        id_source = 'registry_id',
        id_type_id = 140,
        source_id = 0,
        source = 'Japan Registry of Clinical Trials'
        where id_value ~ 'jRCTs[0-9]{9}'"#;
    let res2 = utils::execute_sql(sql, pool).await?;

    info!("{} jCRT japanese identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

    replace_string_in_ident("JAPIC", "Japic", pool).await?; 
    replace_string_in_ident("JapicCTI- ", "JapicCTI-", pool).await?; 
    replace_string_in_ident("Japic CTI-", "JapicCTI-", pool).await?; 
    replace_string_in_ident("JapicCTI0", "JapicCTI-0", pool).await?; 
    replace_string_in_ident("JapicCTI-22-", "JapicCTI-22", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'JapicCTI-[0-9]{6}'),
        id_source = 'registry_id',
        id_type_id = 139,
        source_id = 100157,
        source = 'Japan Pharmaceutical Information Center'
        where id_value ~ 'JapicCTI-[0-9]{6}'"#;
    let res1 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'JapicCTI-R[0-9]{6}'),
        id_source = 'registry_id',
        id_type_id = 139,
        source_id = 100157,
        source = 'Japan Pharmaceutical Information Center'
        where id_value ~ 'JapicCTI-R[0-9]{6}'"#;
    let res2 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'JapicCTI-'||substring(id_value from '[0-9]{6}') 
        where id_type_desc ilike '%JAPIC%'
        and id_value ~ '[0-9]{6}'
        and id_type_id is null"#;
    utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_source = 'registry_id',
        id_type_id = 139,
        source_id = 100157,
        source = 'Japan Pharmaceutical Information Center'
        where id_value ~ 'JapicCTI-[0-9]{6}'
        and id_type_id is null"#;
    let res4 = utils::execute_sql(sql, pool).await?;

    info!("{} JAPIC japanese identifiers found and labelled", res1.rows_affected() + res2.rows_affected() + res4.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'JMA-IIA[0-9]{5}'),
        id_source = 'registry_id',
        id_type_id = 138,
        source_id = 100158,
        source = 'Japan Medical Association Center for Clinical Trials'
        where id_value ~ 'JMA-IIA[0-9]{5}'"#;
    let res = utils::execute_sql(sql, pool).await?;

    info!("{} JMA japanese identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


async fn find_chinese_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // ChiCTR

    replace_string_in_ident("chiCTR", "ChiCTR", pool).await?; 
    replace_string_in_ident("CHiCTR", "ChiCTR", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ChiCTR[0-9]{10}'),
        id_source = 'registry_id',
        id_type_id = 118,
        source_id = 100494,
        source = 'West China Hospital"'
        where id_value ~ 'ChiCTR[0-9]{10}'"#;
    let res1 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ChiCTR-[A-Z]{3,5}-[0-9]{8}'),
        id_source = 'registry_id',
        id_type_id = 118,
        source_id = 100494,
        source = 'West China Hospital"'
        where id_value ~ 'ChiCTR-[A-Z]{3,5}-[0-9]{8}'"#;
    let res2 = utils::execute_sql(sql, pool).await?;
    info!("{} ChiCTR Chinese identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

   // ITMC 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ITMCTR[0-9]{10}'),
        id_source = 'registry_id',
        id_type_id = 133,
        source_id = 0,
        source = 'Lebanese Ministry of Public Health'
        where id_value ~ 'ITMCTR[0-9]{10}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} ITMCTR Trad Medicine identifiers found and labelled", res.rows_affected());	

    // Hong Kong
    
    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, ' ', '')
    where id_value ilike '%HKUCTR%'"#;
    utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'HKUCTR-[0-9]{1,4}'),
        id_source = 'registry_id',
        id_type_id = 156,
        source_id = 0,
        source = 'The University of Hong Kong'
        where id_value ~ 'HKUCTR-[0-9]{1,4}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} HKUCTR Hong Kong identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


async fn find_other_asian_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // CTRI

    let sql = r#"update ad.temp_idents
        set id_value = replace (substring(id_value from 'CTRI/20[0-9]{2}/[0-9]{2,3}/[0-9]{6}'), '/', '-'),
        id_source = 'registry_id',
        id_type_id = 121,
        source_id = 102044,
        source = 'Indian Council of Medical Research'
        where id_value ~ 'CTRI/20[0-9]{2}/[0-9]{2,3}/[0-9]{6}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} CTRI Indian identifiers found and labelled", res.rows_affected());	

    // SRi-LANKA

    replace_string_in_ident("SLCTR/ ", "SLCTR/", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = replace(substring(id_value from 'SLCTR/20[0-9]{2}/[0-9]{3}'),  '/', '-'),
        id_source = 'registry_id',
        id_type_id = 130,
        source_id = 0,
        source = 'Sri Lanka Medical Association'
        where id_value ~ 'SLCTR/20[0-9]{2}/[0-9]{3}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} SLCTR Sri Lankan identifiers found and labelled", res.rows_affected());	

    // KCT 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'KCT[0-9]{7}'),
        id_source = 'registry_id',
        id_type_id = 119,
        source_id = 0,
        source = 'Korea Disease Control and Prevention Agency '
        where id_value ~ 'KCT[0-9]{7}'
        and id_value !~ 'MKKCT[0-9]{7}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} KCT Korean identifiers found and labelled", res.rows_affected());	

    // THAI

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'TCTR20[0-9]{9}'),
        id_source = 'registry_id',
        id_type_id = 131,
        source_id = 0,
        source = 'Central Research Ethics Committee, Thailand'
        where id_value ~ 'TCTR20[0-9]{9}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} TCTR Thai identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


async fn find_middle_eastern_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // IRCT

    replace_string_in_ident("IRCT2020-", "IRCT2020", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'IRCT[0-9]{11,14}N[0-9]{1,2}'),
        id_source = 'registry_id',
        id_type_id = 125,
        source_id = 0,
        source = 'Iranian Ministry of Health and Medical Education'
        where id_value ~ 'IRCT[0-9]{11,14}N[0-9]{1,2}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} IRCT Iranian identifiers found and labelled", res.rows_affected());	

    // LEBANESE

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'LBCTR20[0-9]{8}'),
        id_source = 'registry_id',
        id_type_id = 133,
        source_id = 0,
        source = 'Lebanese Ministry of Public Health'
        where id_value ~ 'LBCTR20[0-9]{8}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} LBCTR Lebanese identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


async fn find_latin_american_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // RBR

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'RBR-[0-9a-z]{6,8}'),
        id_source = 'registry_id',
        id_type_id = 117,
        source_id = 109251,
        source = 'Instituto Oswaldo Cruz'
        where id_value ~ 'RBR-[0-9a-z]{6,8}'"#;

    let res = utils::execute_sql(sql, pool).await?;
    info!("{} RBR Brazilian identifiers found and labelled", res.rows_affected());	

    // PERU

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'PER-[0-9]{3}-[0-9]{2}'),
        id_source = 'registry_id',
        id_type_id = 129,
        source_id = 0,
        source = 'National Institute of Health, Peru'
        where id_value ~ '^PER-[0-9]{3}-[0-9]{2}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} PER Peruvian identifiers found and labelled", res.rows_affected());	

    // CUBA

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'RPCEC[0-9]{8}'),
        id_source = 'registry_id',
        id_type_id = 122,
        source_id = 0,
        source = 'The National Coordinating Center of Clinical Trials, Cuba'
        where id_value ~ 'RPCEC[0-9]{8}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} RPCEC Cuban identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


async fn find_other_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // WHO number 

    let sql = r#"update ad.temp_idents
        set id_value = 'U'||substring(id_value from '1111-[0-9]{4}-[0-9]{4}'),
        id_source = 'registry_id',
        id_type_id = 115,
        source_id = 100114,
        source = 'World Health Organisation'
        where id_value ~ '1111-[0-9]{4}-[0-9]{4}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} WHO U identifiers found and labelled", res.rows_affected());	
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
        id_source = 'registry_id',
        id_type_id = 126,
        source_id = 101421,
        source = 'Springer Nature'
        where id_value ~ 'ISRCTN[0-9]{8}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} ISRCTN UK identifiers found and labelled", res.rows_affected());	
    info!("");

    // ACTRN

    replace_string_in_ident("ACTRN0", "ACTRN", pool).await?;  // preliminary tidying
    replace_string_in_ident("ACTRNO", "ACTRN", pool).await?;  // of a few records

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ACTRN[0-9]{14}'),
        id_source = 'registry_id',
        id_type_id = 116,
        source_id = 100690,
        source = 'National Health and Medical Research Council, Australia'
        where id_value ~ 'ACTRN[0-9]{14}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} ACTRN Australian / NZ identifiers found and labelled", res.rows_affected());	
    info!("");

    // PACTR
    
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'PACTR[0-9]{15,16}'),
        id_source = 'registry_id',
        id_type_id = 128,
        source_id = 0,
        source = 'Cochrane South Africa'
        where id_value ~ 'PACTR[0-9]{15,16}'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} PACTR Pan African identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


async fn find_eu_regulators_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'),
        id_source = 'regulator_id',
        id_type_id = 301,
        source_id = 101408,
        source = 'Agence Nationale de Sécurité du Médicament'
        where id_value ~ '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'
        and (id_type_desc is null or id_type_desc not ilike 'AbbVie')"#;

    let res1 = utils::execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_type_desc from '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'),
        id_source = 'regulator_id',
        id_type_id = 301,
        source_id = 101408,
        source = 'Agence Nationale de Sécurité du Médicament'
        where id_type_desc ~ '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'"#;

    let res2 = utils::execute_sql(sql, pool).await?;
    info!("{} ANSM (ID-RCB) identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	
   
    info!("");
    Ok(())


}


async fn find_ethics_oversight_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    Ok(())
}


async fn find_other_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

     replace_string_in_ident("EORTC ", "EORTC-", pool).await?;  
    
    let sql = r#"update ad.temp_idents
        set id_source = 'sponsor_id',
        id_type_id = 176,
        source_id = 100010,
        source = 'EORTC'
        where id_value ~ '^EORTC'"#;
    let res1 = utils::execute_sql(sql, pool).await?;
    
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'EORTC-[0-9]{4,5}'),
        id_source = 'sponsor_id',
        id_type_id = 176,
        source_id = 100010,
        source = 'EORTC'
        where id_value ~ 'EORTC-[0-9]{4,5}'
        and id_type_id is null"#;
    let res2 = utils::execute_sql(sql, pool).await?;

    info!("{} EORTC identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_source = 'funder_id',
        id_type_id = 410,
        source_id = 100517,
        source = 'Cancer Research UK'
        where id_value ~ '^CRUK'"#;
    let res = utils::execute_sql(sql, pool).await?;
    info!("{} CRUK identifiers found and labelled", res.rows_affected());	
    info!("");
    Ok(())
}

*/
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

async fn switch_number_suffix_to_desc(s: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {  
let sql = format!(r#"update ad.temp_idents
            set id_type_desc = case 
                when id_type_desc is null then '{s}'
                else id_type_desc||', '||'{s}'
                end,
            id_value = trim(replace (id_value, '{s}', ''))
            where id_value ~ '{s}$'"#);
    let res = utils::execute_sql(&sql, pool).await?;
    info!("{} '{}' suffix(es) from id_value to id type description", res.rows_affected(), s);
    Ok(())
}


async fn transfer_coded_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"insert into ad.study_identifiers (sd_sid, identifier_value, identifier_type_id, source_id, source, identifier_link)
                             select sd_sid, id_value, id_type_id, source_id, source, link 
                             from ad.temp_idents 
                             where id_type_id is not null "#;   
    utils::execute_sql(sql, pool).await?;

    let sql = r#"delete from ad.temp_idents 
                             where id_type_id is not null "#;   
    utils::execute_sql(sql, pool).await?;

    Ok(())
}
