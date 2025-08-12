use super::utils::{execute_sql};
use super::idents_utils::{replace_string_in_ident, execute_sql_fb};

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;


pub async fn find_us_nci_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 
       
    // NCI CTRP

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NCI-20[0-9]{2}-[0-9]{5}'),
        id_type_id = 174,
        id_type = 'NCI CTRP ID',
        source_org_id = 100162,
        source_org = 'National Cancer Institute'
        where id_value ~ 'NCI-20[0-9]{2}-[0-9]{5}' "#;
    execute_sql_fb(sql, pool, "NCI CTRP", "found and labelled").await?;

    // PDQ

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'CDR[0-9]{9,10}'),
        id_type_id = 170,
        id_type = 'NCI PDQ ID',
        source_org_id = 100162,
        source_org = 'National Cancer Institute'
        where id_value ~ 'CDR[0-9]{9,10}'"#;
    execute_sql_fb(sql, pool, "CDR NCI PDQ", "found and labelled").await?;
     
    // NCI grants

    let sql = r#"update ad.temp_idents
        set id_type_id = 407,
        id_type = 'NCI Grant id',
        source_org_id = 100162,
        source_org = 'National Cancer Institute'
        where (id_desc in ('NCI', 'National Cancer Institute')
            or id_value ~ 'NCI-[0-9A-Z]{3}-[0-9A-Z]{3,5}'
            or id_value ~ '^REBACCC')
            and id_type_id is null
            and id_class = 'OTHER_GRANT'"#;
    execute_sql_fb(sql, pool, "NCI grant", "found and labelled").await?;

    let sql = r#"update ad.temp_idents
        set id_type_id = 178,
        id_type = 'NCI identifier',
        source_org_id = 100162,
        source_org = 'National Cancer Institute'
        where (id_desc in ('NCI', 'National Cancer Institute')
            or id_value ~ 'NCI-[0-9A-Z]{3}-[0-9A-Z]{3,5}'
            or id_value ~ '^REBACCC')
            and id_type_id is null
            and (id_class is null or id_class <> 'OTHER_GRANT')"#;
    execute_sql_fb(sql, pool, "Other NCI", "found and labelled").await?;
    info!("");
    Ok(())

}


pub async fn find_us_cdc_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 
  
    replace_string_in_ident("CDC - N", "CDC-N", pool).await?;  
    replace_string_in_ident("CDC N", "CDC-N", pool).await?;  
    replace_string_in_ident("CDC IRB", "CDC-IRB", pool).await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 406,
        id_type = 'CDC Grant id',
        source_org_id = 100245,
        source_org = 'Centers for Disease Control and Prevention'
        where (id_desc in ('CDC', 'CDC, USA', 'CDC grant number')
            or (id_desc ~ 'Centers for Disease Control' and id_desc not ilike '%Taiwan%')
            or id_desc ilike 'Center for Disease Control%'
            or id_desc ilike 'CDC Cooperative Agreement%'
            or id_value ilike 'CDC-CGH%'
            or id_value ilike 'CDC-G%'
            or id_value ilike 'CDC-N%'
            or id_value ilike 'CDC-O%'
            or id_value ilike 'CDC %')
            and id_class = 'OTHER_GRANT'"#;
    execute_sql_fb(sql, pool, "CDC grant", "found and labelled").await?;
   
    let sql = r#"update ad.temp_idents
        set id_type_id = 177,
        id_type = 'CDC identifier',
        source_org_id = 100245,
        source_org = 'Centers for Disease Control and Prevention'
        where ((id_desc ~ 'Centers for Disease Control' and id_desc not ilike '%Taiwan%')
            or id_desc in ('CDC', 'CDC, USA', 'CDC number', 'CDC Protocol number',
            'CDC, USA, CDC IRB Protocol number')
            or id_value ilike 'CDC-CGH%'
            or id_value ilike 'CDC-G%'
            or id_value ilike 'CDC-N%'
            or id_value ilike 'CDC-O%'
            or id_value ilike 'CDC %')
            and (id_class is null or id_class <> 'OTHER_GRANT')"#;
    execute_sql_fb(sql, pool, "Other CDC", "found and labelled").await?;
    info!("");
    Ok(())
}


pub async fn find_nih_grant_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 

    let sql = r#"update ad.temp_idents
        set id_type_id = 401,
        id_type = 'NIH grant ID',
        source_org_id = 100134,
        source_org = 'National Institutes of Health'
        where id_class = 'NIH'"#;
    let res1 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_type_id = 401,
        id_type = 'NIH grant ID',
        source_org_id = 100134,
        source_org = 'National Institutes of Health'
        where id_desc in ('Federal Funding, NIH', 'NIH Contract', 'NIH Contract Number', 'US NIH Contract Number',
        'NIH grant number', 'NIH contract')
        or id_desc ilike 'US NIH Grant%'
        or id_desc ilike 'U.S. NIH Grant%'
        or (id_class = 'OTHER_GRANT' 
        and id_desc in ('nih', 'NIH', 'National Institutes of Health (NIH)', 'US NIH'))"#;
    let res2 = execute_sql(sql, pool).await?;

    // attempts to get similar ids to the NIH ones created immediately above,
    // where they do not have type_id_descriptions indicating NIH

    let sql = r#"update ad.temp_idents a
        set id_type_id = 401,
        id_type = 'NIH grant ID',
        source_org_id = 100134,
        source_org = 'National Institutes of Health'
        from
        (select substring(id_value, 1, 4) as pref, count(id) from ad.temp_idents
            where id_type_id = 401
            group by substring(id_value, 1, 4)
            having count(id) >= 30) p
        where substring(a.id_value, 1, 4) = p.pref
        and a.id_type_id is null; "#;
    let res3 = execute_sql(sql, pool).await?;
    info!("{} NIH grant identifiers found and labelled", res1.rows_affected() + res2.rows_affected() + res3.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_fda_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 

    let sql = r#"update ad.temp_idents
        set id_type_id = 403,
        id_type = 'FDA Grant ID',
        source_org_id = 108548,
        source_org = 'Food and Drug Administration'
        where id_class = 'FDA'
        or id_value ~ '^75F4'
        or id_value ~ '^HHSF'"#;
    let res1 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_type_id = 403,
        id_type = 'FDA Grant ID',
        source_org_id = 108548,
        source_org = 'Food and Drug Administration'
        where id_class = 'OTHER_GRANT' 
        and id_desc in ('fda', 'FDA', 'Food and Drug Administration', 
        'US FDA', 'USFDA', 'United States FDA', 'U.S. Food and Drug Administration',
        'US Food and Drug Administration', 'US FOOD AND DRUG ADMN')
        and id_value not ilike '%nih%'"#;
    let res2 = execute_sql(sql, pool).await?;
    info!("{} FDA grant identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

    // FDA Orphan drug IDs
    
    let sql = r#"update ad.temp_idents
        set id_type_id = 183,
        id_type = 'FDA Orphan Drug ID',
        source_org_id = 108548,
        source_org = 'Food and Drug Administration'
        where id_desc ~ 'OOPD'
        or (id_desc ~ 'Orphan' and id_desc ~ 'FDA')
        or id_value ~ 'FD-R-[0-9]{4,7}'"#;
    execute_sql_fb(sql, pool, "FDA orphan drug", "found and labelled").await?;

    // FDA IND / IDEIDs

    let sql = r#"update ad.temp_idents
        set id_type_id = 184,
        id_type = 'FDA IND/IDE number',
        source_org_id = 108548,
        source_org = 'Food and Drug Administration'
        where id_type_id is null
        and 
        (id_value like 'IND%' or id_value like 'IDE%' or 
        id_value like 'BB-IND%' or id_value like 'BB IND%' or id_value like 'BB-%' or id_value like 'BB-IDE%' or id_value ~ 'G[0-9]{6}' 
        or id_value ~ '[0-9]{3},[0-9]{3}' or id_value ~ '[0-9]{2},[0-9]{3}' or  id_value ~ '^[0-9]{4,7}$')
        and 
        (id_desc ~ 'FDA' or id_desc ~ 'IND number' or id_desc ~ 'CBER' or id_desc ~ 'Food and Drug Administration' 
        or id_desc in ('IND', 'Food & Drug Administration' , 'IND approval', 'IND No'))
        and 
        id_desc !~ 'Taiwan' and id_desc !~ 'Korea' and id_desc !~ 'Saudi'  and id_desc !~ 'French' 
        and id_desc !~ 'Chin'  and id_desc !~ 'FDASU' and id_desc !~ 'Shire' and id_desc !~ 'Madrid' and id_desc !~ 'OHRP'
        and id_desc !~ 'Inulin'  and id_desc !~ 'JB' and id_desc !~ 'SFDA' and id_desc !~ 'CFDA' 
        and id_desc !~ 'TFDA' and id_desc !~ 'KFDA' and id_desc !~ 'FDAAA'"#;
        execute_sql_fb(sql, pool, "FDA IND / IDE", "found and labelled").await?;

    // Remaining FDA identifiers

    let sql = r#"update ad.temp_idents
        set id_type_id = 185,
        id_type = 'FDA identifier',
        source_org_id = 108548,
        source_org = 'Food and Drug Administration'
        where id_type_id is null
        and (id_desc ~ 'FDA' or id_desc ~ 'IND number' or id_desc ~ 'CBER'or id_desc ~ 'Food and Drug Administration' 
        or id_desc in ('IND', 'IND approval', 'IND No'))
        and id_desc !~ 'Taiwan' and id_desc !~ 'Korea' and id_desc !~ 'Saudi'  and id_desc !~ 'French' 
        and id_desc !~ 'Chin'  and id_desc !~ 'FDASU' and id_desc !~ 'Shire' and id_desc !~ 'Madrid' and id_desc !~ 'OHRP'
        and id_desc !~ 'Inulin'  and id_desc !~ 'JB' and id_desc !~ 'SFDA' and id_desc !~ 'CFDA' 
        and id_desc !~ 'TFDA' and id_desc !~ 'KFDA' and id_desc !~ 'FDAAA' and id_desc !~ 'State Food ' and id_value !~ '^VCU'"#;
        execute_sql_fb(sql, pool, "Other FDA", "found and labelled").await?;

    info!("");
    Ok(())

}


pub async fn find_other_us_grant_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> { 
      
    // AHRQ

    let sql = r#"update ad.temp_idents
        set id_type_id = 402,
        id_type = 'AHRQ Grant ID',
        source_org_id = 100407,
        source_org = 'Agency for Health Research and Quality'
        where id_class = 'AHRQ'
        or id_desc ilike '%AHRQ%'
        or id_desc ilike '%Research Quality%'"#;
    execute_sql_fb(sql, pool, "AHRQ grant", "found and labelled").await?;
    
    let sql = r#"update ad.temp_idents
        set id_value = trim(replace(id_value, 'AHRQ', ''))
        where id_type_id = 402
        and id_value like '%AHRQ%'"#;
    execute_sql(sql, pool).await?;
   
    // SAMHSA

    let sql = r#"update ad.temp_idents
        set id_type_id = 404,
        id_type = 'SAMHSA Grant ID',
        source_org_id = 108270,
        source_org = 'Substance Abuse and Mental Health Services Administration'
        where id_class = 'SAMHSA'"#;
    let res1 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_type_id = 404,
        id_type = 'SAMHSA Grant ID',
        source_org_id = 108270,
        source_org = 'Substance Abuse and Mental Health Services Administration'
        where id_class = 'OTHER_GRANT' 
        and (id_desc ilike '%SAMHSA%'
        or id_desc ilike '%substance abuse and mental health%')"#;
    let res2 = execute_sql(sql, pool).await?;

    info!("{} SAMHSA grant identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

    // Dept of Defense Grants
    
    let sql = r#"update ad.temp_idents
        set id_type_id = 405,
        id_type = 'US Department of Defense Grant ID',
        source_org_id = 101872,
        source_org = 'US Department of Defense'
        where id_value ilike 'W81XWH%'
        or id_value ilike 'CDMRP%'
        or id_value ilike 'HT9425%'"#;
    let res1 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_type_id = 405,
        id_type = 'US Department of Defense Grant ID',
        source_org_id = 101872,
        source_org = 'US Department of Defense'
        where (id_desc ilike '%department of defense%'
        or id_desc ilike '%dept of defense%'
        or id_desc ilike '%dod%')
        and id_type_id is null"#;
    let res2 = execute_sql(sql, pool).await?;

    info!("{} Department of Defense grant identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	
    info!("");
    
    Ok(())
}

