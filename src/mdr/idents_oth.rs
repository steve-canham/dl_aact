use super::utils::{execute_sql};
use super::idents_utils::{execute_sql_fb, replace_string_in_ident};

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;

pub async fn find_zonmw_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'DRKS[0-9]{8}'),
        id_type_id = 411,
        id_type = 'Dutch ZonMW Grant ID',
        source_org_id = 100467,
        source_org = 'ZonMw: The Netherlands Organisation for Health Research and Development'
        where id_desc ilike '%zonmw%'"#;
    execute_sql_fb(sql, pool, "Dutch ZonMw", "found and labelled").await?;

    info!("");
    Ok(())
}


pub async fn find_eortc_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

     replace_string_in_ident("EORTC ", "EORTC-", pool).await?;  
    
    let sql = r#"update ad.temp_idents
        set id_type_id = 176,
        id_type = 'EORTC ID',
        source_org_id = 100010,
        source_org = 'EORTC'
        where id_value ~ '^EORTC'"#;
    let res1 = execute_sql(sql, pool).await?;
    
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'EORTC-[0-9]{4,5}'),
        id_type_id = 176,
        id_type = 'EORTC ID',
        source_org_id = 100010,
        source_org = 'EORTC'
        where id_value ~ 'EORTC-[0-9]{4,5}'
        and id_type_id is null"#;
    let res2 = execute_sql(sql, pool).await?;
    info!("{} EORTC identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_cruk_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"update ad.temp_idents
        set id_type_id = 410,
        id_type = 'CRUK ID',
        source_org_id = 100517,
        source_org = 'Cancer Research UK'
        where id_value ~ '^CRUK'"#;
    execute_sql_fb(sql, pool, "CRUK funder", "found and labelled").await?;

    info!("");
    Ok(())
}


pub async fn find_basel_ctu_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"SET client_min_messages TO WARNING; 
        drop table if exists ad.temp_adds;
        create table ad.temp_adds as 
        select sd_sid,
		replace(id_value, substring(id_value from '[a-z]{2}[0-9]{2}[A-Z][A-Za-z1-7]+'), '') as id_value,  
        id_class, id_desc
        from ad.temp_idents
        where id_value ~ '[a-z]{2}(1|2)[0-9][A-Z][A-Za-z1-7]+'
            and id_value !~ '^[03SFDBC]'
            and id_value !~ '^[a-z]{3}'
        and replace(id_value, substring(id_value from '[a-z]{2}[0-9]{2}[A-Z][A-Za-z1-7]+'), '') ~ '[1-9]'"#;
    execute_sql_fb(sql, pool, "Additional Swiss", "(found with Basel CTU IDs) added to temp_idents").await?;    
    
    let sql = r#"
        update ad.temp_adds set id_value = trim(id_value);
        update ad.temp_adds set id_value = trim(BOTH ';' from id_value);
        update ad.temp_adds set id_value = trim(BOTH ':' from id_value);
        update ad.temp_adds set id_value = trim(BOTH ',' from id_value);
        update ad.temp_adds set id_value = trim(id_value);"#;
    execute_sql(sql, pool).await?; 

    let sql = r#"update ad.temp_adds
        set id_desc = 'BASEC ID'
        where id_value ~ '^[0-9]{4}-[0-9]{5}$';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"insert into ad.temp_idents
        (sd_sid, id_value, id_class, id_desc)
        select sd_sid, id_value, id_class, id_desc
        from ad.temp_adds;

        drop table ad.temp_adds;"#;

    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '[a-z]{2}[0-9]{2}[A-Z][A-Za-z1-7]+')
        where id_value ~ '[a-z]{2}(1|2)[0-9][A-Z][A-Za-z1-7]+'
            and id_value !~ '^[03SFDBC]'
            and id_value !~ '^[a-z]{3}'
        and id_type_id is null"#;
    execute_sql(sql, pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_type_id = 188,
        id_type = 'Basel CTU ID',
        source_org_id = 100958,
        source_org = 'Universität Basel'
        where id_value ~ '[a-z]{2}(1|2)[0-9][A-Z][A-Za-z1-7]+'
            and id_value !~ '^[03SFDBC]'
            and id_value !~ '^[a-z]{3}'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Basel CTU", "found and labelled").await?;  

    info!("");    
    Ok(())
}


pub async fn find_swiss_basec_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"update ad.temp_idents
        set id_type_id = 802,
        id_type = 'Swiss BASEC ID',
        source_org_id = 0,
        source_org = 'SwissEthics'
        where id_value ~ '^[0-9]{4}-[0-9]{5}$'
        and (id_desc = 'BASEC ID' or id_desc ~ 'Swiss' or id_desc ~ 'ethic')"#;
    execute_sql_fb(sql, pool, "Swiss BASEC etchics", "found and labelled").await?;  

    info!("");    
    Ok(())

}

pub async fn find_chinadrugtrials_nmpa_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // A few are doubled up so need to be split find_other_registry_identities(
          
    let sql = r#"insert into ad.temp_idents 
        (id, sd_sid, id_value, id_class, id_desc)
        select id, sd_sid, trim(unnest(string_to_array(id_value, '/'))) as new_value, 
        id_class, id_desc
        from ad.temp_idents
        where id_value ~ '^CTR[0-9]{8}'
        and id_value !~ '^CTR[0-9]{9}'
        and id_value ~ '/'"#;
    execute_sql(sql, pool).await?; 

    let sql = r#"delete from ad.temp_idents 
        where id_value ~ '^CTR[0-9]{8}'
        and id_value !~ '^CTR[0-9]{9}'
        and id_value ~ '/';"#;
    execute_sql(sql, pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_type_id = 302,
        id_type = 'ChinaDrugTrial ID',
        source_org_id = 107312,
        source_org = 'National Medical Products Administration'
        where id_value ~ '^CTR[0-9]{8}'
        and id_value !~ '^CTR[0-9]{9}'"#;
    execute_sql_fb(sql, pool, "ChinaDrugTrials", "found and labelled").await?;  

    info!("");    
    Ok(())

}


pub async fn find_daides_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '[0-9]{5}'), 
        id_type_id = 173,
        id_type = 'DAIDS-ES registry ID',
        source_org_id = 0,
        source_org = 'NIH NAID Division'
        where (id_desc ilike '%daid%' or id_value ilike '%daid%' 
        or id_desc ilike '%Division of AIDS%')
        and id_value ~ '[0-9]{5}'
        and id_value !~ '^A'
        and id_value !~ '^3UM'"#;
    execute_sql_fb(sql, pool, "DAID-ES", "found and labelled").await?;  

    info!("");    
    Ok(())
}


pub async fn find_eli_lilley_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    let sql = r#"update ad.temp_idents
        set id_type_id = 701,
        id_type = 'Eli Lilly ID',
        source_org_id = 100176,
        source_org = 'Eli Lilly'
        where id_value ~ '^[A-Z][0-9][A-Z]-[A-Z]{2}-[A-Z0-9]{4}$'"#;
    execute_sql_fb(sql, pool, "Eli Lilly protocol", "found and labelled").await?;  

    // Lilly took over Loxo Oncolcogy in 2019

    let sql = r#"update ad.temp_idents
        set id_type_id = 701,
        id_type = 'Eli Lilly ID',
        source_org_id = 100176,
        source_org = 'Eli Lilly'
        where id_value ~ '^LOXO-[A-Z]{3}-'
        or id_value = 'LOXO-260 Expanded Access'"#;
    execute_sql_fb(sql, pool, "Eli Lilly Loxo", "found and labelled").await?;  

let sql = r#"update ad.temp_idents
        set id_type_id = 701,
        id_type = 'Eli Lilly ID',
        source_org_id = 100176,
        source_org = 'Eli Lilly'
        where id_desc ilike '%eli lil%'
        and id_desc !~ 'DICE'
        and id_desc !~ 'AbbVie'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Other Eli Lilly", "found and labelled").await?;  
    
    info!("");    
    Ok(())
}
