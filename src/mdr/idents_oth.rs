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


pub async fn find_taiwanese_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"update ad.temp_idents
        set id_type_id = 189,
        id_type = 'National Taiwan Univ Hosp Study ID',
        source_org_id = 100186,
        source_org = 'National Taiwan University Hospital'
        where id_value ~ '^20(1|2)[0-9]{6}[A-Z]IND'"#;
    execute_sql_fb(sql, pool, "National Taiwan University Hospital Study", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 412,
        id_type = 'National Science and Technology Council Taiwan Grant ID',
        source_org_id = 0,
        source_org = 'National Science and Technology Council Taiwan'
        where id_value ~ '^NSTC( |-)?1[0-9]{2}-'"#;
    execute_sql_fb(sql, pool, "Nat Science and Technology Council Taiwan Grant", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 413,
        id_type = 'Ministry of Health and Welfare Taiwan Grant ID',
        source_org_id = 0,
        source_org = 'Ministry of Health and Welfare Taiwan'
        where id_value ~ '^MOHW( |-)?1[0-9]{2}-'"#;
    execute_sql_fb(sql, pool, "Min of Health and Welfare Taiwan Grant", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 414,
        id_type = 'Ministry of Science and Technology Taiwan Grant ID',
        source_org_id = 102335,
        source_org = 'Ministry of Science and Technology Taiwan'
        where id_value ~ '^MOST( |-)?1[0-9]{2}-'"#;
    execute_sql_fb(sql, pool, "Min of Science and Technology Taiwan Grant", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 415,
        id_type = 'Department of Health Taiwan Grant ID',
        source_org_id = 0,
        source_org = 'Department of Health Taiwan'
        where id_value ~ '^DOH-?9[0-9](-|F)'"#;
    execute_sql_fb(sql, pool, "Department of Health Taiwan Grant", "found and labelled").await?;  

    info!("");    
    Ok(())
}


pub async fn find_collab_group_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // CSWOG

    let sql = r#"update ad.temp_idents
        set id_type_id = 195,
        id_type = 'China Association for Clinical Onc. ID',
        source_org_id = 0,
        source_org = 'China Southern Association for Clinical Oncology'
        where id_value ~ '^CSWOG'"#;
    execute_sql_fb(sql, pool, "Chinese SWOG", "found and labelled").await?;  

    // KSWOG

    let sql = r#"update ad.temp_idents
        set id_type_id = 194,
        id_type = 'Korean SW Onc. Group ID',
        source_org_id = 0,
        source_org = 'Korean South West Oncology Group'
        where id_value ~ '^KSWOG'"#;
    execute_sql_fb(sql, pool, "Korean SWOG", "found and labelled").await?;  

    // SWOG 

    let sql = r#"update ad.temp_idents
        set id_type_id = 193,
        id_type = 'South West Onc. Group ID (US)',
        source_org_id = 100358,
        source_org = 'South West Oncology Group'
        where id_value ~ '^SWOG'
        and (length(id_value) <= 12
        or (id_value ~ 'ICSC$' and length(id_value) <= 16)
        or id_value ~ '-S[0-9]{4}-S[0-9]{4}'
        or id_value ~ '-S[0-9]{4}-[0-9]{4}'
        or id_value ~ '-S[0-9]{4}/S[0-9]{4}')"#;
    execute_sql_fb(sql, pool, "US SWOG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'SWOG' 
        and id_type_id is null"#;
    let res1 = execute_sql(sql, pool).await?;


    // CECOG

    let sql = r#"update ad.temp_idents
        set id_value = replace(replace(id_value, '/', ' '), '  ', ' '),
        id_type_id = 196,
        id_type = 'Central European Cooperative Onc. Group ID',
        source_org_id = 0,
        source_org = 'Central European Cooperative Oncology Group'
        where id_value ~ '^CECOG'"#;
    execute_sql_fb(sql, pool, "Central European COG", "found and labelled").await?;  

    // ECOG

    let sql = r#"update ad.temp_idents
        set id_type_id = 201,
        id_type = 'ECOG-ACRIN ID (US)',
        source_org_id = 101684,
        source_org = 'Eastern Cooperative Oncology Group / American College of Radiology Imaging Network'
        where id_value ~ 'ECOG-ACRIN'"#;
    execute_sql_fb(sql, pool, "ECOG-ACRIN", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 200,
        id_type = 'American Coll. of Radiology Imaging Network ID',
        source_org_id = 104735,
        source_org = 'American College of Radiology Imaging Network'
        where id_value ~ 'ACRIN'
        and (length(id_value) <= 14 or id_value ~ 'RESCUE$')
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "ACRIN", "found and labelled").await?;  


    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'ACRIN' 
        and id_type_id is null"#;
    let res2 = execute_sql(sql, pool).await?;


    let sql = r#"update ad.temp_idents
        set id_type_id = 199,
        id_type = 'Eastern Cooperative Onc. Group ID (US)',
        source_org_id = 100428,
        source_org = 'Eastern Cooperative Oncology Group'
        where (id_value ~ '^ECOG' or id_value = 'E8200 ECOG')
        and length(id_value) <= 16 
        and id_value !~ 'RTOG'
        and id_value !~ 'NCIC'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "ECOG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'ECOG'
        and id_type_id is null"#;
    let res3 = execute_sql(sql, pool).await?;

    // CALCGB

    let sql = r#"update ad.temp_idents
        set id_type_id = 190,
        id_type = 'Cancer & Leukemia GrpB ID (US)',
        source_org_id = 101892,
        source_org = 'Cancer and Leukemia Group B (US)'
        where id_value ~ '^CALGB'
        and (length(id_value) <= 14 
        or (id_value ~ 'ICSC$' and length(id_value) <= 18))"#;
    execute_sql_fb(sql, pool, "CALGB", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'CALGB'
        and id_type_id is null"#;
    let res4 = execute_sql(sql, pool).await?;


    // CAN-NCIC

    let sql = r#"update ad.temp_idents
        set id_type_id = 204,
        id_type = 'National Cancer Inst. of Canada ID',
        source_org_id = 100530,
        source_org = 'NCIC Clinical Trials Group'
        where id_value ~ '^CAN-NCIC'
        and id_value !~ 'GOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "CAN-NCIC", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'CAN-NCIC' 
        and id_type_id is null"#;
    let res5 = execute_sql(sql, pool).await?;


    // CRTOG

    let sql = r#"update ad.temp_idents
        set id_type_id = 205,
        id_type = 'Chinese Radiation Therapy Onc. Group ID',
        source_org_id = 0,
        source_org = 'Chinese Radiation Therapy Oncology Group'
        where id_value ~ 'CRTOG'"#;
    execute_sql_fb(sql, pool, "Chinese RTOG", "found and labelled").await?;  

    // RTOG

    let sql = r#"update ad.temp_idents
        set id_type_id = 191,
        id_type = 'Radiation Therapy Onc. Group ID (US)',
        source_org_id = 100525,
        source_org = 'Radiation Therapy Oncology Group (US)'
        where id_value ~ '^RTOG'
        and id_value !~ 'GOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "RTOG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'RTOG' 
        and id_type_id is null"#;
    let res6 = execute_sql(sql, pool).await?;

    let r = res1.rows_affected() + res2.rows_affected() + res3.rows_affected() 
               + res4.rows_affected() + res5.rows_affected() + res6.rows_affected();
    info!("{} ad hoc collaboration IDs found and labelled", r);

    info!("");    
    Ok(())
}


/*

// SWOG 

let sql = r#"update ad.temp_idents
set id_type_id = 193,
id_type = 'South West Onc. Group ID (US)',
source_org_id = 100358,
source_org = 'South West Oncology Group'
where id_value ~ '^SWOG'
and (length(id_value) <= 12
or (id_value ~ 'ICSC$' and length(id_value) <= 16)
or id_value ~ '-S[0-9]{4}-S[0-9]{4}'
or id_value ~ '-S[0-9]{4}-[0-9]{4}'
or id_value ~ '-S[0-9]{4}/S[0-9]{4}')"#;
    execute_sql_fb(sql, _pool, "Korean SWOG", "found and labelled").await?;  



let sql = r#"update ad.temp_idents
set id_type_id = 220,
id_type = 'Ad Hoc Collaboration ID',
source_org_id = 0,
source_org = 'Ad hoc Collaboration of research organisations'
where id_value ~ 'SWOG' 
and id_type_id is null"#;
let res1 = execute_sql(sql, pool).await?;


// ECOG

let sql = r#"update ad.temp_idents
set id_type_id = 201,
id_type = 'ECOG-ACRIN ID (US)',
source_org_id = 101684,
source_org = 'Eastern Cooperative Oncology Group / American College of Radiology Imaging Network'
where id_value ~ 'ECOG-ACRIN'"#;
    execute_sql_fb(sql, _pool, "Korean SWOG", "found and labelled").await?;  

let sql = r#"update ad.temp_idents
set id_type_id = 200,
id_type = 'American Coll. of Radiology Imaging Network ID',
source_org_id = 104735,
source_org = 'American College of Radiology Imaging Network'
where id_value ~ 'ACRIN'
and (length(id_value) <= 14 or id_value ~ 'RESCUE$')
and id_type_id is null"#;
    execute_sql_fb(sql, _pool, "Korean SWOG", "found and labelled").await?;  


let sql = r#"update ad.temp_idents
set id_type_id = 220,
id_type = 'Ad Hoc Collaboration ID',
source_org_id = 0,
source_org = 'Ad hoc Collaboration of research organisations'
where id_value ~ 'ACRIN' 
and id_type_id is null"#;
let res2 = execute_sql(sql, pool).await?;


let sql = r#"update ad.temp_idents
set id_type_id = 199,
id_type = 'Eastern Cooperative Onc. Group ID (US)',
source_org_id = 100428,
source_org = 'Eastern Cooperative Oncology Group'
where (id_value ~ '^ECOG' or id_value = 'E8200 ECOG')
and length(id_value) <= 16 
and id_value !~ 'RTOG'
and id_value !~ 'NCIC'
and id_type_id is null"#;
    execute_sql_fb(sql, _pool, "Korean SWOG", "found and labelled").await?;  


let sql = r#"update ad.temp_idents
set id_type_id = 220,
id_type = 'Ad Hoc Collaboration ID',
source_org_id = 0,
source_org = 'Ad hoc Collaboration of research organisations'
where id_value ~ 'ECOG'
and id_type_id is null"#;
let res3 = execute_sql(sql, pool).await?;

-- CALCGB

let sql = r#"update ad.temp_idents
set id_type_id = 190,
id_type = 'Cancer & Leukemia GrpB ID (US)',
source_org_id = 101892,
source_org = 'Cancer and Leukemia Group B (US)'
where id_value ~ '^CALGB'
and (length(id_value) <= 14 
or (id_value ~ 'ICSC$' and length(id_value) <= 18))"#;
    execute_sql_fb(sql, _pool, "Korean SWOG", "found and labelled").await?;  

let sql = r#"update ad.temp_idents
set id_type_id = 220,
id_type = 'Ad Hoc Collaboration ID',
source_org_id = 0,
source_org = 'Ad hoc Collaboration of research organisations'
where id_value ~ 'CALGB'
and id_type_id is null"#;
let res4 = execute_sql(sql, pool).await?;


-- CAN-NCIC

let sql = r#"update ad.temp_idents
set id_type_id = 204,
id_type = 'National Cancer Inst. of Canada ID',
source_org_id = 100530,
source_org = 'NCIC Clinical Trials Group'
where id_value ~ '^CAN-NCIC'
and id_value !~ 'GOG'
and id_type_id is null"#;
    execute_sql_fb(sql, _pool, "Korean SWOG", "found and labelled").await?;  

let sql = r#"update ad.temp_idents
set id_type_id = 220,
id_type = 'Ad Hoc Collaboration ID',
source_org_id = 0,
source_org = 'Ad hoc Collaboration of research organisations'
where id_value ~ 'CAN-NCIC' 
and id_type_id is null"#;
let res5 = execute_sql(sql, pool).await?;


-- CRTOG

let sql = r#"update ad.temp_idents
set id_type_id = 205,
id_type = 'Chinese Radiation Therapy Onc. Group ID',
source_org_id = 0,
source_org = 'Chinese Radiation Therapy Oncology Group'
where id_value ~ 'CRTOG'"#;
    execute_sql_fb(sql, _pool, "Korean SWOG", "found and labelled").await?;  

-- RTOG

let sql = r#"update ad.temp_idents
set id_type_id = 191,
id_type = 'Radiation Therapy Onc. Group ID (US)',
source_org_id = 100525,
source_org = 'Radiation Therapy Oncology Group (US)'
where id_value ~ '^RTOG'
and id_value !~ 'GOG'
and id_type_id is null"#;
    execute_sql_fb(sql, _pool, "Korean SWOG", "found and labelled").await?;  

let sql = r#"update ad.temp_idents
set id_type_id = 220,
id_type = 'Ad Hoc Collaboration ID',
source_org_id = 0,
source_org = 'Ad hoc Collaboration of research organisations'
where id_value ~ 'RTOG' 
and id_type_id is null"#;
let res6 = execute_sql(sql, pool).await?;



*/