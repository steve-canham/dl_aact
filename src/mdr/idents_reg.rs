
use super::utils::{execute_sql};
use super::idents_utils::{replace_string_in_ident, execute_sql_fb};

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;


pub async fn find_japanese_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // UMIN 

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'C000[0-9]{6}'),
        id_type_id = 141,
        source_org_id = 100156,
        source_org = 'University Hospital Medical Information Network'
        where id_value ~ '^C000[0-9]{6}'
        or (id_value ~ 'C000[0-9]{6}' and id_value like '%UMIN%')"#;
    let res1 = execute_sql(sql, pool).await?;

    replace_string_in_ident("UMIN 0", "UMIN0", pool).await?;

    // UMIN Ids present in both the id_value and the id_desc fields

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'UMIN[0-9]{9}'),
        id_type_id = 141,
        source_org_id = 100156,
        source_org = 'University Hospital Medical Information Network'
        where id_value ~ 'UMIN[0-9]{9}'"#;
    let res2 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_desc from 'UMIN[0-9]{9}'),
        id_type_id = 141,
        source_org_id = 100156,
        source_org = 'University Hospital Medical Information Network'
        where id_desc ~ 'UMIN[0-9]{9}'"#;
    let res3 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'UMIN'||id_value
        where id_desc ~ 'UMIN'
        and id_value ~ '[0-9]{9}'
        and id_type_id is null"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'UMIN[0-9]{9}'),
        id_type_id = 141,
        source_org_id = 100156,
        source_org = 'University Hospital Medical Information Network'
        where id_value ~ 'UMIN[0-9]{9}'
        and id_type_id is null"#;
    let res4 = execute_sql(sql, pool).await?;

    info!("{} UMIN japanese identifiers found and labelled", res1.rows_affected() + 
                        res2.rows_affected() + res3.rows_affected() + res4.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_type_id = 179,
        id_type = 'Malformed registry Id',
        source_org_id = 100156,
        source_org = 'University Hospital Medical Information Network'
        where id_value ~ 'UMIN[0-9]{8}'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Malformed UMIN", "labelled").await?;
    info!("");

    // jRCT

    replace_string_in_ident("jRCT ", "jRCT", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = 'jRCT'||id_value
        where id_value ~ '^[0-9]{10}'
        and id_desc = 'jRCT'"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'jRCT[0-9]{10}'),
        id_type_id = 140,
        source_org_id = 0,
        source_org = 'Japan Registry of Clinical Trials'
        where id_value ~ 'jRCT[0-9]{10}'"#;
    let res1 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'JPRN-'||substring(id_value from 'jRCTs[0-9]{9}'),
        id_type_id = 140,
        source_org_id = 0,
        source_org = 'Japan Registry of Clinical Trials'
        where id_value ~ 'jRCTs[0-9]{9}'"#;
    let res2 = execute_sql(sql, pool).await?;

    info!("{} jCRT japanese identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	
    info!("");

    replace_string_in_ident("JAPIC", "Japic", pool).await?; 
    replace_string_in_ident("JapicCTI- ", "JapicCTI-", pool).await?; 
    replace_string_in_ident("Japic CTI-", "JapicCTI-", pool).await?; 
    replace_string_in_ident("JapicCTI0", "JapicCTI-0", pool).await?; 
    replace_string_in_ident("JapicCTI-22-", "JapicCTI-22", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'JapicCTI-[0-9]{6}'),
        id_type_id = 139,
        source_org_id = 100157,
        source_org = 'Japan Pharmaceutical Information Center'
        where id_value ~ 'JapicCTI-[0-9]{6}'"#;
    let res1 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'JapicCTI-R[0-9]{6}'),
        id_type_id = 139,
        source_org_id = 100157,
        source_org = 'Japan Pharmaceutical Information Center'
        where id_value ~ 'JapicCTI-R[0-9]{6}'"#;
    let res2 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'JapicCTI-'||substring(id_value from '[0-9]{6}') 
        where id_desc ilike '%JAPIC%'
        and id_value ~ '[0-9]{6}'
        and id_type_id is null"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_type_id = 139,
        source_org_id = 100157,
        source_org = 'Japan Pharmaceutical Information Center'
        where id_value ~ 'JapicCTI-[0-9]{6}'
        and id_type_id is null"#;
    let res4 = execute_sql(sql, pool).await?;

    info!("{} JAPIC japanese identifiers found and labelled", res1.rows_affected() + res2.rows_affected() + res4.rows_affected());	
    
    let sql = r#"update ad.temp_idents
        set id_type_id = 179,
        id_type = 'Malformed registry Id',
        source_org_id = 100157,
        source_org = 'Japan Pharmaceutical Information Center'
        where id_value ~ 'JapicCTI-[0-9]{5}'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Malformed UMIN", "labelled").await?;
    info!("");


    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'JMA-IIA[0-9]{5}'),
        id_type_id = 138,
        source_org_id = 100158,
        source_org = 'Japan Medical Association Center for Clinical Trials'
        where id_value ~ 'JMA-IIA[0-9]{5}'"#;
    let res = execute_sql(sql, pool).await?;

    info!("{} JMA japanese identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_chinese_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // ChiCTR

    replace_string_in_ident("chiCTR", "ChiCTR", pool).await?; 
    replace_string_in_ident("CHiCTR", "ChiCTR", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ChiCTR[0-9]{10}'),
        id_type_id = 118,
        source_org_id = 100494,
        source_org = 'West China Hospital"'
        where id_value ~ 'ChiCTR[0-9]{10}'"#;
    let res1 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ChiCTR-[A-Z]{3,5}-[0-9]{8}'),
        id_type_id = 118,
        source_org_id = 100494,
        source_org = 'West China Hospital"'
        where id_value ~ 'ChiCTR-[A-Z]{3,5}-[0-9]{8}'"#;
    let res2 = execute_sql(sql, pool).await?;
    info!("{} ChiCTR Chinese identifiers found and labelled", res1.rows_affected() + res2.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_type_id = 179,
        id_type = 'Malformed registry Id',
        source_org_id = 100494,
        source_org = 'West China Hospital"'
        where id_value ~ 'ChiCTR[0-9]{7,9}'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Malformed ChiCTR", "labelled").await?;
    info!("");

   // ITMC 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ITMCTR[0-9]{10}'),
        id_type_id = 133,
        source_org_id = 0102245,
        source_org = 'China Academy of Chinese Medical Sciences'
        where id_value ~ 'ITMCTR[0-9]{10}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} ITMCTR Trad Medicine identifiers found and labelled", res.rows_affected());	

    // Hong Kong
    
    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, ' ', '')
    where id_value ilike '%HKUCTR%'"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'HKUCTR-[0-9]{1,4}'),
        id_type_id = 156,
        source_org_id = 0,
        source_org = 'The University of Hong Kong'
        where id_value ~ 'HKUCTR-[0-9]{1,4}'
        or id_value ~ 'HKCTR-[0-9]{1,4}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} HKUCTR Hong Kong identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_other_asian_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // CTRI

    let sql = r#"update ad.temp_idents
        set id_value = replace (substring(id_value from 'CTRI/20[0-9]{2}/[0-9]{2,3}/[0-9]{6}'), '/', '-'),
        id_type_id = 121,
        source_org_id = 102044,
        source_org = 'Indian Council of Medical Research'
        where id_value ~ 'CTRI/20[0-9]{2}/[0-9]{2,3}/[0-9]{6}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} CTRI Indian identifiers found and labelled", res.rows_affected());	

    // SRi-LANKA

    replace_string_in_ident("SLCTR/ ", "SLCTR/", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = replace(substring(id_value from 'SLCTR/20[0-9]{2}/[0-9]{3}'),  '/', '-'),
        id_type_id = 130,
        source_org_id = 0,
        source_org = 'Sri Lanka Medical Association'
        where id_value ~ 'SLCTR/20[0-9]{2}/[0-9]{3}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} SLCTR Sri Lankan identifiers found and labelled", res.rows_affected());	

    // KCT 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'KCT[0-9]{7}'),
        id_type_id = 119,
        source_org_id = 0,
        source_org = 'Korea Disease Control and Prevention Agency '
        where id_value ~ 'KCT[0-9]{7}'
        and id_value !~ 'MKKCT[0-9]{7}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} KCT Korean identifiers found and labelled", res.rows_affected());	

    // THAI

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'TCTR20[0-9]{9}'),
        id_type_id = 131,
        source_org_id = 0,
        source_org = 'Central Research Ethics Committee, Thailand'
        where id_value ~ 'TCTR20[0-9]{9}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} TCTR Thai identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_middle_eastern_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // IRCT

    replace_string_in_ident("IRCT2020-", "IRCT2020", pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'IRCT[0-9]{11,14}N[0-9]{1,2}'),
        source_org_id = 0,
        source_org = 'Iranian Ministry of Health and Medical Education'
        where id_value ~ 'IRCT[0-9]{11,14}N[0-9]{1,2}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} IRCT Iranian identifiers found and labelled", res.rows_affected());	

    // LEBANESE

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'LBCTR20[0-9]{8}'),
        id_type_id = 133,
        source_org_id = 0,
        source_org = 'Lebanese Ministry of Public Health'
        where id_value ~ 'LBCTR20[0-9]{8}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} LBCTR Lebanese identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_latin_american_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // RBR

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'RBR-[0-9a-z]{6,8}'),
        id_type_id = 117,
        source_org_id = 109251,
        source_org = 'Instituto Oswaldo Cruz'
        where id_value ~ 'RBR-[0-9a-z]{6,8}'"#;

    let res = execute_sql(sql, pool).await?;
    info!("{} RBR Brazilian identifiers found and labelled", res.rows_affected());	

    // PERU

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'PER-[0-9]{3}-[0-9]{2}'),
        id_type_id = 129,
        source_org_id = 0,
        source_org = 'National Institute of Health, Peru'
        where id_value ~ '^PER-[0-9]{3}-[0-9]{2}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} PER Peruvian identifiers found and labelled", res.rows_affected());	

    // CUBA

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'RPCEC[0-9]{8}'),
        id_type_id = 122,
        source_org_id = 0,
        source_org = 'The National Coordinating Center of Clinical Trials, Cuba'
        where id_value ~ 'RPCEC[0-9]{8}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} RPCEC Cuban identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_other_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // WHO number 

    let sql = r#"update ad.temp_idents
        set id_value = 'U'||substring(id_value from '1111-[0-9]{4}-[0-9]{4}'),
        id_type_id = 115,
        source_org_id = 100114,
        source_org = 'World Health Organisation'
        where id_value ~ '1111-[0-9]{4}-[0-9]{4}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} WHO U identifiers found and labelled", res.rows_affected());	
    info!("");

    // ACTRN

    replace_string_in_ident("ACTRN0", "ACTRN", pool).await?;  // preliminary tidying
    replace_string_in_ident("ACTRNO", "ACTRN", pool).await?;  // of a few records

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ACTRN[0-9]{14}'),
        id_type_id = 116,
        source_org_id = 100690,
        source_org = 'National Health and Medical Research Council, Australia'
        where id_value ~ 'ACTRN[0-9]{14}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} ACTRN Australian / NZ identifiers found and labelled", res.rows_affected());	
    info!("");

    // PACTR
    
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'PACTR[0-9]{15,16}'),
        id_type_id = 128,
        source_org_id = 0,
        source_org = 'Cochrane South Africa'
        where id_value ~ 'PACTR[0-9]{15,16}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} PACTR Pan African identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}