
use super::utils::{execute_sql};
use super::idents_utils::{replace_string_in_ident};

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;

pub async fn find_eudract_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'),
        id_type_id = 123,
        source_org_id = 100159,
        source_org = 'European Medicines Agency'
        where id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}' 
        and (
        length(id_value) = 14
        or id_value ilike '%eu%'
        or id_value ilike '%udract%'
        or id_value ilike '%edract%')"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} EU CTR identifiers found and labelled", res.rows_affected());	

    // A group of other records incoude EU-CTR IDs, in some cases along with other
    // identifiers. The SQL below removes the other components into a small temporary 
    // table, adding them to the temp_idents table before replacing the original 
    // values with the EU-CTR IDs, and then encoding those.

    let sql = r#"drop table if exists ad.temp_adds;
        create table ad.temp_adds as 
        select sd_sid, 
        replace(id_value, substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'), '') as id_value,  
        id_class, id_desc
        from ad.temp_idents
        where (id_value ~ '^20[0-9]{2}-0[0-9]{5}-[0-9]{2}'  
        or (id_value ~ ' 20[0-9]{2}-0[0-9]{5}-[0-9]{2}')  
        or (id_value ~ '^NL' and id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}')  
        or (id_value ~ '^UK' and id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}')  
        or (id_value ~ '^VIB' and id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}')  
        or (id_value ~ '^ISRCTN' and id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'))
        and id_type_id is null
        and id_value !~ '^SC'
        and id_value !~ '^SF'
        and replace(id_value, substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'), '') ~ '[1-9]'"#;
    execute_sql(sql, pool).await?;
    
    let sql = r#"update ad.temp_adds set id_value = trim(id_value);
        update ad.temp_adds set id_value = trim(BOTH '-' from id_value);
        update ad.temp_adds set id_value = trim(BOTH '/' from id_value);
        update ad.temp_adds set id_value = trim(BOTH ',' from id_value);
        update ad.temp_adds set id_value = trim(BOTH '&' from id_value);
        update ad.temp_adds set id_value = trim(id_value);
        select * from ad.temp_adds;

        insert into ad.temp_idents
        (sd_sid, id_value, id_class, id_desc)
        select sd_sid, id_value, id_class, id_desc
        from ad.temp_adds"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}')
        where (id_value ~ '^20[0-9]{2}-0[0-9]{5}-[0-9]{2}'  
        or (id_value ~ ' 20[0-9]{2}-0[0-9]{5}-[0-9]{2}')  
        or (id_value ~ '^NL' and id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}')  
        or (id_value ~ '^UK' and id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}')  
        or (id_value ~ '^VIB' and id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}')  
        or (id_value ~ '^ISRCTN' and id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'))
        and id_type_id is null
        and id_value !~ '^SC'
        and id_value !~ '^SF'"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'),
        id_type_id = 123,
        source_org_id = 100159,
        source_org = 'European Medicines Agency'
        where id_value ~'20[0-9]{2}-0[0-9]{5}-[0-9]{2}' 
        and (length(id_value) = 14)
        and id_type_id is null"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} Additional EU CTR identifiers found and labelled", res.rows_affected());	

    // A further small group are mis-formatted EUCTR numbers. Some of these can be turned
    // into properly formatted IDs, and then sncoded. The rest are missing digits or have surplus
    // digits and must be coded as mis-formed regiostry identifiers.
    
    // Reverse values in one record

    let sql = r#"update ad.temp_idents  
        set id_value = id_desc,
        id_desc = id_value
        where id_value = 'EudraCT 2009';"#;
    execute_sql(sql, pool).await?;

    // delete various 'empty' ids

    let sql = r#"delete from ad.temp_idents
        where id_value !~ '[0-9]'
        and id_value ilike '%eudr%'
        and id_type_id is null;

        delete from ad.temp_idents
        where id_value = 'EudraCT: 2222 - 222222-22';

        delete from ad.temp_idents
        where id_value ~ '[0-9]-RCB/EUDRACT';"#;
    execute_sql(sql, pool).await?;

    // identify obvious mis-formed and uncorrectable ones

    let sql = r#"update ad.temp_idents 
        set id_type_id = 179,
        id_type = 'Malformed registry Id',
        source_org_id = 100159,
        source_org = 'European Medicines Agency'
        where id_value ilike '%eudr%'
        and (id_value ~ '20[0-9]{2}-[0-9]{2,5}-[0-9]{2}'
        or  id_value ~ '20[0-9]{2}-[0-9]{7}-[0-9]{2}'
        or  id_value ~ '20[0-9]{2}-1[0-9]{5}-[0-9]{2}'
        or id_value ~ '20[0-9]{2}-[0-9]{6}$'
        or id_value ~ '20[0-9]{2}-[0-9]{6}-[0-9]$'
        or id_value ~ '20[0-9]{2}-[0-9]{6}-[0-9][A-Z]'
        or id_value ~ 'EudraCT code [0-9]{4}-[0-9]{3}'
        or id_value ~ 'EUDRACT-[0-9]{8}'
        or id_value ~ ': [0-9]{6}$'
        or id_value ~ 'umber [0-9]{3}$'
        )
        and id_type_id is null;"#;
    execute_sql(sql, pool).await?;

    // Create the correct format where possible

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, '--', '-')
        where id_value ~ '--'
        and id_value ilike '%eudr%'
        and id_type_id is null;

        update ad.temp_idents
        set id_value ='Eudra '||substring(substring(id_value from '20[0-9]{10}'), 1, 4)||'-'||substring(substring(id_value from '20[0-9]{10}'), 5, 6)||'-'||substring(substring(id_value from '20[0-9]{10}'), 11, 2)
        where id_value ~ '20[0-9]{10}'
        and id_value ilike '%eudr%'
        and id_type_id is null;"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'Eudra '||substring(substring(id_value from '20[0-9]{2}-[0-9]{8}'), 1, 11)||'-'||substring(substring(id_value from '20[0-9]{2}-[0-9]{8}'), 12, 2)
        where id_value ~ '20[0-9]{2}-[0-9]{8}'
        and id_value ilike '%eudr%'
        and id_type_id is null;

        update ad.temp_idents
        set id_value = 'Eudra '||substring(substring(id_value from '20[0-9]{8}-[0-9]{2}'), 1, 4)||'-'||substring(substring(id_value from '20[0-9]{8}-[0-9]{2}'), 5, 9)
        where id_value ~ '20[0-9]{8}-[0-9]{2}'
        and id_value ilike '%eudr%'
        and id_type_id is null;"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'Eudra '||substring(substring(id_value from '20[0-9]{4}-[0-9]{4}-[0-9]{2}'), 1, 4)||'-'
        ||substring(substring(id_value from '20[0-9]{4}-[0-9]{4}-[0-9]{2}'), 5, 2)
        ||substring(substring(id_value from '20[0-9]{4}-[0-9]{4}-[0-9]{2}'), 8, 8)
        where id_value ~ '20[0-9]{4}-[0-9]{4}-[0-9]{2}'
        and id_value ilike '%eudr%'
        and id_type_id is null;

        update ad.temp_idents
        set id_value = 'Eudra '||substring(substring(id_value from '20[0-9]{2}-[0-9]{3} [0-9]{3}-[0-9]{2}'), 1, 8)
        ||substring(substring(id_value from '20[0-9]{2}-[0-9]{3} [0-9]{3}-[0-9]{2}'), 10, 6)
        where id_value ~ '20[0-9]{2}-[0-9]{3} [0-9]{3}-[0-9]{2}'
        and id_value ilike '%eudr%'
        and id_type_id is null;"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, ' ', '-')
        where (id_value ~ '20[0-9]{2} [0-9]{6} [0-9]{2}'
        or id_value ~ '20[0-9]{2} [0-9]{6}-[0-9]{2}'
        or id_value ~ '20[0-9]{2}-[0-9]{6} [0-9]{2}')
        and id_value ilike '%eudr%'
        and id_type_id is null;

        update ad.temp_idents
        set id_value = replace(id_value, '/', '-')
        where (id_value ~ '20[0-9]{2}/[0-9]{6}/[0-9]{2}'
        or id_value ~ '20[0-9]{2}/[0-9]{6}-[0-9]{2}'
        or id_value ~ '20[0-9]{2}-[0-9]{6}/[0-9]{2}')
        and id_value ilike '%eudr%'
        and id_type_id is null;"#;
    execute_sql(sql, pool).await?;
    
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'),
        id_type_id = 123,
        source_org_id = 100159,
        source_org = 'European Medicines Agency'
        where id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}' 
        and id_value ilike '%eudr%'
        and id_value not ilike '%eudramed%'
        and id_value not ilike '%eudract'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} EU CTR identifiers found and labelled after re-formatting", res.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_other_eu_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // CTIS number

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'),
        id_type_id = 135,
        source_org_id = 100159,
        source_org = 'European Medicines Agency'
        where id_value ~ '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} EU CTIS identifiers found and labelled", res.rows_affected());	
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
        id_type_id = 126,
        source_org_id = 101421,
        source_org = 'Springer Nature'
        where id_value ~ 'ISRCTN[0-9]{8}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} ISRCTN UK identifiers found and labelled", res.rows_affected());	
    info!("");

    // DRKS

    replace_string_in_ident("DRKS number 0", "DRKS0", pool).await?;  // preliminary tidying (1 rec)
 
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'DRKS[0-9]{8}'),
        id_type_id = 124,
        source_org_id = 105875,
        source_org = 'Federal Institute for Drugs and Medical Devices'
        where id_value ~ 'DRKS[0-9]{8}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} DRKS German identifiers found and labelled", res.rows_affected());	
    info!("");

    // DUTCH

    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, ' ', '')
    where id_value ~ '^NTR [0-9]{1,4}$'"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
    set id_value = replace(id_value, '-', '')
    where id_value ~ '^NTR-[0-9]{1,4}$'"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NTR[0-9]{1,4}'),
        id_type_id = 181,
        source_org_id = 0,
        source_org = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NTR[0-9]{1,4}'"#;

    let res = execute_sql(sql, pool).await?;
    info!("{} NTR Dutch identifiers found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL[0-9]{4}'),
        id_type_id = 182,
        source_org_id = 0,
        source_org = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NL[0-9]{4}'
        and length(id_value) < 7"#;

    let res = execute_sql(sql, pool).await?;
    info!("{} NL Dutch identifiers found and labelled", res.rows_affected());	

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL-OMON[0-9]{1,5}'),
        id_type_id = 132,
        source_org_id = 0,
        source_org = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ 'NL-OMON[0-9]{1,5}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} NL-OMON Dutch identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_japanese_registry_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

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

    info!("{} UMIN japanese identifiers found and labelled", res1.rows_affected() + res2.rows_affected() + res3.rows_affected());	

    replace_string_in_ident("jRCT ", "jRCT", pool).await?; 

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

   // ITMC 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ITMCTR[0-9]{10}'),
        id_type_id = 133,
        source_org_id = 0,
        source_org = 'Lebanese Ministry of Public Health'
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
        where id_value ~ 'HKUCTR-[0-9]{1,4}'"#;
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