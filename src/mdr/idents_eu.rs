use super::utils::{execute_sql};
use super::idents_utils::{replace_string_in_ident, execute_sql_fb};

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;


pub async fn find_ansm_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // Useful to get the ANSM identifiers characterised first, as many of these are
    // wrobgly classed as EUDRACT numbers

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'),
        id_type_id = 301,
        id_type = 'ANSM (ID-RCB) number',
        source_org_id = 101408,
        source_org = 'Agence Nationale de Sécurité du Médicament'
        where id_value ~ '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'
        and (id_desc is null or id_desc not ilike 'AbbVie')"#;
    let res1 = execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_desc from '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'),
        id_type_id = 301,
        id_type = 'ANSM (ID-RCB) number',
        source_org_id = 101408,
        source_org = 'Agence Nationale de Sécurité du Médicament'
        where id_desc ~ '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'
        and id_type_id is null"#;
    let res2 = execute_sql(sql, pool).await?;

    // This group has 'AO' rather than 'A0' in the identifier

    let sql = r#"update ad.temp_idents
        set id_value = replace(substring(id_value from '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'), 'O', '0'),
        id_type_id = 301,
        id_type = 'ANSM (ID-RCB) number',
        source_org_id = 101408,
        source_org = 'Agence Nationale de Sécurité du Médicament'
        where id_value ~ '20[0-9]{2}-AO[0-9]{4}-[0-9]{2}'
        and id_type_id is null"#;
    let res3 = execute_sql(sql, pool).await?;
    info!("{} French ANSM identifiers found and labelled", res1.rows_affected() + res2.rows_affected() + res3.rows_affected());	

    // A series of manipulations of identifiers to get
    // mal-formatted examples back to the standard state (when this is possible)

    let sql = r#"update ad.temp_idents set id_value = '2'||id_value
        where (id_desc ~ 'ANSM' or id_desc ~ 'RCB') and id_value ~ '^0[0-9]{2}-A[0-9]{5}-[0-9]{2}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents set id_value = replace(id_value, '-A-', '-A')
        where (id_desc ~ 'ANSM' or id_desc ~ 'RCB') 
        and id_value ~ '[0-9]{2}-A-[0-9]{5}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents set id_value = replace(id_value, ' ', '-')
        where (id_desc ~ 'ANSM' or id_desc ~ 'RCB')
        and (id_value ~ '20[0-9]{2} A[0-9]{5} [0-9]{2}'
        or id_value ~ '20[0-9]{2} A[0-9]{5}-[0-9]{2}');"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents set id_value = replace(id_value, '/', '-')
        where (id_desc ~ 'ANSM' or id_desc ~ 'RCB') and id_value ~ '20[0-9]{2}/A[0-9]{5}/[0-9]{2}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents set id_value = replace(id_value, '_', '-')
        where (id_desc ~ 'ANSM' or id_desc ~ 'RCB') and (id_value ~ '20[0-9]{2}_A[0-9]{5}-[0-9]{2}'
        or id_value ~ '20[0-9]{2}_A[0-9]{5}_[0-9]{2}');"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents set id_value = replace(id_value, 'a', 'A')
        where (id_desc ~ 'ANSM' or id_desc ~ 'RCB') and id_value ~ '20[0-9]{2}-a[0-9]{5}-[0-9]{2}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"delete from ad.temp_idents where (id_desc ~ 'ANSM' or id_desc ~ 'RCB')
        and id_value  = 'ID-RCB'"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents set id_value = replace(id_value, '.', '-')
        where (id_desc ~ 'ANSM' or id_desc ~ 'RCB') 
        and id_value ~ '20[0-9]{2}\.A[0-9]{5}-[0-9]{2}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents set id_value = replace(id_value, ' ', '')
        where (id_desc ~ 'ANSM' or id_desc ~ 'RCB')
        and (id_value ~ '20[0-9]{2} -A[0-9]{5}-[0-9]{2}' or id_value ~ '20[0-9]{2}- A[0-9]{5}-[0-9]{2}'
        or id_value ~ '20[0-9]{2} - A[0-9]{5}-[0-9]{2}' or id_value ~ '20[0-9]{2}-A[0-9]{5}- [0-9]{2}'
        or id_value ~ '20[0-9]{2}- A[0-9]{5} - [0-9]{2}');"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(substring(id_value from '20[0-9]{2}-A[0-9]{7}$'), 1, 11)||'-'||substring(substring(id_value from '20[0-9]{2}-A[0-9]{7}$'), 12, 2) 
        where id_value ~ '20[0-9]{2}-A[0-9]{7}$'
        and (id_desc <> 'AbbVie' or id_desc is null);"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value, 1, 4)||'-'||substring(id_value, 5, 6)||'-'||substring(id_value, 10, 2) 
        where id_value ~ '^20[0-9]{2}A[0-9]{7}$'
        and (id_desc <> 'AbbVie' or id_desc is null);"#;
    execute_sql(sql, pool).await?;

    // Finally add the newly constructed identifiers  

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'),
        id_type_id = 301,
        id_type = 'ANSM (ID-RCB) number',
        source_org_id = 101408,
        source_org = 'Agence Nationale de Sécurité du Médicament'
        where id_value ~ '20[0-9]{2}-A[0-9]{5}-[0-9]{2}'
        and (id_desc is null or id_desc ~ 'ANSM' or id_desc ~ 'RCB' or id_desc ~ 'Eudra CT')
        and id_type_id is null;"#;
    execute_sql_fb(sql, pool, "Additional ANSM", "found after repairing data").await?;

    // And also characterise the ill-formed ones

    let sql = r#"update ad.temp_idents
        set id_type_id = 2301,
        id_type = 'Malformed ANSM ID',
        source_org_id = 101408,
        source_org = 'Agence Nationale de Sécurité du Médicament'
        where (id_value ~ '20[0-9]{2}-A[0-9]{4}-[0-9]{2}'
        or id_value ~ '20[0-9]{2}-A[0-9]{6}-[0-9]{2}'
        or id_value ~ '20[0-9]{2}-A[0-9]{5}-[0-9]{1}$'
        or id_value ~ '20[0-9]{2}-A[0-9]{2,5}$'
        or id_value ~ '20[0-9]{2}-A$'
        or id_value ~ '20[0-9]{3}-A[0-9]{2,5}$')
        and (id_desc ~ 'ANSM' or id_desc ~ 'RCB')
        and id_type_id is null;"#;
    execute_sql_fb(sql, pool, "Malformed ANSM", "labelled").await?;

    info!("");

    Ok(())
}


pub async fn find_eu_wide_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[0-9]{2}-0[0-9]{5}-[0-9]{2}'),
        id_type_id = 123,
        id_type = 'EMA Eudract ID',
        source_org_id = 100159,
        source_org = 'European Medicines Agency'
        where id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}' 
        and (
        length(id_value) = 14
        or id_value ilike '%eu%'
        or id_value ilike '%udract%'
        or id_value ilike '%edract%')"#;
    execute_sql_fb(sql, pool, "EU CTR", "found and labelled").await?;
    

    // A group of other records include EU-CTR IDs, in some cases along with other
    // identifiers. The SQL below removes the other components into a small temporary 
    // table, adding them to the temp_idents table before replacing the original 
    // values with the EU-CTR IDs, and then encoding those.

    let sql = r#"SET client_min_messages TO WARNING; 
        drop table if exists ad.temp_adds;
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
        from ad.temp_adds;

        drop table ad.temp_adds;"#;
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
    execute_sql_fb(sql, pool, "Additional EU CTR", "found (in compound values)").await?;
    
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
        id_type = 'EMA Eudract ID',
        source_org_id = 100159,
        source_org = 'European Medicines Agency'
        where id_value ~ '20[0-9]{2}-0[0-9]{5}-[0-9]{2}' 
        and id_value ilike '%eudr%'
        and id_value not ilike '%eudramed%'
        and id_value not ilike '%eudract'"#;
    execute_sql_fb(sql, pool, "Additional EU CTR", "found after repairing data").await?;

    // identify obvious mis-formed and uncorrectable ones

    let sql = r#"update ad.temp_idents 
        set id_type_id = 2123,
        id_type = 'Malformed Eudract ID',
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
    execute_sql_fb(sql, pool, "Malformed EUDRACT", "labelled").await?;
    info!("");

    // CTIS number

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'),
        id_type_id = 135,
        id_type = 'EMA CTIS ID',
        source_org_id = 100159,
        source_org = 'European Medicines Agency'
        where id_value ~ '20[2|3][0-9]-5[0-9]{5}-[0-9]{2}'"#;
    execute_sql_fb(sql, pool, "EU CTIS", "found and labelled").await?;
    info!("");

    // Eudamed ID
    
    // Preliminary procesing 

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, 'CVI', 'CIV')
        where id_value ~ 'CVI-[0-9]{2}-[0-9]{2}-[0-9]{6}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, 'CVI ', 'CIV-')
        where id_value ~ 'CIV [0-9]{2}-[0-9]{2}-[0-9]{6}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, ' ', '-')
        where id_value ~ 'CIV-[0-9] [0-9]-[0-9]{2}-[0-9]{6}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, ' ', '')
        where id_value ~ 'CIV -[0-9]{2}-[0-9]{2}-[0-9]{6}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, ' ', '')
        where id_value ~ 'CIV- [0-9]{2}-[0-9]{2}-[0-9]{6}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, 'CIV-ID ', 'CIV-')
        where id_value ~ 'CIV-ID [0-9]{2}-[0-9]{2}-[0-9]{6}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(replace(id_value, ' ', ''), 'Cl', 'CI')
        where id_value ~ 'Cl V-1 3-03';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, '1-8', '18')
        where id_value ~ 'CIV-1-8-06';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'CIV-'||id_value
        where (id_desc ~ 'CIV' or id_desc ~* 'EudaMed')
        and id_value ~ '^[0-9]{2}-[0-9]{2}';"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
    set id_value = substring(id_value from 'CIV-[0-9]{2}-[0-9]{2}-[0-9]{6}'),
            id_type_id =  186,
            id_type = 'Eudamed ID',
            source_org_id = 100574,
            source_org = 'European Commission'
    where id_value ~ 'CIV-[0-9]{2}-[0-9]{2}-[0-9]{6}'"#;
    execute_sql_fb(sql, pool, "Eudamed", "found and labelled").await?;

    let sql = r#"update ad.temp_idents
    set id_value = substring(id_value from 'CIV-[A-Z]{2}-[0-9]{2}-[0-9]{2}-[0-9]{6}'),
            id_type_id =  186,
            id_type = 'Eudamed ID',
            source_org_id = 100574,
            source_org = 'European Commission'
    where id_value ~'CIV-[A-Z]{2}-[0-9]{2}-[0-9]{2}-[0-9]{6}'"#;
    execute_sql_fb(sql, pool, "Eudamed", "with country codes found and labelled").await?;

    let sql = r#"update ad.temp_idents
    set id_type_id =  2186,
        id_type = 'Malformed Eudamed ID',
        source_org_id = 100574,
        source_org = 'European Commission'
    where id_value ~'^CIV-'
    and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Malformed Eudamed", "labelled").await?;

    // Tidy up

    let sql = r#"update ad.temp_idents
    set id_desc = null
    where (id_desc ~ 'CIV' or id_desc ~* 'EudaMed')
    and id_type_id is null"#;
    execute_sql(sql, pool).await?;

    info!("");

    // EUPAS ID

    let sql = r#"update ad.temp_idents
        set id_value = replace(id_value, ' ', ''),
        id_type_id =  187,
        id_type = 'HMA-EMA RWD (EUPAS)',
        source_org_id = 100159,
        source_org = 'European Medicines Agency'
        where id_value ~ '^EUPAS[0-9]{3,12}'
        or id_value ~ '^EUPAS [0-9]{3,12}'"#;
    execute_sql_fb(sql, pool, "HMA-EMA RWD (EUPAS)", "found and labelled").await?;

    info!("");

    Ok(())
}


pub async fn find_dutch_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // DUTCH NTR

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
        id_type = 'Dutch CTR NTR ID (obs)',
        source_org_id = 0,
        source_org = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NTR[0-9]{1,4}'"#;
    execute_sql_fb(sql, pool, "NTR Dutch", "found and labelled").await?;    
    
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_desc from 'NTR[0-9]{4}')
        where id_desc ~ 'NTR[0-9]{4}'
        and id_type_id is null"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'NTR'||id_value
        where id_desc = 'NTR'
        and id_type_id is null"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NTR[0-9]{1,4}'),
        id_type_id = 181,
        id_type = 'Dutch CTR NTR ID (obs)',
        source_org_id = 0,
        source_org = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NTR[0-9]{1,4}'
        and id_value !~ '^NTR[0-9]{5}'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Additional NTR Dutch", "found after repairing data").await?; 

    let sql = r#"update ad.temp_idents
        set id_type_id = 2181,
        id_type = 'Malformed Dutch NTR ID',
        source_org_id = 0,
        source_org = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NTR[0-9]{5}'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Malformed Dutch NTR", "labelled").await?; 
    info!("");

    // DUTCH NL

    // Makes it easier if the CCMO approval numbers aer done first (they are also NL)
    // First tidy up data by making formatting more consistent, and turn the IDs that appear to 
    // be just a string of 10 numbers into a properly formatted CCMO identifier.
   
    let sql = r#"update ad.temp_idents set id_value = replace(id_value, ' ', '.')
        where id_value ~ 'NL[0-9]{5} [0-9]{3} [0-9]{2}';

		update ad.temp_idents set id_value = replace(id_value, '-', '.')
        where id_value ~ 'NL-[0-9]{5}-[0-9]{3}-[0-9]{2}';

		update ad.temp_idents set id_value = replace(id_value, ',', '.')
        where id_value ~ 'NL[0-9]{5},[0-9]{3},[0-9]{2}';

        update ad.temp_idents set id_value = replace(id_value, '_', '.')
        where id_value ~ 'NL[0-9]{5}_[0-9]{3}_[0-9]{2}';

		update ad.temp_idents set id_value = replace(id_value, 'NL.', 'NL')
        where id_value ~ 'NL\.[0-9]{5}\.[0-9]{3}\.[0-9]{2}';

        update ad.temp_idents set id_value = replace(id_value, 'NL nr.: ', 'NL')
        where id_value ~ 'NL nr\.: [0-9]{5}\.[0-9]{3}\.[0-9]{2}';
  
        update ad.temp_idents set id_value = replace(id_value, 'NL nr ', 'NL')
        where id_value ~ 'NL nr [0-9]{5}\.[0-9]{3}\.[0-9]{2}';

        update ad.temp_idents set id_value = replace(id_value, 'NL-', 'NL')
        where id_value ~ 'NL-[0-9]{5}\.[0-9]{3}\.[0-9]{2}';

        update ad.temp_idents set id_value = replace(id_value, 'NL ', 'NL')
        where id_value ~ 'NL [0-9]{5}\.[0-9]{3}\.[0-9]{2}';

		update ad.temp_idents set id_value = replace(id_value, 'NL-', 'NL')
        where id_value ~ 'NL-[0-9]{10}';
        
        update ad.temp_idents set id_value = REGEXP_REPLACE(id_value, 'NL[0-9]{10}', '')
        ||substring(substring(id_value from 'NL[0-9]{10}'), 1, 7)||'.'||substring(substring(id_value from 'NL[0-9]{10}'), 8, 3)
        ||'.'||substring(substring(id_value from 'NL[0-9]{10}'), 11, 2)
        where id_value ~ 'NL[0-9]{10}' and id_value !~ 'NL[0-9]{11}';"#;

    execute_sql(sql, pool).await?; 

    // Then create a temp table
    // that will hold the other IDs often (70+) found with the CCMO IDs.
    // Extract these and add to temp_idents.
    // Then turn id_values containing CCMO numbers into just those CCMO numbers.
    // and finally encode these as CCMO identiiers.

    let sql = r#"SET client_min_messages TO WARNING; 
        drop table if exists ad.temp_adds;
        create table ad.temp_adds as 
        select sd_sid, 
        replace(id_value, substring(id_value from 'NL[0-9]{5}\.[0-9]{3}\.[0-9]{2}'), '') as id_value,  
        id_class, id_desc
        from ad.temp_idents
        where id_value ~ 'NL[0-9]{5}\.[0-9]{3}\.[0-9]{2}' 
        and id_type_id is null
        and replace(id_value, substring(id_value from 'NL[0-9]{5}\.[0-9]{3}\.[0-9]{2}'), '') ~ '[1-9]';"#;
    execute_sql_fb(sql, pool, "Additional Dutch", "(found with CCMO records) added to temp_idents").await?;    

    let sql = r#"update ad.temp_adds set id_value = trim(id_value);
        update ad.temp_adds set id_value = trim(BOTH '-' from id_value);
        update ad.temp_adds set id_value = trim(BOTH '/' from id_value);
        update ad.temp_adds set id_value = trim(BOTH ',' from id_value);
        update ad.temp_adds set id_value = trim(BOTH '|' from id_value);
        update ad.temp_adds set id_value = trim(id_value);
        delete from ad.temp_adds where length(id_value) = 1;
        select * from ad.temp_adds;

        insert into ad.temp_idents
        (sd_sid, id_value, id_class, id_desc)
        select sd_sid, id_value, id_class, id_desc
        from ad.temp_adds;

        drop table ad.temp_adds;"#;
    execute_sql(sql, pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL[0-9]{5}\.[0-9]{3}\.[0-9]{2}')
        where id_value ~ 'NL[0-9]{5}\.[0-9]{3}\.[0-9]{2}'
        and id_type_id is null"#;
    execute_sql(sql, pool).await?; 

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL[0-9]{5}\.[0-9]{3}\.[0-9]{2}'),
        id_type_id = 351,
        id_type = 'Dutch CCMO ID',
        source_org_id = 109113,
        source_org = 'CCMO'
        where id_value ~ 'NL[0-9]{5}\.[0-9]{3}\.[0-9]{2}'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Dutch CCMO", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 2351,
        id_type = 'Malformed Dutch CCMO ID',
        source_org = 'CCMO'
        where (id_value ~ 'NL[0-9]{3,4}\.[0-9]{3}\.[0-9]{2}'
        or id_value ~ 'NL[0-9]{6}\.[0-9]{3}\.[0-9]{2}')
        and id_type_id is null;"#;
    execute_sql_fb(sql, pool, "Malformed Dutch CCMO", "found and labelled").await?;   
    info!("");

    // DUTCH NL
    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL[0-9]{4}'),
        id_type_id = 182,
        id_type = 'Dutch CTR NL ID (obs)',
        source_org_id = 0,
        source_org = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ '^NL[0-9]{4}'
        and length(id_value) < 7"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} NL Dutch identifiers found and labelled", res.rows_affected());	
    
    // DUTCH NL-OMON

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'NL-OMON[0-9]{1,5}'),
        id_type_id = 132,
        id_type = 'Dutch CTR NL-OMON ID',
        source_org_id = 0,
        source_org = 'Centrale Commissie Mensgebonden Onderzoek'
        where id_value ~ 'NL-OMON[0-9]{1,5}'"#;
    let res = execute_sql(sql, pool).await?;
    info!("{} NL-OMON Dutch identifiers found and labelled", res.rows_affected());	
    info!("");

    Ok(())
}


pub async fn find_german_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'DRKS[0-9]{8}'),
        id_type_id = 124,
        id_type = 'German DRKS ID',
        source_org_id = 105875,
        source_org = 'Federal Institute for Drugs and Medical Devices'
        where id_value ~ 'DRKS[0-9]{8}'"#;
    execute_sql_fb(sql, pool, "DRKS German", "found and labelled").await?;
    
    let sql = r#"update ad.temp_idents
        set id_value = 'DRKS'||id_value
        where id_desc ilike '%DRKS%'
        and id_type_id is null"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'DRKS[0-9]{8}'),
        id_type_id = 124,
        id_type = 'German DRKS ID',
        source_org_id = 105875,
        source_org = 'Federal Institute for Drugs and Medical Devices'
        where id_value ~ 'DRKS[0-9]{8}'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Additional DRKS German", "found after repairing data").await?;
    
    let sql = r#"update ad.temp_idents
        set id_type_id = 2124,
        id_type = 'Malformed German DRKS ID',
        source_org_id = 105875,
        source_org = 'Federal Institute for Drugs and Medical Devices'
        where (id_value ~ 'DRKS[0-9]{5,7}' or id_value ~ 'DRKS[0-9]{9}')
        and id_value !~ 'DRKS[0-9]{8}'"#;
    execute_sql_fb(sql, pool, "Malformed German", "labelled").await?;    
    info!("");

    Ok(())
}


pub async fn find_isrctn_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    replace_string_in_ident("ISCRTN", "ISRCTN", pool).await?;  
    replace_string_in_ident("isrctn", "ISRCTN", pool).await?; 
    replace_string_in_ident("ISRCTN : ", "ISRCTN", pool).await?;   // preliminary tidying (few recs)
    replace_string_in_ident("ISRCTN: ", "ISRCTN", pool).await?;  
    replace_string_in_ident("ISRCTN:", "ISRCTN", pool).await?;  
    replace_string_in_ident("ISRCTN ", "ISRCTN", pool).await?; 
    replace_string_in_ident("ISRCTN-", "ISRCTN", pool).await?;  

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ISRCTN[0-9]{8}'),
        id_type_id = 126,
        id_type = 'ISRCTN ID',
        source_org_id = 101421,
        source_org = 'Springer Nature'
        where id_value ~ 'ISRCTN[0-9]{8}'"#;
    execute_sql_fb(sql, pool, "ISRCTN", "found and labelled").await?;

    let sql = r#"update ad.temp_idents
        set id_value = 'ISRCTN'||id_value
        where id_desc ilike '%ISRCTN%'
        and id_value not like 'NCT%'
        and id_type_id is null"#;
    execute_sql(sql, pool).await?;

    let sql = r#"update ad.temp_idents
        set id_value = substring(id_value from 'ISRCTN[0-9]{8}'),
        id_type_id = 126,
        id_type = 'ISRCTN ID',
        source_org_id = 101421,
        source_org = 'Springer Nature'
        where id_value ~ 'ISRCTN[0-9]{8}'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Additional ISRCTN", "ound after repairing data").await?;
       
    let sql = r#"update ad.temp_idents
        set id_type_id = 2126,
        id_type = 'Malformed ISRCTN Id',
        source_org_id = 101421,
        source_org = 'Springer Nature'
        where (id_value ~ 'ISRCTN[0-9]{4,7}'
        or id_value ~ 'ISRCTN[0-9]{9,10}')
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "Malformed ISRCTN", "labelled").await?;
    info!("");

    Ok(())
}
