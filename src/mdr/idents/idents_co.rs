//use super::utils::{execute_sql};
use super::idents_utils::{execute_sql, execute_sql_fb};

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;

pub async fn find_eli_lilly_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
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


pub async fn find_incyte_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    let sql = r#"update ad.temp_idents a
        set id_type_id = 702,
        id_type = 'InCyte Study ID',
        source_org_id = 100524,
        source_org = 'Incyte Corporation'
        from ctgov.sponsors s
        where a.sd_sid = s.nct_id
        and (a.id_value ~ '^INCB'
            or a.id_value ~ '^INCA'
            or a.id_value ~ '^INCMGA'
            or a.id_value ~ '^INCMOR'
            or a.id_value ~ '^I(-| )[0-9]{5}-[0-9]{2}-[0-9]{2}'
            or a.id_value ~ '^[0-9]{5}-[0-9]{3}$'
            or a.id_value ~ '^MOR'
        )
        and a.id_type_id is null
        and s.name ilike 'Incyte%'
        "#;
    execute_sql_fb(sql, pool, "InCyte study", "found and labelled").await?;  

    info!("");    
    Ok(())
}


pub async fn find_novartis_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    // Initial group is IDs where Nopvartis is the sponsor

    let sql = r#"update ad.temp_idents a
        set id_type_id = 703,
        id_type = 'Novartis study ID',
        source_org_id = 100189,
        source_org = 'Novartis'
        from ctgov.sponsors s
        where a.sd_sid = s.snct_id
        and (a.id_value ~ '^C[A-Z]{3}[A-Z0-9]{8,10}'
            or a.id_value ~ '^[0-9]{3}(-| )(P|A|D|G)(-| )[0-9]{3}'
            or a.id_value ~ '^V[0-9]{2}'
            or a.id_value ~ '^M[0-9]{2}'
            or a.id_value ~ '^PRSW-'
            or a.id_value ~ '^NOVARTIS-C'
            or a.id_value ~ '^EGF[0-9]{5}'
            or a.id_value ~ '^H01'
            or a.id_value ~ '^NV'
            or a.id_value ~ '^VOPO'
            or a.id_value ~ '^VOSG'
        )
        and a.id_type_id is null
        and s.name ilike 'Novart%';"#;
    let res1 = execute_sql(sql, pool).await?;

    // Tidy these few  up

    let sql = r#"update ad.temp_idents a
    set id_value = replace(id_value, 'NOVARTIS-', '')
        where id_type_id = 703 
        and id_value ~ '^NOVARTIS-C';"#;
    execute_sql(sql, pool).await?;

    // Addiitonal Novartis IDs when they are not listed as the sponsor
    // There are many exceptions, however, to the basic mmatching regexp

    let sql = r#"update ad.temp_idents a
        set id_type_id = 703,
        id_type = 'Novartis study ID',
        source_org_id = 100189,
        source_org = 'Novartis'
        where id_value ~ '^C[A-Z]{3}[0-9][A-Z0-9]{7,9}'
        and id_value !~ '^CNTO[A-Z0-9]{8,10}'
        and id_value !~ '^CAAE[A-Z0-9]{8,10}'
        and id_value !~ '^CATS[A-Z0-9]{8,10}'
        and id_value !~ '^CC[A-Z0-9]{8,10}'
        and id_value !~ '^CAUT[A-Z0-9]{8,10}'
        and id_value !~ '^CIBI[A-Z0-9]{8,10}'
        and id_value !~ '^CILB[A-Z0-9]{8,10}'
        and id_value !~ '^CGME[A-Z0-9]{8,10}'
        and id_value !~ '^CBIR[A-Z0-9]{8,10}'
        and id_value !~ '^CBEZ[A-Z0-9]{8,10}'
        and id_value !~ '^COPD[A-Z0-9]{8,10}'
        and id_value !~ '^CSPP[A-Z0-9]{8,10}'
        and id_value !~ '^CSTB[A-Z0-9]{8,10}'
        and id_value !~ '^CSCR[A-Z0-9]{8,10}'
        and id_value !~ '^CTOR[A-Z0-9]{8,10}'
        and id_value !~ '^CWNT[A-Z0-9]{8,10}'
        and id_type_id is null"#;
    let res2 = execute_sql(sql, pool).await?;

    info!("{} Novartis study identifiers found and labelled", res1.rows_affected() + res2.rows_affected()); 
    info!("");    

    Ok(())

}


pub async fn find_alcon_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    let sql = r#"update ad.temp_idents a
        set id_type_id = 704,
        id_type = 'Alcon Research study ID',
        source_org_id = 100271,
        source_org = 'Alcon Research'
        from ctgov.sponsors s
        where a.sd_sid = s.nct_id
        and (id_value ~ '^CL[A-Z][0-9]{3}-[A-Z0-9]{4}'
        or id_value ~ '^CT[A-Z][0-9]{3}-[A-Z0-9]{4}'
        or id_value ~ '^DE[A-Z][0-9]{3}-[A-Z0-9]{4}'
        or id_value ~ '^EX[A-Z][0-9]{3}'
        or id_value ~ '^GL[A-Z][0-9]{3}'
        or id_value ~ '^IL[A-Z][0-9]{3}'
        or id_value ~ '^LC[A-Z][0-9]{3}-'
        or id_value ~ '^LK[A-Z][0-9]{3}-'
        or id_value ~ '^LM[A-Z][0-9]{3}-'
        or id_value ~ '^RF[A-Z][0-9]{3}-'
        or id_value ~ '^C-[0-9]{2}-[0-9]{2,3}'
        or id_value ~ '^CM-[0-9]{2}-[0-9]{2}'
        or id_value ~ '^P-[0-9]{2}-[0-9]{2}'
        or id_value ~ '^J-[0-9]{2}-[0-9]{2,3}'
        or id_value ~ '^M-[0-9]{2}-[0-9]{2,3}'
        or id_value ~ '^M[0-9]{2}-[0-9]{2,3}'  
        or id_value ~ '^CMS-[0-9]{2}-[0-9]{2}'
        or id_value ~ '^MS-[0-9]{2}'
        or id_value ~ '^RM-[0-9]{2}'
        or id_value ~ '^EMD-[0-9]{2}-[0-9]{2}'
        or id_value ~ '^RDG-[0-9]{2}-[0-9]{2,3}'
        or id_value ~ '^SMA-[0-9]{2}-[0-9]{2,3}'
        or id_value ~ '^A[0-9]{5}$'
        or id_value ~ '^ALCON'
        or id_value ~ '^ALJ'
        )
        and a.id_type_id is null
        and s.name ilike 'Alcon%'
        "#;
    execute_sql_fb(sql, pool, "Alcon Research study", "found and labelled").await?;  
    
    info!("");    
    Ok(())
}


pub async fn find_pfizer_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
 
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = 100164,
        source_org = 'Pfizer'
        from ctgov.sponsors s
        where a.sd_sid = s.nct_id
        and a.id_value ~ '^(A|B|C)[0-9]{7}$'
        and a.id_type_id is null
        and s.name ilike '%Pfizer%'"#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
 
    



    info!("");   
    Ok(())
}

pub async fn find_gsk_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    /* 
    706		"GSK ID"
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = ,
        source_org = 'Pfizer'
        from ad.lead_orgs s
        where a.sd_sid = s.sd_sid
        and (
        )
        and a.id_type_id is null
        and s.name ilike '%'
        "#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
    
    info!("");   
    */  
    Ok(())
}

pub async fn find_roche_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    /* 
    707		"Roche ID"
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = ,
        source_org = 'Pfizer'
        from ad.lead_orgs s
        where a.sd_sid = s.sd_sid
        and (
        )
        and a.id_type_id is null
        and s.name ilike '%'
        "#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
    
    info!("");   
    */  
    Ok(())
}

pub async fn find_az_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    /* 
    708		"Astra Zeneca ID"
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = ,
        source_org = 'Pfizer'
        from ad.lead_orgs s
        where a.sd_sid = s.sd_sid
        and (
        )
        and a.id_type_id is null
        and s.name ilike '%'
        "#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
    
    info!("");   
    */  
    Ok(())
}

pub async fn find_takeda_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    /* 
   709		"Takeda ID"
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = ,
        source_org = 'Pfizer'
        from ad.lead_orgs s
        where a.sd_sid = s.sd_sid
        and (
        )
        and a.id_type_id is null
        and s.name ilike '%'
        "#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
    
    info!("");   
    */  
    Ok(())
}

pub async fn find_jandj_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    /* 
   710		"Johnson and Johnson ID"
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = ,
        source_org = 'Pfizer'
        from ad.lead_orgs s
        where a.sd_sid = s.sd_sid
        and (
        )
        and a.id_type_id is null
        and s.name ilike '%'
        "#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
    
    info!("");   
    */  
    Ok(())
}


pub async fn find_jannsen_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    /* 
   711		"Jannsen ID"
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = ,
        source_org = 'Pfizer'
        from ad.lead_orgs s
        where a.sd_sid = s.sd_sid
        and (
        )
        and a.id_type_id is null
        and s.name ilike '%'
        "#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
    
    info!("");   
    */  
    Ok(())
}

pub async fn find_sanofi_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    /* 
   712		"Sanofi-Aventis ID"
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = ,
        source_org = 'Pfizer'
        from ad.lead_orgs s
        where a.sd_sid = s.sd_sid
        and (
        )
        and a.id_type_id is null
        and s.name ilike '%'
        "#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
    
    info!("");   
    */  
    Ok(())
}

pub async fn find_bms_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    /* 
   713		"BMS ID"
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = ,
        source_org = 'Pfizer'
        from ad.lead_orgs s
        where a.sd_sid = s.sd_sid
        and (
        )
        and a.id_type_id is null
        and s.name ilike '%'
        "#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
    
    info!("");   
    */  
    Ok(())
}

pub async fn find_abbvie_identities(_pool: &Pool<Postgres>) -> Result<(), AppError> {  

    /* 
   714		"AbbVie ID"
    let sql = r#"update ad.temp_idents a
        set id_type_id = 705,
        id_type = 'Pfizer ID',
        source_org_id = ,
        source_org = 'Pfizer'
        from ad.lead_orgs s
        where a.sd_sid = s.sd_sid
        and (
        )
        and a.id_type_id is null
        and s.name ilike '%'
        "#;
    execute_sql_fb(sql, pool, "Pfizer study", "found and labelled").await?;  
    
    info!("");   
    */  
    Ok(())
}