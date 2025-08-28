use super::idents_utils::{execute_sql, execute_sql_fb};


use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;


pub async fn find_swog_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    // Order in which the routines below are executed is important, as often the
    // SQL relies on identifying those records that have notr yet been identified.
    // Therefore exercise caution if changing the given order.

    // CSWOG - test change

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
    execute_sql_fb(sql, pool, "Ad hoc collaboration", "found and labelled").await?;  

    info!("");    
    Ok(())

}


pub async fn find_cog_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    // CECOG

    let sql = r#"update ad.temp_idents
        set id_value = replace(replace(id_value, '/', ' '), '  ', ' '),
        id_type_id = 196,
        id_type = 'Central European Cooperative Onc. Group ID',
        source_org_id = 0,
        source_org = 'Central European Cooperative Oncology Group'
        where id_value ~ '^CECOG'"#;
    execute_sql_fb(sql, pool, "Central European COG", "found and labelled").await?;  

    // ECOG & ACRIN

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
    let res1 = execute_sql(sql, pool).await?;

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
    let res2 = execute_sql(sql, pool).await?;

    info!("{} ad hoc collaboration IDs found and labelled", res1.rows_affected() + res2.rows_affected());
    info!("");    
    Ok(())
}


pub async fn find_can_and_tog_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
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
    let res1 = execute_sql(sql, pool).await?;

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
    let res2 = execute_sql(sql, pool).await?;


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
    let res3 = execute_sql(sql, pool).await?;

    let r = res1.rows_affected() + res2.rows_affected() + res3.rows_affected();
    info!("{} ad hoc collaboration IDs found and labelled", r);    
    
    info!("");    
    Ok(())
}


pub async fn find_nat_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
     // NCCTG

    let sql = r#"update ad.temp_idents
        set id_type_id = 198,
        id_type = 'North Central Cancer Treatment Gr. ID (US)',
        source_org_id = 0,
        source_org = 'North Central Cancer Treatment Group'
        where id_value ~ '^NCCTG'
        and id_value !~ '-MA'
        and id_value !~ 'NSABP'"#;
    execute_sql_fb(sql, pool, "NCCTG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'NCCTG'
        and id_type_id is null"#;
    let res1 = execute_sql(sql, pool).await?;


    // NSABP

    let sql = r#"update ad.temp_idents
        set id_type_id = 210,
        id_type = 'National Surgical Adjuvant Breast and Bowel Project ID',
        source_org_id = 101689,
        source_org = 'National Surgical Adjuvant Breast and Bowel Project'
        where id_value ~ '^NSABP'
        and id_value !~ 'GBG'
        and id_value !~ 'MA'"#;
    execute_sql_fb(sql, pool, "NSABP", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'NSABP'
        and id_type_id is null"#;
    let res2 = execute_sql(sql, pool).await?;


    // NABTC

    let sql = r#"update ad.temp_idents
        set id_type_id = 203,
        id_type = 'North American Brain Tumor Consortium ID',
        source_org_id = 0,
        source_org = 'North American Brain Tumor Consortium'
        where id_value ~ '^NABTC'"#;
    execute_sql_fb(sql, pool, "NABTC", "found and labelled").await?;  


    // ACOSOG

    let sql = r#"update ad.temp_idents
        set id_type_id = 197,
        id_type = 'American College of Surgeons Onc. Grp. ID',
        source_org_id = 0,
        source_org = 'American College of Surgeons Oncology Group'
        where id_value ~ '^ACOSOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "ACOSOG", "found and labelled").await?;  


    // JH - NABTT

    let sql = r#"update ad.temp_idents
        set id_type_id = 212,
        id_type = 'John Hopkins - NABTT  collab. ID',
        source_org_id = 0,
        source_org = 'John Hopkins Cancer Center / New Approaches to Brain Tumor Therapy project collaboration'
        where id_value ~ 'JHOC-NABTT'
        or id_value ~ 'JHU-NABTT'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "JHOC-NABTT", "found and labelled").await?;  

    // NABTT

    let sql = r#"update ad.temp_idents
        set id_type_id = 202,
        id_type = 'New Approaches to Brain Tumor Therapy ID (US)',
        source_org_id = 0,
        source_org = 'New Approaches to Brain Tumor Therapy'
        where id_value ~ 'NABTT'
        and id_value !~ 'IXR'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "NABTT", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'NABTT'
        and id_type_id is null"#;
    let res3 = execute_sql(sql, pool).await?;

    // JH CC

    let sql = r#"update ad.temp_idents
        set id_type_id = 211,
        id_type = 'John Hopkins Cancer Center ID',
        source_org_id = 0,
        source_org = 'Sidney Kimmel Comprehensive Cancer Center at Johns Hopkins"'
        where (id_value ~ '^JHOC'
        or id_value ~ '^SKCCC')
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "John Hopkins Cancer Center", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where (id_value ~ 'JHOC'  
        or id_value ~ 'SKCCC')
        and id_type_id is null"#;
    let res4 = execute_sql(sql, pool).await?;

    let r = res1.rows_affected() + res2.rows_affected() 
            + res3.rows_affected() + res4.rows_affected();
    info!("{} ad hoc collaboration IDs found and labelled", r);    
    info!("");    
    Ok(())

}


pub async fn find_boog_and_trog_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    // BOOG

    let sql = r#"update ad.temp_idents
        set id_type_id = 206,
        id_type = 'Borstkanker Onderzoek Groep ID',
        source_org_id = 103867,
        source_org = 'Borstkanker Onderzoek Groep (Dutch Breast Cancer Research Group)'
        where id_value ~ '^BOOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "BOOG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'BOOG'
        and id_type_id is null"#;
    let res1 = execute_sql(sql, pool).await?;


    // CHNMC

    let sql = r#"update ad.temp_idents
        set id_type_id = 213,
        id_type = 'City of Hope NMC ID',
        source_org_id = 100355,
        source_org = 'City of Hope National Medical Center'
        where id_value ~ '^CHNMC'"#;
    execute_sql_fb(sql, pool, "City of Hope NMC", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'CHNMC'
        and id_type_id is null"#;
    let res2 = execute_sql(sql, pool).await?;

    // TROG

    let sql = r#"update ad.temp_idents
        set id_type_id = 207,
        id_type = 'Trans Tasman Radiation Onc. Group ID',
        source_org_id = 104543,
        source_org = 'Trans Tasman Radiation Oncology Group '
        where id_value ~ '^TROG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "TROG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ '-TROG'
        and id_type_id is null"#;
    let res3 = execute_sql(sql, pool).await?;

    let r = res1.rows_affected() + res2.rows_affected() + res3.rows_affected();
    info!("{} ad hoc collaboration IDs found and labelled", r);    
    info!("");    
    Ok(())
}



pub async fn find_gog_and_nrj_identities(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    // ANZGOG

    let sql = r#"update ad.temp_idents
        set id_type_id = 209,
        id_type = 'Australia NZ Gynaecological Onc. Group ID',
        source_org_id = 0,
        source_org = 'Australia New Zealand Gynaecological Oncology Group'
        where id_value ~ '^ANZGOG'
        and id_value !~ '-GOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "ANZGOG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'ANZGOG'
        and id_type_id is null"#;
    let res1 = execute_sql(sql, pool).await?;


    // Japanese Gynecologic Oncology Group

    let sql = r#"update ad.temp_idents
        set id_type_id = 214,
        id_type = 'Japanese Gynae Group ID',
        source_org_id = 0,
        source_org = 'Japanese Gynecologic Oncology Group'
        where id_value ~ '^JGOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "JGOG", "found and labelled").await?;  

    // Korean Gynecologic Oncology Group

    let sql = r#"update ad.temp_idents
        set id_type_id = 215,
        id_type = 'Korean Gynae Group ID',
        source_org_id = 0,
        source_org = 'Korean Gynecologic Oncology Group'
        where id_value ~ 'KGOG'
        and id_value !~ 'JGOG'
        and id_value !~ 'THAI'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "KGOG", "found and labelled").await?;  

    // Shanghai Gynecologic Oncology Group

    let sql = r#"update ad.temp_idents
        set id_type_id = 216,
        id_type = 'Shanghai Gynae Group ID',
        source_org_id = 0,
        source_org = 'Shanghai Gynecologic Oncology Group'
        where id_value ~ '^SGOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "SGOG", "found and labelled").await?;  

    // Taiwanese Gynecologic Oncology Group

    let sql = r#"update ad.temp_idents
        set id_type_id = 217,
        id_type = 'Taiwanese Gynae Group ID',
        source_org_id = 0,
        source_org = 'Taiwanese Gynecologic Oncology Group'
        where id_value ~ '^TGOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "TGOG", "found and labelled").await?;  

    // New York Gynecologic Oncology Group

    let sql = r#"update ad.temp_idents
        set id_type_id = 218,
        id_type = 'New York Gynae Group ID',
        source_org_id = 0,
        source_org = 'New York Gynecologic Oncology Group'
        where id_value ~ '^NYGOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "NYGOG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where (id_value ~ 'JGOG'
        or id_value ~ 'KGOG' or id_value ~ 'SGOG' 
        or id_value ~ 'TGOG' or id_value ~ 'NYGOG')
        and id_value !~ 'ANSGOG'
        and id_type_id is null"#;
    let res2 = execute_sql(sql, pool).await?;

    // Chinese Gastrointestinal Oncology Group 

    let sql = r#"update ad.temp_idents
        set id_type_id = 219,
        id_type = 'Chinese GI Onc. Group ID',
        source_org_id = 0,
        source_org = 'Chinese Gastrointestinal Oncology Group'
        where id_value ~ '^CGOG'
        or id_value ~ '-CGOG'"#;
    execute_sql_fb(sql, pool, "CGOG", "found and labelled").await?;  


    // BGOG Belgium and Luxembourg Gynaecological Oncology Group

    let sql = r#"update ad.temp_idents
        set id_type_id = 221,
        id_type = 'Belgium and Lux. Gynael Onc. Group ID',
        source_org_id = 0,
        source_org = 'Belgium and Luxembourg Gynaecological Oncology Group'
        where id_value ~ '^BGOG'
        and id_value !~ '/'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "BGOG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'BGOG'
        and id_type_id is null"#;
    let res3 = execute_sql(sql, pool).await?;


    // CEEGOG Central and Eastern European Gynecologic Oncology Group

    let sql = r#"update ad.temp_idents
        set id_type_id = 222,
        id_type = 'Central and E European Gynae Onc. Group ID',
        source_org_id = 0,
        source_org = 'Central and Eastern European Gynecologic Oncology Group'
        where id_value ~ '^CEEGOG'
        and id_type_id is null"#;
    execute_sql_fb(sql, pool, "CEEGOG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
        set id_type_id = 220,
        id_type = 'Ad Hoc Collaboration ID',
        source_org_id = 0,
        source_org = 'Ad hoc Collaboration of research organisations'
        where id_value ~ 'CEEGOG'
        and id_type_id is null"#;
    let res4 = execute_sql(sql, pool).await?;


    // CQGOG  Chongqing Gynecologic Oncology Group (university cancer hospital)

    let sql = r#"update ad.temp_idents
            set id_type_id = 224,
            id_type = 'Chongqing Gynae Group ID',
            source_org_id = 0,
            source_org = 'Chongqing Gynecologic Oncology Group'
            where id_value ~ '^CQGOG'
            and id_type_id is null"#;
    execute_sql_fb(sql, pool, "CQGOG", "found and labelled").await?;  

    // ENGOT

    let sql = r#"update ad.temp_idents
            set id_type_id = 223,
            id_type = 'European Network of Gynaec Onco. Trial Groups ID',
            source_org_id = 0,
            source_org = 'European Network of Gynaecological Oncological Trial Groups'
            where id_value ~ '^ENGOT'
            and id_value !~ '/'
            and id_value !~ 'DGCG'
            and id_type_id is null"#;
    execute_sql_fb(sql, pool, "ENGOT", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
            set id_type_id = 220,
            id_type = 'Ad Hoc Collaboration ID',
            source_org_id = 0,
            source_org = 'Ad hoc Collaboration of research organisations'
            where id_value ~ 'ENGOT'
            and id_type_id is null"#;
    let res5 = execute_sql(sql, pool).await?;
  
    // GOG

    let sql = r#"update ad.temp_idents
            set id_type_id = 192,
            id_type = 'Gynae Onc. Group ID (US)',
            source_org_id = 0,
            source_org = 'Gynecologic Oncology Group'
            where id_value ~ '^GOG'
            and id_value !~ 'INCB'
            and id_type_id is null"#;
    execute_sql_fb(sql, pool, "GOG", "found and labelled").await?;  

    let sql = r#"update ad.temp_idents
            set id_type_id = 220,
            id_type = 'Ad Hoc Collaboration ID',
            source_org_id = 0,
            source_org = 'Ad hoc Collaboration of research organisations'
            where id_value ~ 'GOG'
            and id_value !~ 'AGOG'
            and id_value !~ 'ANSGOG'
            and id_value !~ 'LGOG'
            and id_type_id is null"#;
    let res6 = execute_sql(sql, pool).await?;

    // NRG Oncology

    let sql = r#"update ad.temp_idents
            set id_type_id = 225,
            id_type = 'NRG Oncology ID',
            source_org_id = 101420,
            source_org = 'NRG Oncology (US)'
            where id_value ~ '^NRG-'
            and id_type_id is null-- NRG Oncology"#;
    execute_sql_fb(sql, pool, "NRG Oncology", "found and labelled").await?;  

    let r = res1.rows_affected() + res2.rows_affected() + res3.rows_affected() 
               + res4.rows_affected() + res5.rows_affected() + res6.rows_affected();
    info!("{} ad hoc collaboration IDs found and labelled", r);

    info!("");    
    Ok(())
}



