//use super::utils::{execute_sql};
use super::idents_utils::{execute_sql_fb};

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