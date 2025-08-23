use sqlx::{Pool, Postgres, postgres::PgQueryResult};
use crate::AppError;
use log::info;


pub async fn execute_sql(sql: &str, pool: &Pool<Postgres>) -> Result<PgQueryResult, AppError> {
    
    sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))
}


pub async fn execute_sql_fb(sql: &str, pool: &Pool<Postgres>, 
            s1: &str, s2: &str) -> Result<(), AppError> {
    
    let res = sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    if res.rows_affected() > 1 {
        info!("{} {} identifiers {}", res.rows_affected(), s1, s2);
    }
    else {
        info!("{} {} identifier {}", res.rows_affected(), s1, s2);
    }
    
    Ok(())
}


pub async fn execute_sql_sfb(sql: &str, pool: &Pool<Postgres>, s: &str) -> Result<(), AppError> {
    
    let res = sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    if res.rows_affected() > 1 {
        info!("{} identifiers {}", res.rows_affected(), s);
    }
    else {
        info!("{} identifier {}", res.rows_affected(), s);
    }
    
    Ok(())
}


pub async fn execute_temp_phased_transfer(sql: &str, max_id: u64, chunk_size: u64, sql_linker: &str, rec_type: &str, pool: &Pool<Postgres>) -> Result<u64, AppError> {
    
    let mut rec_num: u64 = 0;
    let mut start_num: u64 = 0;

    while start_num <= max_id {

        let end_num = start_num + chunk_size;
        let chunk_sql = format!("c.sd_sid >= 'NCT{:0>8}' and c.sd_sid < 'NCT{:0>8}';", start_num, end_num);
        let chsql = sql.to_string() + sql_linker + &chunk_sql;
        
        let res = sqlx::raw_sql(&chsql).execute(pool)
            .await.map_err(|e| AppError::SqlxError(e, chsql.to_string()))?;
        let recs = res.rows_affected();
        rec_num += recs;
        info!("{} {} copied, {}", recs, rec_type, chunk_sql);

        start_num = end_num;
    }

    info!("{} {} transferred in total", rec_num, rec_type);
    info!("");

    Ok(rec_num)
}


pub async fn replace_string_in_ident(s1: &str, s2: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = format!(r#"update ad.temp_idents
        set id_value = replace(id_value, '{}', '{}')
        where id_value like '%{}%'"#, s1, s2, s1);
    let res = execute_sql(&sql, pool).await?.rows_affected();
    if res > 1 {
        info!("{} '{}'s replaced by '{}' in identifiers", res, s1, s2);
    }
    else {
        info!("{} '{}' replaced by '{}' in identifiers", res, s1, s2);
    }
    Ok(())
}


pub async fn remove_leading_char_from_ident(s: char, pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = format!(r#"update ad.temp_idents
        set id_value = trim(LEADING '{}' from id_value)
        where id_value like '{}%'"#, s, s);
    let res = execute_sql(&sql, pool).await?.rows_affected();
    if res > 1 {
        info!("{} '{}' characters removed from start of identifiers", res, s);
    }
    else {
        info!("{} '{}' character removed from start of identifiers", res, s);
    }
    Ok(())
}


pub async fn remove_both_ldtr_char_from_ident(s: char, pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = format!(r#"update ad.temp_idents
        set id_value = trim(BOTH '{}' from id_value)
        where id_value like '%{}' or id_value like '{}%'"#, s, s, s);
    let res = execute_sql(&sql, pool).await?.rows_affected();
    if res > 1 {
        info!("{} '{}' characters removed from start or end of identifiers", res, s);
    }
    else {
        info!("{} '{}' character removed from start or end of identifiers", res, s);
    }
    Ok(())
}


pub async fn switch_number_suffix_to_desc(s: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {  
let sql = format!(r#"update ad.temp_idents
            set id_desc = case 
                when id_desc is null then '{s}'
                else id_desc||', '||'{s}'
                end,
            id_value = trim(replace (id_value, '{s}', ''))
            where id_value ~ '{s}$'"#);
    let res = execute_sql(&sql, pool).await?.rows_affected();
    if res > 1 {
        info!("{} '{}' suffixes moved from id_value to id type description", res, s);
    }
    else {
        info!("{} '{}' suffix moved from id_value to id type description", res, s);
    }
    Ok(())
}


pub async fn transfer_coded_identifiers(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"insert into ad.study_identifiers (sd_sid, id_value, id_type_id, id_type, source_org_id, source_org, id_link)
                             select sd_sid, id_value, id_type_id, id_type, source_org_id, source_org, id_link 
                             from ad.temp_idents 
                             where id_type_id is not null "#;   
    execute_sql(sql, pool).await?;

    let sql = r#"delete from ad.temp_idents 
                             where id_type_id is not null "#;   
    execute_sql_sfb(sql, pool, "transferred from temp_idents to study_identifiers table").await?;
        
    info!("");
    Ok(())
}
