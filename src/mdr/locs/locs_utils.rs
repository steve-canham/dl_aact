use sqlx::{Pool, Postgres, postgres::PgQueryResult};
use crate::AppError;
use log::info;


pub async fn execute_sql(sql: &str, pool: &Pool<Postgres>) -> Result<PgQueryResult, AppError> {
    
    sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))
}


/*
pub async fn execute_sql_fb(sql: &str, pool: &Pool<Postgres>, 
            s1: &str, s2: &str) -> Result<(), AppError> {
    
    let res = sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    if res.rows_affected() > 1 {
        info!("{} {} locations {}", res.rows_affected(), s1, s2);
    }
    else {
        info!("{} {} location {}", res.rows_affected(), s1, s2);
    }
    
    Ok(())
}
*/


pub async fn replace_regex_in_fac_proc(s1: &str, s2: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = format!(r#"update ad.locs
	        set fac_proc = replace(fac_proc, '{}', '{}') where fac_proc ~ '{}' "#, s1, s2, s1); 

    sqlx::raw_sql(&sql).execute(pool)
           .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok({})
}


pub async fn replace_regex_in_fac_proc_fb(s1: &str, s2: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = format!(r#"update ad.locs
	        set fac_proc = replace(fac_proc, '{}', '{}') where fac_proc ~ '{}' "#, s1, s2, s1); 

    let res = sqlx::raw_sql(&sql).execute(pool)
           .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    if res.rows_affected() > 1 {
        info!("{} {} replaced by {} in fac_proc", res.rows_affected(), s1, s2);
    }
    else {
        info!("{} {}s replaced by {} in fac_proc", res.rows_affected(), s1, s2);
    }
    Ok({})
}


pub async fn replace_like_in_fac_proc_fb(s1: &str, s2: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = format!(r#"update ad.locs
	        set fac_proc = replace(fac_proc, '{}', '{}') where fac_proc like '%{}%' "#, s1, s2, s1); 

    let res = sqlx::raw_sql(&sql).execute(pool)
           .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    if res.rows_affected() > 1 {
        info!("{} {} replaced by {} in fac_proc", res.rows_affected(), s1, s2);
    }
    else {
        info!("{} {}s replaced by {} in fac_proc", res.rows_affected(), s1, s2);
    }
    Ok({})
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

