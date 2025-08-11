use sqlx::{postgres::PgQueryResult, Pool, Postgres};
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
        info!("{} {} identifiers {}", res.rows_affected(), s1, s2);
    }
    else {
        info!("{} {} identifier {}", res.rows_affected(), s1, s2);
    }
    
    Ok(())
}
*/

pub async fn execute_phased_transfer(sql: &str, max_id: u64, chunk_size: u64, sql_linker: &str, rec_type: &str, rec_dest: &str, pool: &Pool<Postgres>) -> Result<u64, AppError> {
    
    let mut rec_num: u64 = 0;
    let mut start_num: u64 = 0;

    while start_num <= max_id {

        let end_num = start_num + chunk_size;
        let chunk_sql = format!("c.nct_id >= 'NCT{:0>8}' and c.nct_id < 'NCT{:0>8}';", start_num, end_num);
        let chsql = sql.to_string() + sql_linker + &chunk_sql;
        
        let res = sqlx::raw_sql(&chsql).execute(pool)
            .await.map_err(|e| AppError::SqlxError(e, chsql.to_string()))?;
        let recs = res.rows_affected();
        rec_num += recs;
        info!("{} {} transferred to {}, {}", recs, rec_type, rec_dest, chunk_sql);

        start_num = end_num;
    }

    info!("{} {} transferred in total", rec_num, rec_type);
    info!("");

    Ok(rec_num)
}


/* 
pub async fn execute_phased_update(sql: &str, rec_num: u64, chunk_size: u64, fback: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let mut total_recs = 0;

    for i in (0..rec_num).step_by(chunk_size.try_into().unwrap()) {
        
        let start_num = i + 1000001;
        let mut end_num = start_num + chunk_size - 1;
        if end_num > rec_num + 1000001 {
            end_num = rec_num + 1000000;
        }

        let chunk_sql = format!("s.id >= {} and s.id <= {};", start_num, end_num);
        let chsql = sql.to_string() + " and " + &chunk_sql;
        let res = sqlx::raw_sql(&chsql).execute(pool)
            .await.map_err(|e| AppError::SqlxError(e, chsql.to_string()))?;
        info!("{} {}, {}", res.rows_affected(), fback, chunk_sql);
        total_recs += res.rows_affected();
    }

    info!("{} records affected in total", total_recs);
    info!("");

    Ok(())
}
*/


pub async fn vacuum_table (table: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // For studies table, normally leave until final updates...after iec_flag calculated

    let size_sql = format!("SELECT pg_size_pretty(pg_total_relation_size('ad.{}'));", table);
    let before: String = sqlx::query_scalar(&size_sql).fetch_one(pool)
        .await.map_err(|e| AppError::SqlxError(e, size_sql.clone()))?;
    
    let vac_sql =format!("VACUUM (FULL, ANALYZE) ad.{};", table);
    sqlx::raw_sql(&vac_sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, vac_sql))?;

    let after: String = sqlx::query_scalar(&size_sql).fetch_one(pool)
        .await.map_err(|e| AppError::SqlxError(e, size_sql))?;

    info!("vacuum carried out on {} table, (size changing from {} to {})", table, before, after);

    Ok(())
}