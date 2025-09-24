use sqlx::{Pool, Postgres, postgres::PgQueryResult};
use crate::AppError;
use log::info;


pub async fn execute_sql(sql: &str, pool: &Pool<Postgres>) -> Result<PgQueryResult, AppError> {
    
    sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))
}


pub async fn replace_in_fac_proc(s1: &str, s2: &str, sql_where: &str, rb: bool, fb: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    let sql1 = format!("update ad.locs set fac_proc = replace(fac_proc, '{}', '{}') where ", s1, s2);
    let sql2 = match sql_where {
        "r" => format!(" fac_proc ~ '{}'", s1),
        "k" => format!(" fac_proc like '%{}%'", s1),
        "b" => format!(" fac_proc ~ '^{}'", s1),
        "e" => format!(" fac_proc ~ '{}$'", s1),
        _ => format!("{};", sql_where),
    };
    let sql = sql1 + &sql2 + "; ";

    let r = sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();
    
    // Feedback line provided (unless rb = false and fb = "")

    let recs = if r==1 {"record"} else {"records"};
    if rb {
        if fb != "" {
            info!("{} {} had '{}' replaced by '{}' {}", r, recs, s1, s2, fb);
        }
        else {
            info!("{} {} had '{}' replaced by '{}'", r, recs, s1, s2);
        }
    }
    else {
        if fb != "" {
            info!("{} {} had {}", r, recs, fb);
        }
    }

        
    Ok(())
}


pub async fn replace_regex_in_fac_proc(s1: &str, s2: &str, pool: &Pool<Postgres>, with_fb: bool) -> Result<(), AppError> {

    let sql = format!(r#"update ad.locs
	        set fac_proc = replace(fac_proc, '{}', '{}') where fac_proc ~ '{}' "#, s1, s2, s1); 

    let r = sqlx::raw_sql(&sql).execute(pool)
           .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();

    if with_fb {
        let action_string: String = if s2 == "" {"removed".to_string()} else {format!("replaced by '{}'", s2)};
        let recs = if r==1 {"record"} else {"records"};
        info!("{} {} had '{}'s {}", r, recs, s1, action_string);
    }

    Ok({})
}


pub async fn replace_like_in_fac_proc(s1: &str, s2: &str, pool: &Pool<Postgres>, with_fb: bool) -> Result<(), AppError> {

    let sql = format!(r#"update ad.locs
	        set fac_proc = replace(fac_proc, '{}', '{}') where fac_proc like '%{}%' "#, s1, s2, s1); 

    let r = sqlx::raw_sql(&sql).execute(pool)
           .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();

    if with_fb {
        let action_string: String = if s2 == "" {"removed".to_string()} else {format!("replaced by '{}'", s2)};
        let recs = if r==1 {"record"} else {"records"};
        info!("{} {} had '{}'s {}", r, recs, s1, action_string);
    }

    Ok({})
}


pub async fn remove_leading_char_in_fac_proc(s: &str, pool: &Pool<Postgres>, with_fb: bool) -> Result<(), AppError> {

    let mut sql = format!(r#"update ad.locs set fac_proc = substring(fac_proc, 2) where fac_proc ~ '^{}'; "#, s); 
    
    if s == "." {
        sql = format!(r#"update ad.locs set fac_proc = substring(fac_proc, 2) where fac_proc ~ '^\.'; "#); 
    } 

    let r = sqlx::raw_sql(&sql).execute(pool)
           .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();
    
    if with_fb {
        if r > 1 {
            info!("{} records had leading '{}'s removed", r, s);
        }
        else {
            info!("{} record had leading '{}' removed", r, s);
        }
    }

    Ok({})
}


pub async fn remove_trailing_char_in_fac_proc(s: &str, pool: &Pool<Postgres>, with_fb: bool) -> Result<(), AppError> {

    let mut sql = format!(r#"update ad.locs set fac_proc = substring(fac_proc, 1, length(fac_proc) - 1) where fac_proc ~ '{}$'; "#, s); 
    
    if s == "." {
        sql = format!(r#"update ad.locs set fac_proc = substring(fac_proc, 1, length(fac_proc) - 1) where fac_proc ~ '\.$'; "#); 
    } 

    let r = sqlx::raw_sql(&sql).execute(pool)
           .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();

    if with_fb {
        if r > 1 {
            info!("{} records had trailing '{}'s removed", r, s);
        }
        else {
            info!("{} record had trailing '{}' removed", r, s);
        }
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

