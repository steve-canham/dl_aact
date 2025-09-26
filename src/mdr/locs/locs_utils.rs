use sqlx::{Pool, Postgres, postgres::PgQueryResult};
use crate::AppError;
use log::info;


pub async fn execute_sql(sql: &str, pool: &Pool<Postgres>) -> Result<PgQueryResult, AppError> {
    
    sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))
}


pub async fn replace_in_fac_proc(s1: &str, s2: &str, sql_where: &str, rb: bool, fb: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    let sql1 = format!("update ad.locs set fac_proc = replace(fac_proc, '{}', '{}') where ", s1, s2);
   
    let s = if s1.contains(".") { &s1.replace(".", r"\.") } else { s1 };
    let sql2 = match sql_where {
        "r" => format!(" fac_proc ~ '{}'", s),
        "k" => format!(" fac_proc like '%{}%'", s),
        "b" => format!(" fac_proc ~ '^{}'", s),
        "e" => format!(" fac_proc ~ '{}$'", s),
        _ => format!("{};", sql_where),
    };
    let sql = sql1 + &sql2 + "; ";

    let r = sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();
    
    // Feedback line provided (unless rb = false and fb = "")

    let recs = if r==1 {"record"} else {"records"};
    let had_string = match sql_where {
        "b" => "had initial",
        "e" => "had trailing", 
        _ => "had",
    };
    let action_string: String = if s2 == "" {"removed".to_string()} else {format!("replaced by '{}'", s2)};

    if rb {
        if fb != "" {
            info!("{} {} {} '{}' {} {}", r, recs, had_string, s1, action_string, fb);
        }
        else {
            info!("{} {} {} '{}' {}", r, recs, had_string, s1, action_string);
        }
    }
    else {
        if fb != "" {
            info!("{} {} {} {}", r, recs, had_string, fb);
        }
    }
        
    Ok(())
}


pub async fn remove_leading_char_in_fac_proc(s: &str, fb: bool, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let mut sql = format!(r#"update ad.locs set fac_proc = substring(fac_proc, 2) where fac_proc ~ '^{}'; "#, s); 
    
    if s == "." {
        sql = format!(r#"update ad.locs set fac_proc = substring(fac_proc, 2) where fac_proc ~ '^\.'; "#); 
    } 

    let r = sqlx::raw_sql(&sql).execute(pool)
           .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();
    
    if fb {
        if r > 1 {
            info!("{} records had leading '{}'s removed", r, s);
        }
        else {
            info!("{} record had leading '{}' removed", r, s);
        }
    }

    Ok({})
}


pub async fn remove_trailing_char_in_fac_proc(s: &str, fb: bool, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let mut sql = format!(r#"update ad.locs set fac_proc = substring(fac_proc, 1, length(fac_proc) - 1) where fac_proc ~ '{}$'; "#, s); 
    
    if s == "." {
        sql = format!(r#"update ad.locs set fac_proc = substring(fac_proc, 1, length(fac_proc) - 1) where fac_proc ~ '\.$'; "#); 
    } 

    let r = sqlx::raw_sql(&sql).execute(pool)
           .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();

    if fb {
        if r > 1 {
            info!("{} records had trailing '{}'s removed", r, s);
        }
        else {
            info!("{} record had trailing '{}' removed", r, s);
        }
    }

    Ok({})
}

pub async fn replace_list_items_with_target(target: &str, ws: &[&str], pool: &Pool<Postgres>) -> Result<(), AppError> {

    for w in ws {
        replace_in_fac_proc( w, target, "r", true, "", pool).await?; 
    }

    Ok({})
}


pub async fn replace_list_items_with_capitalised(ws: &[&str], pool: &Pool<Postgres>) -> Result<(), AppError> {

    // Here the assumption is that the input words are all upper case, and we need to leave only the first character in this state.
    // Below is from https://stackoverflow.com/questions/38406793/why-is-capitalizing-the-first-letter-of-a-string-so-convoluted-in-rust
    // Exactly how this is working needs more work!
    
    for w in ws {
        let wlower  = w.to_lowercase();     // initially make it all lower case
        let mut c = wlower.chars();      // turn word into0 a vector of characters
        let wcap = match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        };
        replace_in_fac_proc( w, &wcap, "r", true, "", pool).await?; 
    }

    Ok({})
}

pub async fn replace_list_items_with_lower_case(ws: &[&str], pool: &Pool<Postgres>) -> Result<(), AppError> {

    for w in ws {
        let wlower = w.to_lowercase();
        replace_in_fac_proc( w, &wlower, "r", true, "", pool).await?; 
    }

    Ok({})
}

pub async fn add_zzz_prefix_to_list_items(ws: &[&str], pool: &Pool<Postgres>) -> Result<(), AppError> {


    let mut res = "".to_string();
    for w in ws {
        let sql = if w.starts_with("*")
        {
            format!("update ad.locs set fac_proc = 'ZZZ'||fac_proc where fac_proc ~* '^{}'", &w[1..])
        }
        else {
            format!("update ad.locs set fac_proc = 'ZZZ'||fac_proc where fac_proc ~ '^{}'", w)
        };
        let r = sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();
        res = res + &format!("{} ({}), ", w, r);
    }

    let res2 = &res[..res.len() - 2];
    info!("Protective prefix attached to: {}", res2);
    Ok({})
}


pub async fn remove_regexp_from_fac_proc(ss: &str, sql_where: &str, fb: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    let sql = format!(r#"update ad.locs c
	set fac_proc = trim(replace(fac_proc, substring (fac_proc from '{}'), ''))
	where fac_proc ~ '{}'"#, ss, sql_where);
    
    let r = sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?.rows_affected();
    
    // Feedback line provided (unless fb = "")

    let recs = if r==1 {"record"} else {"records"};
    if fb != "" {
        info!("{} {} had {}", r, recs, fb);
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

