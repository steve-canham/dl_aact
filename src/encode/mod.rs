use crate::err::AppError;
use sqlx::postgres::PgPool;

pub async fn do_data_encoding(_pool: &PgPool) -> Result<(), AppError> {  

    // TO DO! 
    Ok(())

}