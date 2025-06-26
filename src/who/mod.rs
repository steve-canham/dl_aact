use crate::err::AppError;
use sqlx::postgres::PgPool;

pub async fn do_who_transfer(_pool: &PgPool) -> Result<(), AppError> {  

    // TO DO! 
    Ok(())

}