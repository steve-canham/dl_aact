use crate::err::AppError;
use sqlx::postgres::PgPool;

pub async fn do_mdr_import(_pool: &PgPool) -> Result<(), AppError> {  

    // Simplify the aact tables after initial restore of posthres.dmp file
    

    // create the ad.studies table and load most of that data


    // create, fill and tidy the secondary identifiers


    // create, fill and tidy the study location data


    // create, fill and tidy the people and organisation data


    // create, fill and tidy the features, conditions and keyword data


    // create, fill and tidy the refs, links and ipd data


    // create and fill data object data 


    // TO DO! 
    
    
    Ok(())

}
