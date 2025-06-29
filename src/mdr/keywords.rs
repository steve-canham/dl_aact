use super::utils;

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;


pub async fn build_topics_table (pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"SET client_min_messages TO WARNING; 
    DROP TABLE IF EXISTS ad.study_topics;
    CREATE TABLE ad.study_topics(
      id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
    , sd_sid                 VARCHAR         NOT NULL
    , topic_type_id          INT             NULL
    , original_value         VARCHAR         NULL       
    , original_ct_type_id    INT             NULL
    , original_ct_code       VARCHAR         NULL 
    , mesh_code              VARCHAR         NULL
    , mesh_value             VARCHAR         NULL
    , added_on               TIMESTAMPTZ     NOT NULL default now()
    , coded_on               TIMESTAMPTZ     NULL
    );
    CREATE INDEX study_topics_sid ON ad.study_topics(sd_sid);"#;

    utils::execute_sql(sql, pool).await?;
    info!("study topics table (re)created");
    
    Ok(())

}

pub async fn build_conditions_table (pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"SET client_min_messages TO WARNING; 
    DROP TABLE IF EXISTS ad.study_conditions;
    CREATE TABLE ad.study_conditions(
        id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
    , sd_sid                 VARCHAR         NOT NULL
    , original_value         VARCHAR         NULL
    , original_ct_type_id    INT             NULL
    , original_ct_code       VARCHAR         NULL                 
    , icd_code               VARCHAR         NULL
    , icd_name               VARCHAR         NULL
    , added_on               TIMESTAMPTZ     NOT NULL default now()
    , coded_on               TIMESTAMPTZ     NULL
    );
    CREATE INDEX study_conditions_sid ON ad.study_conditions(sd_sid);"#;

    utils::execute_sql(sql, pool).await?;
    info!("study conditions table (re)created");
    
    Ok(())

}


pub async fn build_features_table (pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"SET client_min_messages TO WARNING; 
    DROP TABLE IF EXISTS ad.study_features;
    CREATE TABLE ad.study_features(
      id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
    , sd_sid                 VARCHAR         NOT NULL
    , feature_type_id        INT             NULL
    , feature_value_id       INT             NULL
    , added_on               TIMESTAMPTZ     NOT NULL default now()

    );
    CREATE INDEX study_features_sid ON ad.study_features(sd_sid);"#;

    utils::execute_sql(sql, pool).await?;
    info!("study features table (re)created");
    
    Ok(())

}

