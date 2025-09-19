mod studies;
mod idents;
mod locs;
mod peoporgs;
mod keywords;
mod links;
mod dataobjs;
mod utils;

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;

pub async fn do_mdr_import(data_date: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // Different portions of the import process can be turned on and off.
    // In normal use all data would be imported, but during development it is 
    // very useful to 'turn off' functionality that would simply repeat processing 
    // already successfully completed and import of data already present in the DB.

    // The parts of the mdr import process to be implemented are stored iu 
    // the 'mdr_params' string. If that string is empty - the normal default - all 
    // parts of the import process are implemented. 
    // If it contains any value then the letters are used to set specific boolean variables 
    // that are used to control the execution of specific parts...
  
    let mut simplify_tables = true;
    let mut import_studies = true;

    let mut import_titles = true;
    let mut import_idents = true;

    let mut import_locations = true;
    let mut import_countries = true;

    let mut import_orgs = true;
    let mut import_people = true;

    let mut import_features = true;
    let mut import_topics = true;
    let mut import_conditions = true;

    let mut import_rels = true;
    let mut import_refs = true;
    let mut import_links = true;
    let mut import_ipd_avail = true;    

    let mut import_data_objects = true;
    let mut import_datasets = true;
    let mut import_obj_instances = true;
    let mut import_obj_titles = true;
    let mut import_obj_dates = true;


    // To be added to program parameters...
    // Current value...
    //******************************************
    let mdr_params = "l";
    //******************************************

    if mdr_params != "" {
        simplify_tables = (mdr_params).contains("m");
        import_studies = (mdr_params).contains("d");
        import_titles = (mdr_params).contains("t");
        import_idents = (mdr_params).contains("n");

        import_locations = (mdr_params).contains("l");
        import_countries = (mdr_params).contains("c");

        import_orgs = (mdr_params).contains("g");
        import_people = (mdr_params).contains("p");

        import_rels = (mdr_params).contains("q");
        import_refs = (mdr_params).contains("r");
        import_links = (mdr_params).contains("s");
        import_ipd_avail = (mdr_params).contains("t");

        import_features = (mdr_params).contains("x");
        import_topics = (mdr_params).contains("y");
        import_conditions = (mdr_params).contains("z");

        import_data_objects = (mdr_params).contains("a");
        import_datasets = (mdr_params).contains("e");
        import_obj_instances = (mdr_params).contains("i");
        import_obj_titles = (mdr_params).contains("o");
        import_obj_dates = (mdr_params).contains("u");
    }
        
    let max_id = get_max_nct_id(pool).await?;

    if simplify_tables {

        // Simplify the aact tables after initial restore of postgres.dmp file
        // Remove tables not required (mostly from results details section).
        // Then clarify the very big studies table by dropping unused fields
    
        utils::execute_sql(drop_tables_a_sql(), pool).await?;
        utils::execute_sql(drop_tables_b_sql(), pool).await?;

        utils::execute_sql(drop_columns_a_sql(), pool).await?;
        utils::execute_sql(drop_columns_b_sql(), pool).await?;
    }

    if import_studies {
        studies::build_studies_table(pool).await?;
        studies::load_studies_data(data_date, max_id, pool).await?;
    }

    if import_titles {
        idents::build_titles_table(pool).await?;
        idents::load_titles_data (max_id, pool).await?;
    }

    if import_idents {
        idents::build_idents_table(pool).await?;
        let idents_processing = "full";
        idents::load_idents_data (idents_processing, max_id, pool).await?;
    }

    if import_locations {
        locs::build_locations_table(pool).await?;
        let locs_processing = "full";
        locs::load_facs_data (locs_processing, max_id, pool).await?;
    }

    if import_countries {
        locs::build_countries_table(pool).await?;
    }

    if import_orgs {
        peoporgs::build_orgs_table(pool).await?;
    }

    if import_people {
        peoporgs::build_people_table(pool).await?;
    }

    if import_features {
        keywords::build_features_table(pool).await?;
    }

    if import_topics {
        keywords::build_topics_table(pool).await?;
    }
        
    if import_conditions {
        keywords::build_conditions_table(pool).await?;
    }

    if import_rels {
        links::build_rels_table(pool).await?;
    }

    if import_refs {
        links::build_refs_table(pool).await?;
    }
    
    if import_links {
        links::build_links_table(pool).await?;
    }
        
    if import_ipd_avail {
        links::build_ipd_available_table(pool).await?;
    }

    if import_data_objects {
        dataobjs::build_data_objects_table(pool).await?;
    }

    if import_datasets {
        dataobjs::build_datasets_table(pool).await?;
    }

    if import_obj_instances {
        dataobjs::build_obj_instances_table(pool).await?;
    }

    if import_obj_titles {
        dataobjs::build_obj_titles_table(pool).await?;
    }

    if import_obj_dates {
        dataobjs::build_obj_dates_table(pool).await?;
    }

    utils::execute_sql(set_messages_to_notice(), pool).await?;
    info!("");
   
    Ok(())

}


fn drop_tables_a_sql <'a>() -> &'a str {
    r#"SET client_min_messages TO WARNING; 
    drop table if exists ctgov.baseline_counts cascade;
    drop table if exists ctgov.baseline_measurements cascade;
    drop table if exists ctgov.design_groups cascade;
    drop table if exists ctgov.design_outcomes cascade;
    drop table if exists ctgov.design_group_interventions cascade;
    drop table if exists ctgov.facility_contacts cascade;
    drop table if exists ctgov.facility_investigators cascade;
    drop table if exists ctgov.browse_conditions cascade;
    drop table if exists ctgov.browse_interventions cascade;
    drop table if exists ctgov.detailed_descriptions cascade;"#
}


fn drop_tables_b_sql <'a>() -> &'a str {
    r#"SET client_min_messages TO WARNING; 
    drop table if exists ctgov.drop_withdrawals cascade;
    drop table if exists ctgov.milestones cascade;
    drop table if exists ctgov.outcome_analyses cascade;
    drop table if exists ctgov.outcome_analysis_groups cascade;
    drop table if exists ctgov.outcome_counts cascade;
    drop table if exists ctgov.outcome_measurements cascade;
    drop table if exists ctgov.participant_flows cascade;
    drop table if exists ctgov.pending_results cascade;
    drop table if exists ctgov.reported_events cascade;
    drop table if exists ctgov.reported_event_totals cascade;
    drop table if exists ctgov.result_agreements cascade;
    drop table if exists ctgov.result_groups cascade;
    drop table if exists ctgov.search_results cascade;
    drop table if exists ctgov.search_terms cascade;
    drop table if exists ctgov.search_term_results cascade;
    drop table if exists ctgov.study_searches cascade;"#
}


fn drop_columns_a_sql <'a>() -> &'a str {
    r#"SET client_min_messages TO WARNING; 
    ALTER TABLE ctgov.studies
    DROP COLUMN if exists nlm_download_date_description,
    DROP COLUMN if exists verification_month_year,
    DROP COLUMN if exists verification_date,
    DROP COLUMN if exists disposition_first_submitted_date,
    DROP COLUMN if exists disposition_first_submitted_qc_date,
    DROP COLUMN if exists disposition_first_posted_date,
    DROP COLUMN if exists disposition_first_posted_date_type,
    DROP COLUMN if exists primary_completion_month_year,
    DROP COLUMN if exists rimary_completion_date_type,
    DROP COLUMN if exists primary_completion_date,
    DROP COLUMN if exists number_of_arms,
    DROP COLUMN if exists number_of_groups,
    DROP COLUMN if exists is_ppsd,
    DROP COLUMN if exists is_us_export,
    DROP COLUMN if exists has_dmc,
    DROP COLUMN if exists delayed_posting;"#
}


fn drop_columns_b_sql <'a>() -> &'a str {
    r#"SET client_min_messages TO WARNING; 
    ALTER TABLE ctgov.studies
    DROP COLUMN if exists study_first_submitted_date,
    DROP COLUMN if exists results_first_submitted_date,
    DROP COLUMN if exists last_update_submitted_date,
    DROP COLUMN if exists study_first_submitted_qc_date,
    DROP COLUMN if exists results_first_submitted_qc_date,
    DROP COLUMN if exists last_update_submitted_qc_date,
    DROP COLUMN if exists target_duration,
    DROP COLUMN if exists baseline_population,
    DROP COLUMN if exists limitations_and_caveats,
    DROP COLUMN if exists is_fda_regulated_drug,
    DROP COLUMN if exists s_fda_regulated_device,
    DROP COLUMN if exists is_unapproved_device,
    DROP COLUMN if exists source,
    DROP COLUMN if exists source_class,
    DROP COLUMN if exists fdaaa801_violation,
    DROP COLUMN if exists baseline_type_units_analyzed;"#
}


fn set_messages_to_notice <'a>() -> &'a str {
    "SET client_min_messages TO NOTICE;"
}

async fn get_max_nct_id(pool: &Pool<Postgres>) -> Result<u64, AppError> {  

    let sql= "select max(nct_id) from ctgov.studies";
	let res: String = sqlx::query_scalar(sql).fetch_one(pool)
		.await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    let res_as_string = &res[3..].to_string();
    let max_id: u64 = res_as_string.parse()
			.map_err(|e| AppError::ParseError(e))?;
    Ok(max_id)
}