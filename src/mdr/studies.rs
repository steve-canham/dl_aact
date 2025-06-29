/*
use super::utils;

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;
use chrono::Datelike;

 
pub async fn build_studies_table (pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"SET client_min_messages TO WARNING; 
    create schema if not exists ad;
	DROP TABLE IF EXISTS ad.studies;
	CREATE TABLE ad.studies(
	  id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY  (start with 1000001 increment by 1)
	, sd_sid                 VARCHAR         NOT NULL
	, display_title          VARCHAR         NULL
	, title_lang_code        VARCHAR         NOT NULL default 'en'
	, brief_description      VARCHAR         NULL
	, reg_year           	 INT             NULL
	, reg_month        	     INT             NULL
    , reg_date_type          CHAR(1)         NULL
	, start_year      	     INT             NULL
	, start_month      	     INT             NULL
    , start_date_type        CHAR(1)         NULL
	, comp_year      		 INT             NULL
	, comp_month      	     INT             NULL
	, comp_date_type         CHAR(1)         NULL
	, res_year      		 INT             NULL
	, res_month      		 INT             NULL
    , res_date_type          CHAR(1)         NULL
	, type_id                INT             NOT NULL default 0
	, status_id        	     INT             NOT NULL default 0
	, enrolment              VARCHAR         NULL
	, enrolment_type         CHAR(1)         NULL
	, gender_flag            CHAR(1)         NULL
	, min_age                INT             NULL
	, min_age_units_id       INT             NULL
	, max_age                INT             NULL
	, max_age_units_id       INT             NULL
	, age_group_flag         INT             NOT NULL default 0
	, iec_flag               INT             NOT NULL default 0 
	, ipd_sharing			 VARCHAR         NULL
	, dt_of_data    	     TIMESTAMPTZ     NULL
	, added_on               TIMESTAMPTZ     NOT NULL default now()
	);
	CREATE INDEX studies_sid ON ad.studies(sd_sid);"#;

	utils::execute_sql(sql, pool).await?;
    info!("studies table (re)created");
    
    Ok(())

}


pub async fn load_studies_data (data_date: &str, max_id: u64, pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let chunk_size = 2000000;
    let rec_num = utils::execute_phased_transfer(core_study_data_sql(), max_id, chunk_size, " where ", "core study records", pool).await?;

	let chunk_size = 150000;
    utils::execute_phased_update(registries_update_sql(), rec_num, chunk_size, "registries identified", pool).await?;
    utils::execute_phased_update(status_sql(), rec_num, chunk_size, "statuses inserted", pool).await?;
    utils::execute_phased_update(last_known_status_sql(), rec_num, chunk_size, "statuses added using last known statuses", pool).await?;
    utils::execute_phased_update(descriptions_sql(), rec_num, chunk_size, "study descriptions added", pool).await?;

    utils::execute_phased_update(min_and_max_age_sql(), rec_num, chunk_size, "min / max ages inserted", pool).await?;
    utils::execute_phased_update(age_group_child_sql(), rec_num, chunk_size, "child studies identified", pool).await?;
    utils::execute_phased_update(age_group_adult_sql(), rec_num, chunk_size, "adult studies identified", pool).await?;
    utils::execute_phased_update(age_group_elderly_sql(), rec_num, chunk_size, "elderly studies identified", pool).await?;
    utils::execute_phased_update(gender_flag_sql(), rec_num, chunk_size, "gender eligibilities inserted", pool).await?;

	utils::execute_phased_update(update_start_date_types_sql(), rec_num, chunk_size, "start date types updated", pool).await?;
	utils::execute_phased_update(update_comp_date_types_sql(), rec_num, chunk_size, "comp date types updated", pool).await?;
	utils::execute_phased_update(completed_status_1_sql(), rec_num, chunk_size, "comp statuses updated using result dates", pool).await?;
	utils::execute_phased_update(completed_status_2_sql(), rec_num, chunk_size, "comp statuses updated using comp dates", pool).await?;
	let sql_string = completed_status_3_sql();  // Function returns a string rather than &str
	utils::execute_phased_update(&sql_string, rec_num, chunk_size, "comp statuses updated using est. comp dates on older studies", pool).await?;

	utils::execute_phased_update(ipd_1_sql(), rec_num, chunk_size, "ipd basics added", pool).await?;
	utils::execute_sql(ipd_2_sql(), pool).await?;
	utils::execute_phased_update(ipd_3_sql(), rec_num, chunk_size, "ipd details added", pool).await?;
	utils::execute_sql(ipd_4_sql(), pool).await?;

	let sql_string = max_date_sql(data_date);  // Function returns a string rather than &str
	utils::execute_phased_update(&sql_string, rec_num, chunk_size, "dates of data set inserted", pool).await?;

	utils::vacuum_table("studies", pool).await?;
    
    Ok(())

}


fn core_study_data_sql <'a>() -> &'a str {
    r#"insert into ad.studies (sd_sid, display_title, 
	reg_year, reg_month, reg_date_type,
	start_year, start_month, start_date_type, 
	comp_year, comp_month, comp_date_type, 
	res_year, res_month, res_date_type, 
	type_id, enrolment, enrolment_type)
	select nct_id, brief_title, 
	substring(study_first_posted_date::varchar, 1, 4)::int, 
	substring(study_first_posted_date::varchar, 6, 2)::int, lower(left(study_first_posted_date_type, 1)),
	substring(start_month_year, 1, 4)::int, 
	substring(start_month_year, 6, 2)::int, lower(left(start_date_type, 1)),
	substring(completion_month_year, 1, 4)::int, 
	substring(completion_month_year, 6, 2)::int, lower(left(completion_date_type, 1)),
	substring(results_first_posted_date::varchar, 1, 4)::int, 
	substring(results_first_posted_date::varchar, 6, 2)::int, lower(left(results_first_posted_date_type, 1)),
	case 
		when study_type = 'INTERVENTIONAL' then 11
		when study_type = 'OBSERVATIONAL' then 12
		when study_type = 'EXPANDED_ACCESS' then 14
		else 0
	end,
	enrollment, 
	lower(left(enrollment_type, 1))
	from ctgov.studies c "#
}


fn registries_update_sql <'a>() -> &'a str {
    r#"Update ad.studies s
	set type_id = 13    -- patient registry
	from ctgov.studies c
	where s.sd_sid = c.nct_id
	and c.patient_registry = true "#
}


fn status_sql <'a>() -> &'a str {
    r#"Update ad.studies s
	set status_id = case
		when c.overall_status = 'COMPLETED' then 30
		when c.overall_status = 'NOT_YET_RECRUITING' then 10
		when c.overall_status = 'WITHDRAWN' then 12
		when c.overall_status = 'RECRUITING' then 14
		when c.overall_status = 'APPROVED_FOR_MARKETING' then 30
		when c.overall_status = 'ENROLLING_BY_INVITATION' then 16
		when c.overall_status = 'ACTIVE_NOT_RECRUITING' then 18
		when c.overall_status = 'AVAILABLE' then 20
		when c.overall_status = 'SUSPENDED' then 25
		when c.overall_status = 'TERMINATED' then 32
		else 0
	end
	from ctgov.studies c
	where s.sd_sid = c.nct_id
	and c.overall_status <> 'UNKNOWN' "#
}


fn last_known_status_sql <'a>() -> &'a str {
    r#"Update ad.studies s
	set status_id = case
		when c.last_known_status = 'NOT_YET_RECRUITING' then 10
		when c.last_known_status = 'RECRUITING' then 14
		when c.last_known_status = 'ENROLLING_BY_INVITATION' then 16
		when c.last_known_status = 'ACTIVE_NOT_RECRUITING' then 18
	end
	from ctgov.studies c
	where s.sd_sid = c.nct_id
	and c.overall_status = 'UNKNOWN'
	and c.last_known_status is not null "#
}


fn descriptions_sql <'a>() -> &'a str {
    r#"update ad.studies s
	set brief_description = c.description
	from ctgov.brief_summaries c
	where s.sd_sid = c.nct_id "#
}


fn min_and_max_age_sql <'a>() -> &'a str {
    r#"update ad.studies s
	set min_age = c.minimum_age_num,
	min_age_units_id = case
		when minimum_age_unit = 'year' then 17
		when minimum_age_unit = 'month' then 16
		when minimum_age_unit = 'week' then 15
		when minimum_age_unit = 'day' then 14
		when minimum_age_unit = 'hour' then 13
		when minimum_age_unit = 'minute' then 12
	end,
	max_age = c.maximum_age_num,
	max_age_units_id = case
		when maximum_age_unit = 'year' then 17
		when maximum_age_unit = 'month' then 16
		when maximum_age_unit = 'week' then 15
		when maximum_age_unit = 'day' then 14
		when maximum_age_unit = 'hour' then 13
		when maximum_age_unit = 'minute' then 12
	end
	from ctgov.calculated_values c
	where s.sd_sid = c.nct_id "#
}

fn age_group_child_sql <'a>() -> &'a str {
    r#"update ad.studies s
	set age_group_flag = 1
	from ctgov.eligibilities c
	where s.sd_sid = c.nct_id
	and c.child = true "#
}


fn age_group_adult_sql <'a>() -> &'a str {
    r#"update ad.studies s
	set age_group_flag = age_group_flag + 2
	from ctgov.eligibilities c
	where s.sd_sid = c.nct_id
	and c.adult = true "#
}


fn age_group_elderly_sql <'a>() -> &'a str {
    r#"update ad.studies s
	set age_group_flag = age_group_flag + 4
	from ctgov.eligibilities c
	where s.sd_sid = c.nct_id
	and c.older_adult = true "#
}


fn gender_flag_sql <'a>() -> &'a str {
    r#"update ad.studies s
	set gender_flag = lower(left(c.gender, 1))
	from ctgov.eligibilities c
	where s.sd_sid = c.nct_id "#
}


fn update_start_date_types_sql <'a>() -> &'a str {

	// Partial dates are often not given a status.
	// Here forced to 'e' for estimated.

    r#"update ad.studies s
	set start_date_type = 'e'
	where start_date_type is null
	and start_year is not null "#
}

fn update_comp_date_types_sql <'a>() -> &'a str {

	// Partial dates are often not given a status.
	// Here forced to 'e' for estimated.

    r#"update ad.studies s
	set comp_date_type = 'e'
	where comp_date_type is null
	and comp_year is not null "#
}


fn completed_status_1_sql <'a>() -> &'a str {

	// update study status to 'complete' if a results date present
    // if not already 'complete'or 'terminated'
    // (leaving 'withdrawn', if any, as they are)

    r#"update ad.studies s
	set status_id = 30
	where res_year is not null
	and status_id < 30 
	and status_id <> 12 "#
}


fn completed_status_2_sql <'a>() -> &'a str {

	// update study status to 'complete' if a 'actual' 
    // complete date present. If not already 'complete' or 'terminated'
    // (leaving 'withdrawn', if any, as they are)

    r#"update ad.studies s
	set status_id = 30
	where s.status_id < 30
	and s.status_id <> 12 
	and s.comp_date_type = 'a' "#
}


fn completed_status_3_sql () -> String {

	// update studies to 'complete' if a 'estimated' 
    // complete date present 3 years ago at least.
    // If not already 'complete' or 'terminated'
    // (leaving 'withdrawn' and 'suspended', if any, as they are)

	let current_date = chrono::Utc::now();
    let current_year = current_date.year();
    let current_month = current_date.month();

	let target_year = current_year - 3;

    format!(r#"update ad.studies s
	set status_id = 30
	where (s.comp_year < {}
	or (s.comp_year = {} and s.comp_month < {}))
	and s.status_id < 30
	and s.status_id <> 12 
	and s.status_id <> 25
	and s.comp_date_type = 'e' "#, target_year, target_year, current_month)
	
}


fn ipd_1_sql <'a>() -> &'a str {

	r#"update ad.studies s
	set ipd_sharing = ds.sharing
	from
		(select nct_id, plan_to_share_ipd||
		case 
			when plan_to_share_ipd_description is not null then E'\n'||plan_to_share_ipd_description
			else ''
		end as sharing
		from ctgov.studies c
		where plan_to_share_ipd = 'NO' or plan_to_share_ipd = 'UNDECIDED') ds
	where s.sd_sid = ds.nct_id "#
}


fn ipd_2_sql <'a>() -> &'a str {

	r#"SET client_min_messages TO WARNING; 
	drop table if exists ctgov.assoc_ipd_docs;
	create table ctgov.assoc_ipd_docs 
	(
		nct_id varchar primary key,
		docs  varchar 
	);

	insert into ctgov.assoc_ipd_docs 
	select nct_id, 
	string_agg (
			replace(replace(name, 'STUDY_PROTOCOL' ,'Study protocol'), 'ANALYTIC_CODE' , 'Analytic code'), ', '
			)
	from ctgov.ipd_information_types
	group by nct_id ;
	SET client_min_messages TO NOTICE;"#
}


fn ipd_3_sql <'a>() -> &'a str {

	r#"update ad.studies s
	set ipd_sharing = ds.sharing
	from
		(select c.nct_id, plan_to_share_ipd||
		case 
			when plan_to_share_ipd_description is not null then E'\n'||plan_to_share_ipd_description
			else ''
		end
		|| case 
			when ipd_time_frame is not null then E'\nTime frame: '||ipd_time_frame
			else ''
		end
		|| case 
			when ipd_access_criteria is not null then E'\nAccess criteria: '||ipd_access_criteria
			else ''
		end
		||case 
			when ipd_url is not null then E'\nURL: '||ipd_url
			else ''
		end
		||case 
			when docs is not null then E'\nSupporting documents: '|| docs
			else ''
		end
		|| E'\n(as of '||c.last_update_posted_date||')' as sharing
		from ctgov.studies c
		left join ctgov.assoc_ipd_docs t
		on c.nct_id = t.nct_id
		where c.plan_to_share_ipd = 'YES') ds
	where s.sd_sid = ds.nct_id "#
}


fn ipd_4_sql <'a>() -> &'a str {
	r#"drop table if exists ctgov.assoc_ipd_docs;"#
}


fn max_date_sql (data_date: &str) -> String {

	// data_date pre-checked as being a valid date in ISO format.
	
    format!(r#"update ad.studies s
	set dt_of_data = '{}'
	where id > 1 "#, data_date)
}

*/
