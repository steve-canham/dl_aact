use sqlx::{postgres::PgQueryResult, Pool, Postgres};
use crate::AppError;
use log::info;


pub async fn build_studies_table (pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql = r#"create schema if not exists ad;
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
	, dt_of_data_fetch	     TIMESTAMPTZ     NULL
	, added_on               TIMESTAMPTZ     NOT NULL default now()
	);
	CREATE INDEX studies_sid ON ad.studies(sd_sid);"#;

	execute_sql(sql, pool).await?;
    info!("studies table (re)created");
    
    Ok(())

}


pub async fn load_studies_data (pool: &Pool<Postgres>) -> Result<(), AppError> {  

    let sql= "select max(nct_id) from ctgov.studies";
	let res: String = sqlx::query_scalar(&sql).fetch_one(pool)
		.await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    let res_as_string = &res[3..].to_string();
    
	let max_id: u64 = res_as_string.parse()
			.map_err(|e| AppError::ParseError(e))?;
    
    let rec_num = execute_phased_transfer(core_study_data_sql(), max_id, pool).await?;
    info!("{} study records created", rec_num);

	let chunk_size = 150000;

    execute_phased_update(registries_update_sql(), rec_num, chunk_size, "identifying registries", pool).await?;
    execute_phased_update(status_sql(), rec_num, chunk_size, "updating statuses", pool).await?;
    execute_phased_update(last_known_status_sql(), rec_num, chunk_size, "using last known statuses", pool).await?;
	info!("");

    execute_phased_update(descriptions_sql(), rec_num, chunk_size, "adding study descriptions", pool).await?;
	info!("");

    execute_phased_update(min_and_max_age_sql(), rec_num, chunk_size, "inserting min and max ages", pool).await?;
    info!("");
    execute_phased_update(age_group_1_sql(), rec_num, chunk_size, "child studies identified", pool).await?;
    execute_phased_update(age_group_2_sql(), rec_num, chunk_size, "adult studies identified", pool).await?;
    execute_phased_update(age_group_4_sql(), rec_num, chunk_size, "elderly studies identified", pool).await?;
	info!("");

    execute_phased_update(gender_flag_sql(), rec_num, chunk_size, "gender eligibility inserted", pool).await?;
	info!("");

	execute_phased_update(max_date_sql(), rec_num, chunk_size, "updating date of data set", pool).await?;
	info!("");

	execute_phased_update(update_start_date_types_sql(), rec_num, chunk_size, "updating start date types", pool).await?;
	execute_phased_update(update_comp_date_types_sql(), rec_num, chunk_size, "updating comp date types", pool).await?;
	info!("");
	execute_phased_update(completed_status_1_sql(), rec_num, chunk_size, "updating comp status using result dates", pool).await?;
	execute_phased_update(completed_status_2_sql(), rec_num, chunk_size, "updating comp status using comp dates", pool).await?;
	execute_phased_update(completed_status_3_sql(), rec_num, chunk_size, "updating comp status using est. comp dates on older studies", pool).await?;
	info!("");

	execute_phased_update(ipd_1_sql(), rec_num, chunk_size, "ipd basics added", pool).await?;
	execute_sql(ipd_2_sql(), pool).await?;
	execute_phased_update(ipd_3_sql(), rec_num, chunk_size, "ipd details added", pool).await?;
	execute_sql(ipd_4_sql(), pool).await?;
	info!("");
	
	execute_sql(vacuum_sql(), pool).await?;
	info!("vacuum carried out on studies table");
    
    Ok(())

}


async fn execute_sql(sql: &str, pool: &Pool<Postgres>) -> Result<PgQueryResult, AppError> {
    
    sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))
}


async fn execute_phased_transfer(sql: &str, max_id: u64, pool: &Pool<Postgres>) -> Result<u64, AppError> {
    
	let mut rec_num: u64 = 0;
	let mut start_num: u64 = 0;
	let chunk_size = 2000000;

	while start_num <= max_id {

		let end_num = start_num + chunk_size;
		let chunk_sql = format!("c.nct_id >= 'NCT{:0>8}' and c.nct_id < 'NCT{:0>8}';", start_num, end_num);
        let chsql = sql.to_string() + " where " + &chunk_sql;
		let res = sqlx::raw_sql(&chsql).execute(pool)
			.await.map_err(|e| AppError::SqlxError(e, chsql.to_string()))?;
		let recs = res.rows_affected();
        rec_num += recs;
		info!("{} study core records transferred, {}", recs, chunk_sql);

		start_num = end_num;
	}

	Ok(rec_num)
}



async fn execute_phased_update(sql: &str, rec_num: u64, chunk_size: u64, fback: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

	for i in (0..rec_num).step_by(chunk_size.try_into().unwrap()) {
        
		let start_num = i + 1000001;
        let mut end_num = start_num + chunk_size - 1;
		if end_num > rec_num + 1000001 {
			end_num = rec_num + 1000000;
		}
		let chunk_sql = format!("s.id >= {} and s.id <= {};", start_num, end_num);
        let chsql = sql.to_string() + " and " + &chunk_sql;
		sqlx::raw_sql(&chsql).execute(pool)
			.await.map_err(|e| AppError::SqlxError(e, chsql.to_string()))?;
        info!("{}, {}", fback, chunk_sql);
    }

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

fn age_group_1_sql <'a>() -> &'a str {
    r#"update ad.studies s
	set age_group_flag = 1
	from ctgov.eligibilities c
	where s.sd_sid = c.nct_id
	and c.child = true "#
}


fn age_group_2_sql <'a>() -> &'a str {
    r#"update ad.studies s
	set age_group_flag = age_group_flag + 2
	from ctgov.eligibilities c
	where s.sd_sid = c.nct_id
	and c.adult = true "#
}


fn age_group_4_sql <'a>() -> &'a str {
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

fn max_date_sql <'a>() -> &'a str {
    r#"update ad.studies s
	set dt_of_data_fetch = dt.max
	from
	(select max(updated_at) as max from ctgov.studies ) as dt
	where id > 1 "#
}

fn update_start_date_types_sql <'a>() -> &'a str {

    r#"update ad.studies s
	set start_date_type = 'e'
	where start_date_type is null
	and start_year is not null "#
}

fn update_comp_date_types_sql <'a>() -> &'a str {

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


fn completed_status_3_sql <'a>() -> &'a str {

	// update studies to 'complete' if a 'estimated' 
    // complete date present before 2020 - 5 years ago at least
    // If not already 'complete' or 'terminated'
    // (leaving 'withdrawn' and 'suspended', if any, as they are)
	// Need to calculate the cut off point rather than have it set

    r#"update ad.studies s
	set status_id = 30
	where s.comp_year < 2020
	and s.status_id < 30
	and s.status_id <> 12 
	and s.status_id <> 25
	and s.comp_date_type = 'e' "#
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


fn vacuum_sql <'a>() -> &'a str {
	
	// Normally leave until final updates...after iec_flag calculated

	// SELECT pg_size_pretty(pg_total_relation_size('ad.studies'));
	r#"VACUUM (FULL, VERBOSE, ANALYZE) ad.studies;"#
	// SELECT pg_size_pretty(pg_total_relation_size('ad.studies'));
}

