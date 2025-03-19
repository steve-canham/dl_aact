

		
-- Remove tables not required (mostly from results details section).
-- This also removes the biggest tables

drop table if exists ctgov.baseline_counts cascade;
drop table if exists ctgov.baseline_measurements cascade;
drop table if exists ctgov.design_groups cascade;
drop table if exists ctgov.design_outcomes cascade;
drop table if exists ctgov.design_group_interventions cascade;
drop table if exists ctgov.facility_contacts cascade;
drop table if exists ctgov.facility_investigators cascade;
drop table if exists ctgov.browse_conditions cascade;
drop table if exists ctgov.browse_interventions cascade;
drop table if exists ctgov.detailed_descriptions cascade;

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
drop table if exists ctgov.study_searches cascade;
		
-- Clarify this very big table by dropping unused fields

ALTER TABLE ctgov.studies
DROP COLUMN nlm_download_date_description,
DROP COLUMN verification_month_year,
DROP COLUMN verification_date,
DROP COLUMN disposition_first_submitted_date,
DROP COLUMN disposition_first_submitted_qc_date,
DROP COLUMN disposition_first_posted_date,
DROP COLUMN disposition_first_posted_date_type,
DROP COLUMN primary_completion_month_year,
DROP COLUMN primary_completion_date_type,
DROP COLUMN primary_completion_date,
DROP COLUMN number_of_arms,
DROP COLUMN number_of_groups,
DROP COLUMN is_ppsd,
DROP COLUMN is_us_export,
DROP COLUMN has_dmc,
DROP COLUMN delayed_posting;


ALTER TABLE ctgov.studies
DROP COLUMN study_first_submitted_date,
DROP COLUMN results_first_submitted_date,
DROP COLUMN last_update_submitted_date,
DROP COLUMN study_first_submitted_qc_date,
DROP COLUMN results_first_submitted_qc_date,
DROP COLUMN last_update_submitted_qc_date,
DROP COLUMN target_duration,
DROP COLUMN baseline_population,
DROP COLUMN limitations_and_caveats,
DROP COLUMN is_fda_regulated_drug,
DROP COLUMN is_fda_regulated_device,
DROP COLUMN is_unapproved_device,
DROP COLUMN source,
DROP COLUMN source_class,
DROP COLUMN fdaaa801_violation,
DROP COLUMN baseline_type_units_analyzed;

--select * from ctgov.studies

create schema if not exists ad;
