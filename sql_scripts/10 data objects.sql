


DROP TABLE IF EXISTS ad.data_objects;
CREATE TABLE ad.data_objects(
	id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
  , sd_oid                 VARCHAR         NOT NULL
  , sd_sid                 VARCHAR         NULL
  , title                  VARCHAR         NULL
  , version                VARCHAR         NULL
  , display_title          VARCHAR         NULL
  , doi                    VARCHAR         NULL 
  , doi_status_id          INT             NULL
  , publication_year       INT             NULL
  , object_class_id        INT             NULL
  , object_type_id         INT             NULL
  , managing_org_id        INT             NULL
  , managing_org           VARCHAR         NULL
  , managing_org_ror_id    VARCHAR         NULL
  , lang_code              VARCHAR         NULL
  , access_type_id         INT             NULL
  , access_details         VARCHAR         NULL
  , access_details_url     VARCHAR         NULL
  , url_last_checked       DATE            NULL
  , eosc_category          INT             NULL
  , add_study_contribs     BOOLEAN         NULL
  , add_study_topics       BOOLEAN         NULL
  , datetime_of_data_fetch TIMESTAMPTZ     NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
  , coded_on               TIMESTAMPTZ     NULL   
);    
CREATE INDEX data_objects_oid ON ad.data_objects(sd_oid);
CREATE INDEX data_objects_sid ON ad.data_objects(sd_sid);


DROP TABLE IF EXISTS ad.object_datasets;
CREATE TABLE ad.object_datasets(
	id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
  , sd_oid                 VARCHAR         NULL
  , record_keys_type_id    INT             NULL 
  , record_keys_details    VARCHAR         NULL    
  , deident_type_id        INT             NULL  
  , deident_direct 	       BOOLEAN         NULL   
  , deident_hipaa 	       BOOLEAN         NULL   
  , deident_dates 	       BOOLEAN         NULL   
  , deident_nonarr 	       BOOLEAN         NULL   
  , deident_kanon	       BOOLEAN         NULL   
  , deident_details        VARCHAR         NULL    
  , consent_type_id        INT             NULL  
  , consent_noncommercial  BOOLEAN         NULL
  , consent_geog_restrict  BOOLEAN         NULL
  , consent_research_type  BOOLEAN         NULL
  , consent_genetic_only   BOOLEAN         NULL
  , consent_no_methods     BOOLEAN         NULL
  , consent_details        VARCHAR         NULL 
  , added_on               TIMESTAMPTZ     NOT NULL default now()
);

CREATE INDEX object_datasets_oid ON ad.object_datasets(sd_oid)


DROP TABLE IF EXISTS ad.object_dates;
CREATE TABLE ad.object_dates(
	id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
  , sd_oid                 VARCHAR         NULL
  , date_type_id           INT             NULL
  , date_is_range          BOOLEAN         NULL default false
  , date_as_string         VARCHAR         NULL
  , start_year             INT             NULL
  , start_month            INT             NULL
  , start_day              INT             NULL
  , end_year               INT             NULL
  , end_month              INT             NULL
  , end_day                INT             NULL
  , details                VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
);
CREATE INDEX object_dates_oid ON ad.object_dates(sd_oid);


DROP TABLE IF EXISTS ad.object_instances;
CREATE TABLE ad.object_instances(
	id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
  , sd_oid                 VARCHAR         NULL
  , system_id              INT             NULL
  , system                 VARCHAR         NULL
  , url                    VARCHAR         NULL
  , url_accessible         BOOLEAN         NULL
  , url_last_checked       DATE            NULL
  , resource_type_id       INT             NULL
  , resource_size          VARCHAR         NULL
  , resource_size_units    VARCHAR         NULL
  , resource_comments      VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
  , coded_on               TIMESTAMPTZ     NULL   
);
CREATE INDEX object_instances_oid ON ad.object_instances(sd_oid);


DROP TABLE IF EXISTS ad.object_titles;
CREATE TABLE ad.object_titles(
	id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
  , sd_oid                 VARCHAR        NULL
  , title_type_id          INT             NULL
  , title_text             VARCHAR         NULL
  , lang_code              VARCHAR         NOT NULL
  , lang_usage_id          INT             NOT NULL default 11
  , is_default             BOOLEAN         NULL
  , comments               VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
);
CREATE INDEX object_titles_oid ON ad.object_titles(sd_oid);

