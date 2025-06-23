

DROP TABLE IF EXISTS ad.study_relationships;
CREATE TABLE ad.study_relationships(
	id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , relationship_type_id   INT             NULL
  , target_sd_sid          VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
);
CREATE INDEX study_relationships_sid ON ad.study_relationships(sd_sid);
CREATE INDEX study_relationships_target_sid ON ad.study_relationships(target_sd_sid);


DROP TABLE IF EXISTS ad.study_references;
CREATE TABLE ad.study_references(
	id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , pmid                   VARCHAR         NULL
  , citation               VARCHAR         NULL
  , doi                    VARCHAR         NULL	
  , type_id                INT             NULL
  , comments               VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
);
CREATE INDEX study_references_sid ON ad.study_references(sd_sid);




DROP TABLE IF EXISTS ad.study_links;
CREATE TABLE ad.study_links(
	id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , link_label             VARCHAR         NULL
  , link_url               VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
);
CREATE INDEX study_links_sid ON ad.study_links(sd_sid);


DROP TABLE IF EXISTS ad.study_ipd_available;
CREATE TABLE ad.study_ipd_available(
	id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , ipd_id                 VARCHAR         NULL
  , ipd_type               VARCHAR         NULL
  , ipd_url                VARCHAR         NULL
  , ipd_comment            VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
);
CREATE INDEX study_ipd_available_sid ON ad.study_ipd_available(sd_sid);


