

DROP TABLE IF EXISTS ad.study_people;
CREATE TABLE ad.study_people(
	id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , contrib_type_id        INT             NULL
  , person_given_name      VARCHAR         NULL
  , person_family_name     VARCHAR         NULL
  , person_full_name       VARCHAR         NULL
  , orcid_id               VARCHAR         NULL
  , person_affiliation     VARCHAR         NULL
  , organisation_id        INT             NULL
  , organisation_name      VARCHAR         NULL
  , organisation_ror_id    VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
  , coded_on               TIMESTAMPTZ     NULL
);
CREATE INDEX study_people_sid ON ad.study_people(sd_sid);


DROP TABLE IF EXISTS ad.study_organisations;
CREATE TABLE ad.study_organisations(
	id                     INT             PRIMARY KEY GENERATED ALWAYS AS IDENTITY (start with 10000001 increment by 1)
  , sd_sid                 VARCHAR         NOT NULL
  , contrib_type_id        INT             NULL
  , organisation_id        INT             NULL
  , organisation_name      VARCHAR         NULL
  , organisation_ror_id    VARCHAR         NULL
  , added_on               TIMESTAMPTZ     NOT NULL default now()
  , coded_on               TIMESTAMPTZ     NULL
);
CREATE INDEX study_organisations_sid ON ad.study_organisations(sd_sid);


