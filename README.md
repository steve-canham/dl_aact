
EARLY STAGES OF DEVELOPMENT

<h2>Introduction</h2>
This program is designed to transform a full set of clinicaltrials.gov (CTG) data into an MDR 'acumulated data' (ad) database, so that the CTG can be incorporated into an MDR processing pipeline. It therefore provides a mechanism to 'short-cut' the traditional accumulation of CTG data via the API, creating a full dataset in a single process rather than (as the MDR normally would) downloading all half a million plus CTG records, transforming them into .json files, ingesting those files to a staging databse, and then transferring themn to the accumulated data scherma. In situations where the entire CTG dataset needs (re-)building this has the potential to reduce the time required from several days to about 30 minutes.<br/><br/>

The source is a downloaded postgres.dmp file, available from https://aact.ctti-clinicaltrials.org/snapshots. This is part of the US based Clinical Trials Innovation Iniative, or CTTI. They make a copy of the entire CTG dataset, updated and free to download each day. As NLM no longer seem to make the whole dataset available (it used to be available as a zipped collection of XML, then JSON files, but the link to this appears to have been removed) the CTII download is the only way to obtain the whole CTG dataset.<br/><br/>

From the CTII web site: "AACT is a publicly available relational database that contains all information (protocol and result data elements) about every study registered in ClinicalTrials.gov. Content is downloaded from ClinicalTrials.gov daily and loaded into AACT." Much more detail on the system can be found at https://github.com/ctti-clinicaltrials/aact.<br/><br/>

<h2>Operation</h2>

<ul>
<li>The initial stage is to download the zip file from the AACT snapshot web page (the url above). Unzip it and place the postgres.dmp into a suitable folder. The zip file also contains some documentation files but these, like the documentation web pages on the AACT site, seem out of date and do not fully cover the current edition of the database. </li>
<li>A database called aact should be created on a Postgres cluster, if not already present.</li>
<li>The AACT database then needs to be restored using a pg_restore command, with the entire database appearing as the ctgov schema within the aact DB. Details on the restore command are available in one of the docs files in this repo.</li>
<li>**Once the restore is completed** simply run the program. It largely consists of a long series of SQL statements that are fired at the AACT database, to extract the data in to an MDR compliant form. 
<li>The result is an  MDR 'ad' version of the CTG data, in a schema called 'ad' in the aact database. From there the data can be transferred to another database (e.g. ctg) using FTW mechanisms.</li> 
<li>By default this data is not fully coded, e.g. ROR organisation codes have not yet been applied. The coding phase of the CTG pipeline therefore still needs to be applied to the data. Some coding may be available, and switched on using a CLI flag (this needs to be developed).</li>  
<li>At the end of the process a summary version of the study data is transferred to the who db, so that it can summarised along with data from other registries (needs to be developed and the dl_who process amended accordingly).</li>  
</ul>

<h2>Current Status</h2>

Development, in June 2025, is at a very early stage. <br/>
It currently consists of writing and testing the large series of SQL commands that carry out the data transformations and cleaning - at present manually through a Postgres GUI (DBeaver). The SQL scripts are stored in the repo as a form of source control, along with documents describing different aspects of the system. <br/> <br/>
Once the scripts are complete they will be embedded into a series of Rust statements which will apply them to the target database, allowing the process to proceed automatically after the inital database restore. The Rust system will necessarily be single-threaded, allowing sequential application of the SQL (database calls using sqlx are async by default). 

<h2>Issues and Aspects (so far)</h2>

<ul>
<li>The AACT DB seems to include <i>all</i> of the CTG data, (indeed it includes some additional 'calculated values'). In the past this has not been the case, which is why the AACT DB was not used as a data source in the initial versions of the MDR. In fact, much of the data is not required for the MDR (e.g. the large amounts of results details). The initial stage of processing the data is therefore to drop many of the larger tables, as well as many of the fields in the very large studies table. This makes the resulting databse clearer and easier to work with.</li>
<li>Creating the ad table often involves inserting data into a table and then repeatedly updating it. An update in Postgres is really an insert followed by a delete, but because the delete does not automatically release the disk space back to the OS the apparent table size can increase dramatically, to several GB in many cases. Because of this the final stage of table creation, in many cases, is a 'full vacuum', in effect forcing Postgres to rewrite the table from scratch and release all the space that is no longer used. This can shrink tables dramatically, in some cases to an eighth of their size before the vacuum.</li>
<li>The major difficulty is not so much the need to re-arrange the data as to clean it, for example to reduce the large amount of spurious symbols and text that are to be found in some fields, and to make terms consistent enough for easier coding. A large proportion of the SQL statements are therefore intended to support data cleaning. Particular effort is made to accurately collect secondary Ids, as these are critical in discovering links between studies registered in multiple registries.</li>  
<li>Carrying out this exercise has suggested some changes to the MDR's data schema - in some cases simplification, in others slight extension or a more logical use of category codes. These have been documented in the docs folder. </li>  
</ul>
