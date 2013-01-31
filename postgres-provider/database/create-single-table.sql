---------------------------------------------------------------------------------------------------------------------------
-- INSTALL INSTRUCTIONS: BEFORE PROCEEDING, PERFORM THE FOLLOWING STEPS
--
-- 1- Create a PostGIS spatially enabled database to be used as the Metadata Catalog if it does 
-- not yet exist (ddf is the default name, but doesn't have to be used).  This can be done by 
-- downloading PostGIS 2.0 http://postgis.refractions.net/ and following the instructions provided.
--
-- 2. If using a version of PostgreSQL that is earlier than 9.1, you will have to run the following scripts
-- while connected to the spatial database that you will be using as the Metadata Catalog.  If you are using
-- PostgreSQL 9.1 or later proceed to step 3.
--   - Execute the script $POSTGRESQL_HOME/share/contrib/btree_gist.sql
--   - Execute the script $POSTGRESQL_HOME/share/contrib/pg_trgm.sql
--    $POSTGRESQL_HOME should be replaced with the path of your Postgresql installation directory
--
-- 3. Replace the TABLESPACE_DIRECTORY in this file around line 29 with the absolute path to the 
--    directory where the tablespace files will be stored
--
-- 4. Log into psql and run the following this script and ensure no errors are returned
----------------------------------------------------------------------------------------------------
-- Open up a psql prompt and execute the following:

-- Drop ddf schema if it exists
drop schema if exists ddf cascade;
   
-- Log onto the database using psql command and create a 'ddf' schema with the following command
create schema ddf;

create extension if not exists postgis with schema ddf;
create extension if not exists btree_gist with schema ddf;
create extension if not exists pg_trgm with schema ddf;

CREATE TABLESPACE default_space LOCATION 'TABLESPACE_DIRECTORY';
SET default_tablespace = default_space;
-- Create the base 'ddf_catalog_tab' table, which contains all the columns, 
create table ddf.ddf_catalog_tab(
    catalog_id varchar (32),
    catalog_title text not null,
	catalog_datatype 	varchar (128) not null, 
	catalog_version 		varchar (128) not null,
	catalog_created_timestamp timestamp with time zone default current_timestamp not null,
    catalog_modified_timestamp timestamp with time zone default current_timestamp not null,
	catalog_effective_timestamp timestamp with time zone not null,
	catalog_expiration_timestamp timestamp with time zone null,
	catalog_metadata     text not null,
	catalog_resource_uri varchar (1024) not null,
	catalog_resource_size varchar (128) null,
	catalog_thumbnail bytea null,
	text_search_index_column   tsvector,
	catalog_wkt_text text,
	catalog_source_location geometry
);

-- Add a primary key constraint ddf_catalog_pk to the ddf.mdf_catalog_table, 
alter table ddf.ddf_catalog_tab add constraint ddf_catalog_pk primary key(catalog_id);

create index idx_datatype on ddf.ddf_catalog_tab (catalog_datatype);
create index idx_location on ddf.ddf_catalog_tab using gist (catalog_source_location);

-- Create a gin and gist indexes on specific columns (text_search_index_column, timestamp columns, etc) to increase performance
CREATE INDEX idx_textsearch ON ddf.ddf_catalog_tab USING gin(text_search_index_column);

create index idx_modified_timestamp on ddf.ddf_catalog_tab using gist (catalog_modified_timestamp);
create index idx_effective_timestamp on ddf.ddf_catalog_tab using gist (catalog_effective_timestamp);


-- Create a unique constraint on the catalog_resource_uri column
create unique index idx_resource on ddf.ddf_catalog_tab (catalog_resource_uri);

-- Create a trigger to keep the text_search_index_column up-to-date when the catalog_title and catalog_metadata get updated
create trigger text_search_main_update_trigger before insert or update on ddf.ddf_catalog_tab
for each row execute procedure tsvector_update_trigger(text_search_index_column, 'pg_catalog.english', catalog_title, catalog_metadata ); 
