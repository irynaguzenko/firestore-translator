CREATE DATABASE world;
USE DATABASE world;

CREATE FOREIGN DATA WRAPPER firestore;
CREATE SERVER firestoreConnectionFactory FOREIGN DATA WRAPPER firestore;

CREATE SCHEMA basicSchema SERVER firestoreConnectionFactory;
SET SCHEMA basicSchema;

CREATE FOREIGN TABLE CountriesT (
                irreligious double OPTIONS (NAMEINSOURCE 'Religion.No religion'),
                country_area double OPTIONS (NAMEINSOURCE 'area'),
                country_name varchar(255) PRIMARY KEY OPTIONS (NAMEINSOURCE 'name') )
              OPTIONS (NAMEINSOURCE 'countries');