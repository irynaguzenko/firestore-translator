CREATE DATABASE world;
USE DATABASE world;

CREATE FOREIGN DATA WRAPPER firestore;
CREATE SERVER firestoreConnectionFactory FOREIGN DATA WRAPPER firestore;

CREATE SCHEMA basicSchema SERVER firestoreConnectionFactory;
SET SCHEMA basicSchema;

CREATE FOREIGN TABLE CountriesT (
                country_area double OPTIONS (NAMEINSOURCE '/countries/{id}/area'),
                country_name varchar(255) PRIMARY KEY OPTIONS (NAMEINSOURCE '/countries/{id}/name') )
              OPTIONS (NAMEINSOURCE 'countries');