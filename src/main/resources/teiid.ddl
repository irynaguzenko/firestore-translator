CREATE DATABASE world;
USE DATABASE world;

CREATE FOREIGN DATA WRAPPER firestore;
CREATE SERVER firestoreConnectionFactory FOREIGN DATA WRAPPER firestore;

CREATE SCHEMA basicSchema SERVER firestoreConnectionFactory;
SET SCHEMA basicSchema;

CREATE FOREIGN TABLE CountriesT (
                id varchar (255) PRIMARY KEY OPTIONS (NAMEINSOURCE '__name__'),
                irreligious double OPTIONS (NAMEINSOURCE 'Religion.No religion'),
                president_name varchar (255) OPTIONS (NAMEINSOURCE 'power.president.name'),
                right_side_driving boolean OPTIONS (NAMEINSOURCE 'right-side-driving'),
                country_capital varchar (255) OPTIONS (NAMEINSOURCE 'capital'),
                country_area double OPTIONS (NAMEINSOURCE 'area'),
                test boolean OPTIONS (NAMEINSOURCE 'test'),
                int_array integer[] OPTIONS (NAMEINSOURCE 'intArray'),
                varchar_array varchar[] OPTIONS (NAMEINSOURCE 'varcharArray'),
                country_name varchar (255) OPTIONS (NAMEINSOURCE 'name') )
              OPTIONS (NAMEINSOURCE 'countries', UPDATABLE TRUE);

CREATE FOREIGN TABLE CitiesT (
                id varchar (255) PRIMARY KEY OPTIONS (NAMEINSOURCE '__name__'),
                parent_id varchar (255) OPTIONS (NAMEINSOURCE '__parent_name__'),
                city_name varchar (255) OPTIONS (NAMEINSOURCE 'name'),
                population double OPTIONS (NAMEINSOURCE 'population'))
              OPTIONS (NAMEINSOURCE 'cities', UPDATABLE TRUE);