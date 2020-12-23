# DROP TABLES

staging_immigration_table_drop = "DROP TABLE IF EXISTS staging_immigration"
staging_temperature_table_drop = "DROP TABLE IF EXISTS staging_temperature"
staging_demographic_table_drop = "DROP TABLE IF EXISTS staging_demographic"
city_table_drop = "DROP TABLE IF EXISTS city"
immigrant_table_drop = "DROP TABLE IF EXISTS immigrant"
temperature_table_drop = "DROP TABLE IF EXISTS temperature"
demographic_table_drop = "DROP TABLE IF EXISTS demographic"


# CREATE TABLES

staging_immigration_table_create= ("""CREATE TABLE IF NOT EXISTS staging_immigration(
                                    cicid INT,
                                    i94yr INT,
                                    i94mon INT,
                                    i94cit VARCHAR,
                                    i94res INT,
                                    i94port VARCHAR,
                                    arrdate DATE,
                                    i94mode INT,
                                    i94addr VARCHAR(2),
                                    depdate DATE,
                                    i94bir INT,
                                    i94visa INT,
                                    biryear INT,
                                    gender VARCHAR(1),
                                    airline VARCHAR,
                                    fltno VARCHAR,
                                    visatype VARCHAR);
                                    """)

staging_temperature_table_create = ("""CREATE TABLE IF NOT EXISTS staging_temperature(
                                    dt DATE,
                                    AverageTemperature FLOAT,
                                    AverageTemperatureUncertainty FLOAT, 
                                    City VARCHAR, 
                                    Country VARCHAR,
                                    Longitude VARCHAR,
                                    Latitude VARCHAR);
                                    """)

staging_demographic_table_create = ("""CREATE TABLE IF NOT EXISTS staging_demographic(
                                    city VARCHAR,
                                    state VARCHAR,
                                    median_age FLOAT,
                                    female_population INT,
                                    male_population INT,
                                    total_population INT,
                                    foreign_born INT,
                                    state_code VARCHAR,           
                                    Indian_Alaska INT,
                                    Asian INT,
                                    Black INT,
                                    Hispanic INT,
                                    White INT,
                                    major_race VARCHAR
                                    );
                                    """)

city_table_create = ("""CREATE TABLE IF NOT EXISTS city(
                        city_id VARCHAR PRIMARY KEY,
                        city_name VARCHAR, 
                        state VARCHAR,
                        average_temperature FLOAT,
                        temperature_datetime DATE,
                        major_race VARCHAR,
                        total_population INT,
                        total_immigrants INT
                        );
                        """)


immigrant_table_create = ("""CREATE TABLE IF NOT EXISTS immigrant
                        ( immigrant_id VARCHAR PRIMARY KEY,
                        gender VARCHAR(1), 
                       	age INT NOT NULL,
                        arrival_date DATE,
                        departure_date DATE,
                        orign_city VARCHAR ,
                        desination_city VARCHAR,
                        state_code VARCHAR(2),
                        visa_type INT,
                        transport_mode INT);
                        """)

temperature_table_create = ("""CREATE TABLE IF NOT EXISTS temperature
                            ( temperature_id VARCHAR PRIMARY KEY,
                            datetime DATE,
                            average_temperature FLOAT, 
                            average_temperature_uncertainty FLOAT,
                            city VARCHAR,
                            country VARCHAR ,
                            latitude VARCHAR,
                            longitude VARCHAR);""")

demographic_table_create = ("""CREATE TABLE IF NOT EXISTS demographic
                            (demographic_id VARCHAR PRIMARY KEY,
                            city VARCHAR,   
                            state VARCHAR,
                            median_age FLOAT ,
                            female_population INT,
                            male_population INT,
                            total_population INT,
                            foreign_born INT,
                            state_code VARCHAR(2),
                            major_race VARCHAR);""")
                       
                      
city_table_insert = ("""INSERT INTO city (city_id, city_name, state, average_temperature, temperature_datetime, 
                        major_race, total_population, total_immigrants)
                        SELECT 
                        md5(d.city || d.state) city_id,
                        d.city,
                        d.state,
                        t.AverageTemperature,
                        t.dt,
                        d.major_race,
                        d.total_population,
                        (SELECT DISTINCT COUNT(cicid) FROM staging_immigration WHERE i94port= LOWER(d.city) GROUP BY i94port)
                        FROM staging_demographic AS d  LEFT JOIN staging_temperature AS t ON LOWER(d.city) = LOWER(t.City) 
                        """)
                        

immigrant_table_insert = ("""INSERT INTO immigrant (immigrant_id, gender, age, arrival_date, departure_date, 
                            orign_city, desination_city, state_code, visa_type, transport_mode)
                            SELECT 
                            md5(cicid),
                            gender,
                            i94bir,
                            arrdate,
                            depdate,
                            i94cit,
                            i94port,
                            i94addr,
                            i94visa,
                            i94mode
                            FROM staging_immigration                   
                            """)

temperature_table_insert = ("""INSERT INTO temperature (temperature_id,datetime,average_temperature, average_temperature_uncertainty,
                            city, country, latitude, longitude)             
                            SELECT 
                            md5(City||dt) temperature_id,
                            dt,
                            AverageTemperature,
                            AverageTemperatureUncertainty, 
                            City, 
                            Country,
                            Longitude,
                            Latitude
                            FROM staging_temperature    
                            """)

demographic_table_insert = ("""INSERT INTO demographic (demographic_id,city, state, median_age, female_population, male_population, 
                            total_population, foreign_born, state_code, major_race)
                           SELECT 
                           md5(city || state) demographic_id,
                           city,
                           state,
                           median_age,
                           female_population,
                           male_population,                       
                           total_population,
                           foreign_born,
                           state_code,
                           major_race
                           FROM staging_demographic
                           """)


# QUERY LISTS

create_table_queries = [staging_immigration_table_create, staging_temperature_table_create ,staging_demographic_table_create, city_table_create, immigrant_table_create, temperature_table_create, demographic_table_create]
drop_table_queries = [staging_immigration_table_drop, staging_temperature_table_drop, staging_demographic_table_drop, city_table_drop, immigrant_table_drop, temperature_table_drop, demographic_table_drop]
insert_table_queries = [city_table_insert, temperature_table_insert, immigrant_table_insert, demographic_table_insert]
