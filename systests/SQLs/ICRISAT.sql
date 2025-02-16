CREATE TABLE ICRISAT (
    Station TEXT NOT NULL,
    obvdate DATE PRIMARY KEY,
    MaxT FLOAT,
    MinT FLOAT,
    RH1 FLOAT,
    RH2 FLOAT,
    Wind FLOAT,
    Rain FLOAT,
    SSH FLOAT,
    Evap FLOAT,
    Radiation FLOAT,
    FAO56_ET FLOAT,
    Lat FLOAT,
    Lon FLOAT,
    Cum_Rain FLOAT
);
