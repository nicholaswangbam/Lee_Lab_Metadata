DROP DATABASE IF EXISTS data;
CREATE DATABASE data;
USE data;
DROP TABLE IF EXISTS records;
CREATE TABLE records (
	rowID INT(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    Study_Name TEXT,
    Sample_ID TEXT,
    Age INT,
    Race TEXT,
    Sex TEXT,
    BMI FLOAT,
    Region TEXT,
    Viral_Load INT,
    CD4_Count INT,
    MSM_Status TEXT,
    ART_Status TEXT,
    ART_Regimen TEXT,
    HIV_Status TEXT
);
