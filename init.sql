CREATE DATABASE IF NOT EXISTS EmployeeTest;
USE EmployeeTest;

CREATE TABLE IF NOT EXISTS high_salary (
    Id VARCHAR(255),
    Name VARCHAR(255),
    Department VARCHAR(255),
    Salary INT,
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS low_salary (
    Id VARCHAR(255),
    Name VARCHAR(255),
    Department VARCHAR(255),
    Salary INT,
    timestamp TIMESTAMP
);
