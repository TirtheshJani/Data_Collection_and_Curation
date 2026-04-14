CREATE DATABASE IF NOT EXISTS EmployeeTest;
USE EmployeeTest;

CREATE TABLE IF NOT EXISTS high_salary (
    Id VARCHAR(255) NOT NULL,
    Name VARCHAR(255) NOT NULL,
    Department VARCHAR(255) NOT NULL,
    Salary INT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_high_salary_department (Department),
    INDEX idx_high_salary_salary (Salary)
);

CREATE TABLE IF NOT EXISTS low_salary (
    Id VARCHAR(255) NOT NULL,
    Name VARCHAR(255) NOT NULL,
    Department VARCHAR(255) NOT NULL,
    Salary INT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_low_salary_department (Department),
    INDEX idx_low_salary_salary (Salary)
);
