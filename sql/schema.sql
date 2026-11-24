CREATE DATABASE IF NOT EXISTS autopulse_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE autopulse_db;

DROP TABLE IF EXISTS trip_logs;
DROP TABLE IF EXISTS vehicles;

CREATE TABLE vehicles (
    vehicle_id INT PRIMARY KEY,
    model_name VARCHAR(64) NOT NULL,
    fuel_type VARCHAR(32) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE trip_logs (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL UNIQUE,
    vehicle_id INT NOT NULL,
    trip_date DATETIME NOT NULL,
    distance_km DECIMAL(10, 2) NOT NULL,
    amount_inr DECIMAL(12, 2) NOT NULL,
    txn_type VARCHAR(32) NOT NULL,
    is_outlier TINYINT(1) NOT NULL DEFAULT 0,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_trip_date (trip_date),
    INDEX idx_vehicle_id (vehicle_id),
    INDEX idx_txn_type (txn_type),
    CONSTRAINT fk_trip_vehicle
        FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);