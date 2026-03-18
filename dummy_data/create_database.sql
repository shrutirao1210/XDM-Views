-- =============================================================================
-- Customer Database Schema and Dummy Data
-- =============================================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS Customer;

-- Create Customer table
CREATE TABLE Customer (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL
);

-- Insert dummy customer data
INSERT INTO Customer (customer_id, name, city) VALUES
(1, 'Alice Johnson', 'New York'),
(2, 'Bob Smith', 'Los Angeles'),
(3, 'Charlie Brown', 'Chicago'),
(4, 'Diana Prince', 'Houston'),
(5, 'Eve Wilson', 'Phoenix'),
(6, 'Frank Miller', 'Philadelphia'),
(7, 'Grace Lee', 'San Antonio'),
(8, 'Henry Taylor', 'San Diego'),
(9, 'Isaac Newton', 'Dallas'),
(10, 'Julia Roberts', 'San Jose');

-- Verify data
SELECT * FROM Customer;
