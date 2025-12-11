-- ============================================================================
-- BANKING FRAUD DETECTION AND RISK ANALYTICS DATABASE
-- Complete Schema with Stored Procedures, Functions, Views and Triggers
-- ============================================================================

DROP DATABASE IF EXISTS banking_fraud_db;

CREATE DATABASE banking_fraud_db;

USE banking_fraud_db;

-- ============================================================================
-- PART 1: CREATE ALL TABLES WITH FOREIGN KEYS
-- ============================================================================

CREATE TABLE branch (
    branch_id INT PRIMARY KEY,
    branch_name VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    region VARCHAR(20) NOT NULL,
    INDEX idx_state (state),
    INDEX idx_city (city)
) ENGINE=InnoDB;

CREATE TABLE banker (
    banker_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    role VARCHAR(50) NOT NULL,
    branch_id INT NOT NULL,
    experience_years INT NOT NULL,
    INDEX idx_role (role),
    INDEX idx_branch (branch_id)
) ENGINE=InnoDB;

CREATE TABLE customer (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    gender VARCHAR(10) NOT NULL,
    date_of_birth DATE NOT NULL,
    occupation VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    risk_score DECIMAL(3,2) NOT NULL,
    age INT NOT NULL,
    INDEX idx_occupation (occupation),
    INDEX idx_state (state),
    INDEX idx_risk_score (risk_score)
) ENGINE=InnoDB;


CREATE TABLE account (
    account_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    branch_id INT NOT NULL,
    account_type VARCHAR(20) NOT NULL,
    opening_date DATE NOT NULL,
    balance DECIMAL(12,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    account_age_days INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (branch_id) REFERENCES branch(branch_id),
    INDEX idx_customer (customer_id),
    INDEX idx_status (status)
) ENGINE=InnoDB;

CREATE TABLE loan (
    loan_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    loan_type VARCHAR(20) NOT NULL,
    loan_amount DECIMAL(12,2) NOT NULL,
    issue_date DATE NOT NULL,
    loan_status VARCHAR(20) NOT NULL,
    is_fraud TINYINT(1) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    INDEX idx_customer (customer_id),
    INDEX idx_is_fraud (is_fraud)
) ENGINE=InnoDB;


CREATE TABLE transaction (
    transaction_id INT PRIMARY KEY,
    account_id INT NOT NULL,
    transaction_date DATETIME NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    is_fraud TINYINT(1) NOT NULL,
    FOREIGN KEY (account_id) REFERENCES account(account_id),
    INDEX idx_account (account_id),
    INDEX idx_is_fraud (is_fraud),
    INDEX idx_date (transaction_date)
) ENGINE=InnoDB;


CREATE TABLE fraud_case (
    fraud_id INT PRIMARY KEY,
    transaction_id INT NOT NULL,
    fraud_type VARCHAR(30) NOT NULL,
    detection_method VARCHAR(30) NOT NULL,
    report_date DATE NOT NULL,
    resolution_status VARCHAR(20) NOT NULL,
    loss_amount DECIMAL(12,2) NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES transaction(transaction_id),
    INDEX idx_transaction (transaction_id),
    INDEX idx_fraud_type (fraud_type)
) ENGINE=InnoDB;

CREATE TABLE credit_card (
    card_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    card_type VARCHAR(30) NOT NULL,
    limit_amount DECIMAL(12,2) NOT NULL,
    issue_date DATE NOT NULL,
    is_active TINYINT(1) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
) ENGINE=InnoDB;

SELECT '✅ All tables created successfully!' AS Status;

show tables;

-- ============================================================================
-- PART 2: LOAD DATA FROM CSV FILES
-- ============================================================================

SELECT '============================================' AS '';
SELECT '📂 LOADING DATA FROM CSV FILES' AS '';
SELECT '============================================' AS '';

USE banking_fraud_db;

SET FOREIGN_KEY_CHECKS = 0;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.4\\Uploads\\branch.csv'
INTO TABLE branch
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(branch_id, branch_name, city, state, region);

-------------------------------------------------------
-- 2. BANKER
-------------------------------------------------------

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.4\\Uploads\\banker.csv'
INTO TABLE banker
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(banker_id, name, role, branch_id, experience_years);

-------------------------------------------------------
-- 3. CUSTOMER
-------------------------------------------------------

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.4\\Uploads\\customer.csv'
INTO TABLE customer
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(customer_id,
 name,
 gender,
 @dob,          -- take raw date string here
 occupation,
 city,
 state,
 risk_score,
 age)
SET date_of_birth = STR_TO_DATE(@dob, '%d-%m-%Y');   -- convert DD-MM-YYYY → DATE

-------------------------------------------------------
-- 4. ACCOUNT
-------------------------------------------------------

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.4\\Uploads\\account.csv'
INTO TABLE account
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(account_id,
 customer_id,
 branch_id,
 account_type,
 @opening_date,      -- read raw string here
 balance,
 status,
 account_age_days)
SET opening_date = STR_TO_DATE(@opening_date, '%d-%m-%Y');   -- convert DD-MM-YYYY → DATE

-------------------------------------------------------
-- 5. LOAN
-------------------------------------------------------
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.4\\Uploads\\loan.csv'
INTO TABLE loan
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(loan_id,
 customer_id,
 loan_type,
 loan_amount,
 @issue_date,         -- read raw DD-MM-YYYY
 loan_status,
 is_fraud)
SET issue_date = STR_TO_DATE(@issue_date, '%d-%m-%Y');  -- convert to DATE

-------------------------------------------------------
-- 6. TRANSACTION
-------------------------------------------------------

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.4\\Uploads\\transaction.csv'
INTO TABLE `transaction`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(transaction_id,
 account_id,
 @tdate,              -- read raw datetime text
 transaction_type,
 amount,
 channel,
 is_fraud)
SET transaction_date = STR_TO_DATE(@tdate, '%d-%m-%Y %H:%i');

-------------------------------------------------------
-- 7. FRAUD_CASE
-------------------------------------------------------

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.4\\Uploads\\fraud_case.csv'
INTO TABLE fraud_case
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(fraud_id,
 transaction_id,
 fraud_type,
 detection_method,
 @rdate,                -- read raw date
 resolution_status,
 loss_amount)
SET report_date = STR_TO_DATE(@rdate, '%d-%m-%Y');   -- convert DD-MM-YYYY to DATE

-------------------------------------------------------
-- 8. CREDIT_CARD  (as per your CREATE TABLE)
-------------------------------------------------------

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.4\\Uploads\\credit_card.csv'
INTO TABLE credit_card
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(card_id,
 customer_id,
 card_type,
 limit_amount,
 @issue_date,
 is_active)
SET issue_date = STR_TO_DATE(@issue_date, '%Y-%m-%d');
   -- 03-02-2017 → 2017-02-03

SET FOREIGN_KEY_CHECKS = 1;

select * from transaction;
select * from credit_card;
select * from fraud_case;
select * from loan;
select * from customer;
select * from banker;
select * from branch;
select * from account;

show tables from   fraud_detection_db;
use  fraud_detection_db;

-- Verify row counts
SELECT 'customer' AS table_name, COUNT(*) AS row_count FROM customer
UNION ALL
SELECT 'branch', COUNT(*) FROM branch
UNION ALL
SELECT 'banker', COUNT(*) FROM banker
UNION ALL
SELECT 'account', COUNT(*) FROM account
UNION ALL
SELECT 'credit_card', COUNT(*) FROM credit_card
UNION ALL
SELECT 'loan', COUNT(*) FROM loan
UNION ALL
SELECT 'transaction', COUNT(*) FROM transaction
UNION ALL
SELECT 'fraud_case', COUNT(*) FROM fraud_case;

-- =============================================
---   STORED PROCEDURES WITH CALL STATEMENTS
-- =============================================

---- Q1. Find the total number of customers.

DELIMITER $$
CREATE PROCEDURE total_customers()
BEGIN
    SELECT COUNT(*) AS total_customers
    FROM customer;
END $$
DELIMITER ;

call total_customers();

---- Q2. Find the total number of transactions.

DELIMITER $$
CREATE PROCEDURE total_transactions()
BEGIN
    SELECT COUNT(*) AS total_transactions
    FROM transaction;
END $$

DELIMITER ;

call total_transactions();

Q3. Find the total transaction amount in the bank.

DELIMITER $$

CREATE PROCEDURE total_transaction_amount()
BEGIN
    SELECT SUM(amount) AS total_transaction_amount
    FROM transaction;
END $$

DELIMITER ;

call total_transaction_amount();

Q4. Find the total number of fraudulent transactions.

DELIMITER $$

CREATE PROCEDURE total_fraud_transactions()
BEGIN
    SELECT COUNT(*) AS total_fraud_transactions
    FROM transaction
    WHERE is_fraud = 1;
END $$

DELIMITER ;

call total_fraud_transactions();

Q5. Show total transaction amount by transaction type.

DELIMITER $$

CREATE PROCEDURE amount_by_transaction_type()
BEGIN
    SELECT 
        transaction_type,
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_amount
    FROM  transaction
    GROUP BY transaction_type
    ORDER BY total_amount DESC;
END $$

DELIMITER ;

call amount_by_transaction_type();

Q6. Show number of loans by loan type.

DELIMITER $$

CREATE PROCEDURE loans_by_loan_type()
BEGIN
    SELECT 
        loan_type,
        COUNT(*) AS total_loans
    FROM loan
    GROUP BY loan_type
    ORDER BY total_loans DESC;
END $$

DELIMITER ;

call loans_by_loan_type();

Q7. Show total loan amount by loan status.

DELIMITER $$

CREATE PROCEDURE loan_amount_by_status()
BEGIN
    SELECT 
        loan_status,
        COUNT(*) AS total_loans,
        SUM(loan_amount) AS total_loan_amount
    FROM  loan
    GROUP BY loan_status
    ORDER BY total_loan_amount DESC;
END $$

DELIMITER ;

call loan_amount_by_status();

Q8. Show number of credit cards by card type.

DELIMITER $$

CREATE PROCEDURE cards_by_card_type()
BEGIN
    SELECT 
        card_type,
        COUNT(*) AS total_cards
    FROM credit_card
    GROUP BY card_type
    ORDER BY total_cards DESC;
END $$

DELIMITER ;

call cards_by_card_type();

Q9. Show number of branches in each region.

DELIMITER $$

CREATE PROCEDURE branches_by_region()
BEGIN
    SELECT 
        region,
        COUNT(*) AS total_branches
    FROM branch
    GROUP BY region
    ORDER BY total_branches DESC;
END $$

DELIMITER ;

call branches_by_region();

Q10. Show number of accounts by account status.

DELIMITER $$

CREATE PROCEDURE accounts_by_status()
BEGIN
    SELECT 
        status,
        COUNT(*) AS total_accounts
    FROM account
    GROUP BY status
    ORDER BY total_accounts DESC;
END $$

DELIMITER ;

call accounts_by_status();


Q11. For each branch, find how many accounts it has.

DELIMITER $$

CREATE PROCEDURE accounts_per_branch()
BEGIN
    SELECT 
        b.branch_id,
        b.branch_name,
        COUNT(a.account_id) AS total_accounts
    FROM branch b
    LEFT JOIN account a
        ON b.branch_id = a.branch_id
    GROUP BY b.branch_id, b.branch_name
    ORDER BY total_accounts DESC;
END $$

DELIMITER ;

call accounts_per_branch();

Q12. For each branch, find the total transaction amount.

DELIMITER $$

CREATE PROCEDURE transaction_amount_per_branch()
BEGIN
    SELECT 
        b.branch_id,
        b.branch_name,
        SUM(t.amount) AS total_transaction_amount
    FROM branch b
    LEFT JOIN account a
        ON b.branch_id = a.branch_id
    LEFT JOIN transaction t
        ON a.account_id = t.account_id
    GROUP BY b.branch_id, b.branch_name
    ORDER BY total_transaction_amount DESC;
END $$
DELIMITER ;

call transaction_amount_per_branch();

Q13. For each customer, find how many accounts they have.

DELIMITER $$

CREATE PROCEDURE accounts_per_customer()
BEGIN
    SELECT 
        c.customer_id,
        c.name AS customer_name,
        COUNT(a.account_id) AS total_accounts
    FROM customer c
    LEFT JOIN account a
        ON c.customer_id = a.customer_id
    GROUP BY c.customer_id, c.name
    ORDER BY total_accounts DESC;
END $$

DELIMITER ;

call accounts_per_customer();

Q14. For each city, find the total loan amount (based on customer city).

DELIMITER $$

CREATE PROCEDURE loan_amount_by_city()
BEGIN
    SELECT 
        c.city,
        SUM(l.loan_amount) AS total_loan_amount,
        COUNT(l.loan_id) AS total_loans
    FROM customer c
    JOIN  loan l
        ON c.customer_id = l.customer_id
    GROUP BY c.city
    ORDER BY total_loan_amount DESC;
END $$

DELIMITER ;

call loan_amount_by_city();

Q15. For each card type, find average risk score of card holders.

DELIMITER $$

CREATE PROCEDURE avg_risk_by_card_type()
BEGIN
    SELECT 
        cc.card_type,
        AVG(c.risk_score) AS avg_risk_score,
        COUNT(DISTINCT cc.customer_id) AS total_customers
    FROM credit_card cc
    JOIN customer c
        ON cc.customer_id = c.customer_id
    GROUP BY cc.card_type
    ORDER BY avg_risk_score DESC;
END $$

DELIMITER ;

call avg_risk_by_card_type();

Q16. For each branch, find total number of fraudulent transactions.

DELIMITER $$

CREATE PROCEDURE fraud_transactions_per_branch()
BEGIN
    SELECT 
        b.branch_id,
        b.branch_name,
        COUNT(t.transaction_id) AS fraud_transactions
    FROM branch b
    LEFT JOIN account a
        ON b.branch_id = a.branch_id
    LEFT JOIN transaction t
        ON a.account_id = t.account_id
    WHERE t.is_fraud = 1
    GROUP BY b.branch_id, b.branch_name
    ORDER BY fraud_transactions DESC;
END $$

DELIMITER ;

call fraud_transactions_per_branch();

Q17. For a given customer, list all their transactions.

(Uses an IN parameter.)

DELIMITER $$

CREATE PROCEDURE transactions_by_customer(
    IN p_customer_id INT
)
BEGIN
    SELECT 
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.transaction_type,
        t.amount,
        t.channel,
        t.is_fraud
    FROM transaction t
    JOIN account a
        ON t.account_id = a.account_id
    WHERE a.customer_id = p_customer_id
    ORDER BY t.transaction_date;
END $$

DELIMITER ;

CALL transactions_by_customer(101);

Q18. For a given branch, find the total loan amount of its customers.

DELIMITER $$

CREATE PROCEDURE loan_amount_by_branch(
    IN p_branch_id INT
)
BEGIN
    SELECT 
        b.branch_id,
        b.branch_name,
        SUM(l.loan_amount) AS total_loan_amount,
        COUNT(l.loan_id) AS total_loans
    FROM branch b
    JOIN account a
        ON b.branch_id = a.branch_id
    JOIN customer c
        ON a.customer_id = c.customer_id
    JOIN loan l
        ON c.customer_id = l.customer_id
    WHERE b.branch_id = p_branch_id
    GROUP BY b.branch_id, b.branch_name;
END $$

DELIMITER ;

CALL loan_amount_by_branch(10);

Q19. For each region, find the number of fraud cases.

DELIMITER $$

CREATE PROCEDURE fraud_cases_by_region()
BEGIN
    SELECT 
        b.region,
        COUNT(f.fraud_id) AS total_fraud_cases
    FROM branch b
    LEFT JOIN account a
        ON b.branch_id = a.branch_id
    LEFT JOIN transaction t
        ON a.account_id = t.account_id
    LEFT JOIN fraud_case f
        ON t.transaction_id = f.transaction_id
    GROUP BY b.region
    ORDER BY total_fraud_cases DESC;
END $$

DELIMITER ;

call fraud_cases_by_region();

Q20. For a given date range, show total transaction amount and fraud count.

DELIMITER $$

CREATE PROCEDURE summary_between_dates(
    IN p_start_date DATE,
    IN p_end_date   DATE
)
BEGIN
    SELECT 
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_amount,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS total_fraud_transactions
    FROM transaction
    WHERE DATE(transaction_date) BETWEEN p_start_date AND p_end_date;
END $$

DELIMITER ;

CALL summary_between_dates('2024-01-01', '2024-12-31');




















