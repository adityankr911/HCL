USE  InsuranceDB;
CREATE TABLE dim_date (
  date_key INT PRIMARY KEY,
  `date` DATE NOT NULL,
  day INT,
  month INT,
  month_name VARCHAR(20),
  quarter INT,
  year INT,
  is_weekend TINYINT,
  fiscal_year INT,
  load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE dim_region (
  region_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  region_code VARCHAR(20) UNIQUE,
  region_name VARCHAR(100),
  country VARCHAR(100) DEFAULT 'United States',
  load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- DROP TABLE dim_address;
CREATE TABLE dim_address_scd (
    address_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    address_id VARCHAR(100),                  -- natural key

    country VARCHAR(100),
    region VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    full_address VARCHAR(500),

    region_sk BIGINT,

    -- SCD2 metadata
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current TINYINT DEFAULT 1,
    row_hash VARCHAR(200),

    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_region FOREIGN KEY (region_sk)
        REFERENCES dim_region(region_sk)
);

CREATE TABLE dim_policy_type (
  policy_type_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  policy_type_code VARCHAR(50) UNIQUE,
  policy_type_name VARCHAR(100),
  description TEXT,
  load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_customer_scd (
    customer_sk BIGINT PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,

    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(20),
    date_of_birth DATE,

    segment VARCHAR(50),
    marital_status VARCHAR(50),

    address_sk BIGINT,
    
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN,
    row_hash VARCHAR(200),

    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    source_region VARCHAR(100),
    source_file VARCHAR(500),

    CONSTRAINT fk_dim_address_scd
        FOREIGN KEY (address_sk)
        REFERENCES dim_address_scd(address_sk)
);


CREATE INDEX idx_customer_natural ON dim_customer_scd(customer_id, is_current);

CREATE TABLE dim_policy_scd (
  policy_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  policy_id VARCHAR(100) NOT NULL,
  policy_name VARCHAR(200),
  policy_type_sk BIGINT,
  term VARCHAR(50),
  start_date DATE,
  completion_date DATE,
  status VARCHAR(50),
  customer_id VARCHAR(100),
  customer_sk BIGINT,
  premium_amount DECIMAL(18,2),
  effective_from DATE NOT NULL,
  effective_to DATE,
  is_current TINYINT DEFAULT 1,
  row_hash VARCHAR(64),
  load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  source_region VARCHAR(50),
  source_file VARCHAR(255),
  FOREIGN KEY (policy_type_sk) REFERENCES dim_policy_type(policy_type_sk)
);

CREATE INDEX idx_policy_natural ON dim_policy_scd(policy_id, is_current);

CREATE TABLE fact_policy_txn (
  fact_txn_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  txn_id VARCHAR(100),
  policy_sk BIGINT NOT NULL,
  customer_sk BIGINT NOT NULL,
  date_key INT NOT NULL,
  region_sk BIGINT,
  policy_type_sk BIGINT,
  installment_number INT,
  transaction_amount DECIMAL(18,2),
  policy_amount DECIMAL(18,2),
  premium_amount DECIMAL(18,2),
  premium_paid DECIMAL(18,2),
  late_fee DECIMAL(18,2),
  days_late INT,
  payment_status VARCHAR(50),
  payment_method VARCHAR(50),
  is_reversal TINYINT DEFAULT 0,
  load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  source_region VARCHAR(50),
  source_file VARCHAR(255),
  FOREIGN KEY (policy_sk) REFERENCES dim_policy_scd(policy_sk),
  FOREIGN KEY (customer_sk) REFERENCES dim_customer_scd(customer_sk),
  FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
  FOREIGN KEY (region_sk) REFERENCES dim_region(region_sk),
  FOREIGN KEY (policy_type_sk) REFERENCES dim_policy_type(policy_type_sk)
);

CREATE INDEX idx_fact_date ON fact_policy_txn(date_key);
CREATE INDEX idx_fact_policy_date ON fact_policy_txn(policy_sk, date_key);



