--FOR NEW USER INSERTING ONLY
CREATE TABLE customer_data (
    cu_name VARCHAR(255),
    ph_no VARCHAR(20),
    address VARCHAR(255),
    postal_code VARCHAR(10),
    valid_date DATE
);

DROP TABLE customer_data;

CREATE TYPE customer_type AS (
    cu_name VARCHAR,
    ph_no VARCHAR,
    address VARCHAR,
    postal_code VARCHAR,
	valid_date DATE
);

CREATE OR REPLACE PROCEDURE add_to_customers_type_from_table()
LANGUAGE plpgsql
AS $$
DECLARE
    customer customer_type;
BEGIN
    -- Iterate through each record in the composite type
    FOR customer IN
        SELECT cu_name, ph_no, address, postal_code, valid_date FROM customer_data
    LOOP
        -- Insert the record into your original table (assumed name: original_customer_table)
        INSERT INTO raw_customers (cu_name, ph_no, address, postal_code, valid_from)
        VALUES (customer.cu_name, customer.ph_no, customer.address, customer.postal_code, customer.valid_date);
    END LOOP;
END;
$$;

--FOR UPDATE USER ONLY
UPDATE raw_customers_scd
SET VARCHAR(100) = VARCHAR(100)
WHERE VARCHAR(50) = VARCHAR(50) OR VARCHAR(50) = VARCHAR(50);

CREATE TABLE update_data (
	cu_id VARCHAR(50),
    ph_no VARCHAR(20),
    address VARCHAR(255),
    postal_code VARCHAR(10)
);

CREATE TYPE update_type AS (
	cu_id VARCHAR,
    ph_no VARCHAR,
    address VARCHAR,
    postal_code VARCHAR
);

CREATE OR REPLACE PROCEDURE add_update_customers_type_from_table()
LANGUAGE plpgsql
AS $$
DECLARE
    customer update_type;
    update_query TEXT;
    field BOOLEAN;
BEGIN
    -- Iterate through each record in the customer_data table
    FOR customer IN
        SELECT cu_id, ph_no, address, postal_code FROM update_data
    LOOP
        -- Start constructing the UPDATE query
        update_query := 'UPDATE raw_customers SET ';
        field := TRUE;

        IF customer.ph_no IS NOT NULL THEN
            IF NOT field THEN
                update_query := update_query || ', ';
            END IF;
            update_query := update_query || 'ph_no = ' || quote_literal(customer.ph_no);
            field := FALSE;
        END IF;

        IF customer.address IS NOT NULL THEN
            IF NOT field THEN
                update_query := update_query || ', ';
            END IF;
            update_query := update_query || 'address = ' || quote_literal(customer.address);
            field := FALSE;
        END IF;

        IF customer.postal_code IS NOT NULL THEN
            IF NOT first_field THEN
                update_query := update_query || ', ';
            END IF;
            update_query := update_query || 'postal_code = ' || quote_literal(customer.postal_code);
            field := FALSE;
        END IF;

        -- Add the WHERE clause to filter by cu_id
        update_query := update_query || ' WHERE cu_id = ' || quote_literal(customer.cu_id);

        -- Execute the dynamically constructed query
        EXECUTE update_query;
    END LOOP;
END;
$$;

--CREATE TWO TABLES FOR ALIGN WITH SCD METHOD
CREATE TABLE raw_customers(
	cu_id VARCHAR(10) PRIMARY KEY,
	cu_name VARCHAR(50) NOT NULL,
	ph_no VARCHAR(50) NOT NULL,
	address VARCHAR(100) NOT NULL,
	postal_code VARCHAR(50) NOT NULL,
	valid_from DATE DEFAULT CURRENT_DATE
);

CREATE TABLE dim_customers(
	cu_name VARCHAR(50), 
	ph_no VARCHAR(50), 
	address VARCHAR(100), 
	postal_code VARCHAR(50), 
	valid_from DATE, 
	valid_to DATE NULL, 
	current_record BOOLEAN DEFAULT TRUE, 
	active_status CHAR(1) DEFAULT '1'
	);

--GENERATE CU_ID FOR INSERT ROWS
CREATE TRIGGER before_insert_generate_cu_id
BEFORE INSERT ON raw_customers
FOR EACH ROW
WHEN (NEW.cu_id IS NULL) 
EXECUTE FUNCTION generate_cu_id();

CREATE OR REPLACE FUNCTION generate_cu_id()
RETURNS TRIGGER AS $$
BEGIN
    -- Generate the cu_id with a prefix and a formatted sequence number
    NEW.cu_id := 'Cu-0' || LPAD(nextval('custom_id_seq')::TEXT, 1, '0');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Create the sequence if it doesn't exist:
CREATE SEQUENCE IF NOT EXISTS custom_id_seq START 1;

--Reset the sequence to start from 1:
ALTER SEQUENCE custom_id_seq RESTART WITH 1;
    
--INSERT TRIGGER FOR RAW_CUSTOMER TABLE
CREATE TRIGGER insert_trg_Customers
AFTER INSERT 
ON raw_customers
FOR EACH ROW
EXECUTE FUNCTION insert_trg_dim();

--INSERT TRIGGER FUNCTION
CREATE OR REPLACE FUNCTION insert_trg_dim()
RETURNS TRIGGER AS $$
DECLARE
    cu_name VARCHAR(50);
    ph_no VARCHAR(50);
    address VARCHAR(100);
    postal_code VARCHAR(50);
    valid_date DATE;
BEGIN
    -- Assigning values from NEW (the new row being inserted)
    cu_name := NEW.cu_name;
    ph_no := NEW.ph_no;
    address := NEW.address;
    postal_code := NEW.postal_code;
	valid_date := NEW.valid_from;

    -- Inserting into the dim_customers_scd table
    INSERT INTO dim_customers(cu_name, ph_no, address, postal_code, valid_from)
    VALUES (cu_name, ph_no, address, postal_code, valid_date);
    
    -- Returning the NEW row
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--UPDATE TRIGGER TO RAW_CUSTOMER
CREATE TRIGGER update_trg_Customers
AFTER UPDATE
ON raw_customers
FOR EACH ROW
EXECUTE FUNCTION update_trg_dim();

--UPDATE TRIGGER FUNCTION
CREATE OR REPLACE FUNCTION update_trg_dim()
RETURNS TRIGGER AS $$
DECLARE
    cu_id VARCHAR(10);
	valid_date DATE;
BEGIN
    cu_id := NEW.cu_id;
    UPDATE dim_customers
    SET valid_to = CURRENT_DATE,
        current_record = FALSE,
        active_status = '0'
    WHERE cu_name = NEW.cu_name
      AND valid_to IS NULL; -- Update only the current active record

    -- Insert the new version of the customer record with updated details
	valid_date := CURRENT_DATE;
    INSERT INTO dim_customers (cu_name, ph_no, address, postal_code, valid_from)
    VALUES (NEW.cu_name, NEW.ph_no, NEW.address, NEW.postal_code, valid_date); -- Insert the new active record

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

INSERT INTO customer_data VALUES ('Phone Myat Kaung', '0966778899', 'Yangon', '11061', '2023-10-08');

CALL add_to_customers_type_from_table();

INSERT INTO update_data(cu_id, ph_no) VALUES ('Cu-01', '09987860037');

CALL add_update_customers_type_from_table();

DROP TABLE dim_customers;

TRUNCATE dim_customers;

raw_customers

dim_customers







