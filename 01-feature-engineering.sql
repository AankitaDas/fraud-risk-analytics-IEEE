CREATE TABLE fraud_transactions (
TransactionID BIGINT,
isFraud INT,
TransactionDT BIGINT,
TransactionAmt FLOAT,
ProductCD TEXT,
card1 INT,
card2 FLOAT,
card3 FLOAT,
card4 TEXT,
card5 FLOAT,
card6 TEXT,
addr1 FLOAT,
addr2 FLOAT,
dist1 FLOAT,
dist2 FLOAT,
P_emaildomain TEXT,
R_emaildomain TEXT
);
CREATE TABLE fraud_identity (
TransactionID BIGINT,
DeviceType TEXT,
DeviceInfo TEXT,
id_12 TEXT,
id_13 FLOAT,
id_14 FLOAT,
id_15 TEXT,
id_16 TEXT,
id_17 FLOAT,
id_18 FLOAT,
id_19 FLOAT,
id_20 FLOAT
);
COPY fraud_transactions
FROM 'C:/Users/anike/Downloads/Fraud_Risk_Project/train_transaction.csv'
DELIMITER ','
CSV HEADER
QUOTE '"'
ESCAPE '"';
drop table fraud_transactions;
TRUNCATE TABLE fraud_transaction;
SELECT COUNT(*) FROM fraud_transaction;
DROP TABLE IF EXISTS stg_transactions;

CREATE TABLE stg_transactions (
    TransactionID BIGINT,
    isFraud INT,
    TransactionDT INT,
    TransactionAmt NUMERIC,
    ProductCD TEXT,
    card1 INT,
    card2 INT,
    card3 INT,
    card4 TEXT,
    card5 INT,
    card6 TEXT,
    addr1 INT,
    addr2 INT,
    dist1 NUMERIC,
    dist2 NUMERIC,
    P_emaildomain TEXT,
    R_emaildomain TEXT
);
INSERT INTO stg_transactions
SELECT
    (string_to_array(raw_data, ','))[1]::INT,
    (string_to_array(raw_data, ','))[2]::INT,
    (string_to_array(raw_data, ','))[3]::INT,
    (string_to_array(raw_data, ','))[4]::NUMERIC,
    (string_to_array(raw_data, ','))[5],
    (string_to_array(raw_data, ','))[6]::INT,
    NULLIF((string_to_array(raw_data, ','))[7],'')::INT,
    (string_to_array(raw_data, ','))[8]::INT,
    (string_to_array(raw_data, ','))[9],
    NULLIF((string_to_array(raw_data, ','))[10],'')::INT,
    (string_to_array(raw_data, ','))[11],
    NULLIF((string_to_array(raw_data, ','))[12],'')::INT,
    NULLIF((string_to_array(raw_data, ','))[13],'')::INT,
    NULLIF((string_to_array(raw_data, ','))[14],'')::NUMERIC,
    NULLIF((string_to_array(raw_data, ','))[15],'')::NUMERIC,
    (string_to_array(raw_data, ','))[16],
    (string_to_array(raw_data, ','))[17]
FROM fraud_transaction
OFFSET 1;
TRUNCATE TABLE stg_transactions;

INSERT INTO stg_transactions
SELECT
    (arr[1])::BIGINT,
    (arr[2])::INT,
    (arr[3])::INT,
    (arr[4])::NUMERIC,
    arr[5],
    (arr[6])::INT,
    NULLIF(arr[7],'')::INT,
    (arr[8])::INT,
    arr[9],
    NULLIF(arr[10],'')::INT,
    arr[11],
    NULLIF(arr[12],'')::INT,
    NULLIF(arr[13],'')::INT,
    NULLIF(arr[14],'')::NUMERIC,
    NULLIF(arr[15],'')::NUMERIC,
    arr[16],
    arr[17]
FROM (
    SELECT string_to_array(raw_data, ',') AS arr
    FROM fraud_transaction
    WHERE raw_data NOT LIKE 'TransactionID%'
) t;
TRUNCATE TABLE stg_transactions;

INSERT INTO stg_transactions
SELECT
    (arr[1])::BIGINT,
    (arr[2])::INT,
    (arr[3])::INT,
    (arr[4])::NUMERIC,
    arr[5],
    (arr[6])::INT,
    NULLIF(arr[7],'')::NUMERIC::INT,
    NULLIF(arr[8],'')::NUMERIC::INT,
    arr[9],
    NULLIF(arr[10],'')::NUMERIC::INT,
    arr[11],
    NULLIF(arr[12],'')::NUMERIC::INT,
    NULLIF(arr[13],'')::NUMERIC::INT,
    NULLIF(arr[14],'')::NUMERIC,
    NULLIF(arr[15],'')::NUMERIC,
    arr[16],
    arr[17]
FROM (
    SELECT string_to_array(raw_data, ',') AS arr
    FROM fraud_transaction
    WHERE raw_data NOT LIKE 'TransactionID%'
) t;
SELECT COUNT(*) FROM stg_transactions;
DROP TABLE IF EXISTS fact_fraud_transactions;

CREATE TABLE fact_fraud_transactions AS
SELECT
    transactionid,
    isfraud,
    transactiondt,
    transactionamt,
    productcd,
    card1,
    card2,
    card3,
    card4,
    card5,
    card6,
    addr1,
    addr2,
    dist1,
    dist2,
    p_emaildomain,
    r_emaildomain
FROM stg_transactions;
DROP TABLE IF EXISTS stg_identity;

CREATE TABLE stg_identity (
    TransactionID BIGINT,
    DeviceType TEXT,
    DeviceInfo TEXT,
    id_12 TEXT,
    id_13 FLOAT,
    id_14 FLOAT,
    id_15 TEXT,
    id_16 TEXT,
    id_17 FLOAT,
    id_18 FLOAT,
    id_19 FLOAT,
    id_20 FLOAT,
    id_30 TEXT,
    id_31 TEXT,
    id_32 FLOAT,
    id_33 TEXT,
    id_34 TEXT,
    id_35 TEXT,
    id_36 TEXT,
    id_37 TEXT,
    id_38 TEXT
);
DROP TABLE IF EXISTS stg_identity;
CREATE TABLE stg_identity (
TransactionID BIGINT,
id_01 FLOAT,
id_02 FLOAT,
id_03 FLOAT,
id_04 FLOAT,
id_05 FLOAT,
id_06 FLOAT,
id_07 FLOAT,
id_08 FLOAT,
id_09 FLOAT,
id_10 FLOAT,
id_11 FLOAT,
id_12 TEXT,
id_13 FLOAT,
id_14 FLOAT,
id_15 TEXT,
id_16 TEXT,
id_17 FLOAT,
id_18 FLOAT,
id_19 FLOAT,
id_20 FLOAT,
id_21 FLOAT,
id_22 FLOAT,
id_23 FLOAT,
id_24 FLOAT,
id_25 FLOAT,
id_26 FLOAT,
id_27 FLOAT,
id_28 TEXT,
id_29 TEXT,
id_30 TEXT,
id_31 TEXT,
id_32 FLOAT,
id_33 TEXT,
id_34 TEXT,
id_35 TEXT,
id_36 TEXT,
id_37 TEXT,
id_38 TEXT,
DeviceType TEXT,
DeviceInfo TEXT
);
DROP TABLE IF EXISTS stg_identity;
CREATE TABLE stg_identity (
TransactionID TEXT,
id_01 TEXT,
id_02 TEXT,
id_03 TEXT,
id_04 TEXT,
id_05 TEXT,
id_06 TEXT,
id_07 TEXT,
id_08 TEXT,
id_09 TEXT,
id_10 TEXT,
id_11 TEXT,
id_12 TEXT,
id_13 TEXT,
id_14 TEXT,
id_15 TEXT,
id_16 TEXT,
id_17 TEXT,
id_18 TEXT,
id_19 TEXT,
id_20 TEXT,
id_21 TEXT,
id_22 TEXT,
id_23 TEXT,
id_24 TEXT,
id_25 TEXT,
id_26 TEXT,
id_27 TEXT,
id_28 TEXT,
id_29 TEXT,
id_30 TEXT,
id_31 TEXT,
id_32 TEXT,
id_33 TEXT,
id_34 TEXT,
id_35 TEXT,
id_36 TEXT,
id_37 TEXT,
id_38 TEXT,
DeviceType TEXT,
DeviceInfo TEXT
);
SELECT COUNT(*) FROM stg_identity;
DROP TABLE IF EXISTS fraud_features;

CREATE TABLE fraud_features AS
SELECT

-- PRIMARY KEY
t.transactionid,

-- TARGET VARIABLE
t.isfraud,

-- BASIC TRANSACTION FEATURES
t.transactionamt,
t.productcd,
t.card1,
t.card2,
t.card3,
t.card4,
t.card5,
t.card6,

-- LOCATION FEATURES
t.addr1,
t.addr2,
t.dist1,
t.dist2,

-- EMAIL DOMAIN
t.p_emaildomain,
t.r_emaildomain,

-- DEVICE BEHAVIOUR FEATURES
i.devicetype,
i.deviceinfo,
i.id_23,
i.id_30,
i.id_31,
i.id_33,

-- 🚨 VERY IMPORTANT RISK FEATURES
CASE 
WHEN i.id_23 LIKE '%TRANSPARENT%' THEN 1
WHEN i.id_23 LIKE '%ANONYMOUS%' THEN 2
WHEN i.id_23 LIKE '%HIDDEN%' THEN 3
ELSE 0
END AS proxy_risk_flag,

CASE 
WHEN i.devicetype = 'mobile' THEN 1
ELSE 0
END AS is_mobile_device

FROM stg_transactions t
LEFT JOIN stg_identity i
ON t.transactionid = i.transactionid;
DROP TABLE IF EXISTS fraud_features;
CREATE TABLE fraud_features AS
SELECT
t.transactionid,
t.isfraud,
t.transactionamt,
t.productcd,
t.card1,
t.card2,
t.card3,
t.card4,
t.card5,
t.card6,
t.addr1,
t.addr2,
t.dist1,
t.dist2,
t.p_emaildomain,
t.r_emaildomain,
i.devicetype,
i.deviceinfo,
i.id_23,
i.id_30,
i.id_31,
i.id_33,
CASE 
WHEN i.id_23 LIKE '%TRANSPARENT%' THEN 1
WHEN i.id_23 LIKE '%ANONYMOUS%' THEN 2
WHEN i.id_23 LIKE '%HIDDEN%' THEN 3
ELSE 0
END AS proxy_risk_flag,
CASE 
WHEN i.devicetype = 'mobile' THEN 1
ELSE 0
END AS is_mobile_device
FROM stg_transactions t
LEFT JOIN stg_identity i
ON t.transactionid = i.transactionid;

DROP TABLE IF EXISTS fraud_features;

CREATE TABLE fraud_features AS
SELECT * FROM
(
    SELECT
	t.transactionid,
    t.isfraud,
	t.transactionamt,
    t.productcd,
    t.card1,
    t.card2,
    t.card3,
    t.card4,
    t.card5,
    t.card6,
	t.addr1,
    t.addr2,
    t.dist1,
    t.dist2,
	t.p_emaildomain,
    t.r_emaildomain,
	i.devicetype,
    i.deviceinfo,
    i.id_23,
    i.id_30,
    i.id_31,
    i.id_33,
	CASE 
    WHEN i.id_23 LIKE '%TRANSPARENT%' THEN 1
    WHEN i.id_23 LIKE '%ANONYMOUS%' THEN 2
    WHEN i.id_23 LIKE '%HIDDEN%' THEN 3
    ELSE 0
    END AS proxy_risk_flag,
	CASE 
    WHEN i.devicetype = 'mobile' THEN 1
    ELSE 0
    END AS is_mobile_device
	FROM stg_transactions t
    LEFT JOIN stg_identity i
    ON t.transactionid = CAST(i.transactionid AS BIGINT)
) x;
ALTER TABLE fraud_features
ADD COLUMN avg_amt_per_card FLOAT;
UPDATE fraud_features f
SET avg_amt_per_card = sub.avg_amt
FROM
(
    SELECT
    card1,
    AVG(transactionamt) AS avg_amt
    FROM fraud_features
    GROUP BY card1
) sub
WHERE f.card1 = sub.card1;
ALTER TABLE fraud_features
ADD COLUMN amt_deviation FLOAT;
UPDATE fraud_features
SET amt_deviation = transactionamt / avg_amt_per_card;
ALTER TABLE fraud_features
ADD COLUMN device_risk_score INT;
UPDATE fraud_features
SET device_risk_score =
CASE
WHEN is_mobile_device = 1 THEN 2
ELSE 1
END
+ proxy_risk_flag;
SELECT
transactionamt,
avg_amt_per_card,
amt_deviation,
device_risk_score
FROM fraud_features
LIMIT 10;







