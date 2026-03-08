#!/bin/bash
# MariaDB + ETL Milestone User Data Script (Ubuntu)

# REQUIRED: log output for debugging user-data failures
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Exit immediately if a command fails
set -euo pipefail

# Prevents installation prompts which is required for automation
export DEBIAN_FRONTEND=noninteractive

# -----------------------------
# CONFIG (EDIT USERNAME ONLY)
# -----------------------------
USERNAME="cfrierson"     # first initial + last name (Canvas)
UIN="437007345"
DATA_URL="https://622.gomillion.org/data/437007345.zip"

HOME_DIR="/home/${USERNAME}"
ZIP_PATH="${HOME_DIR}/${UIN}.zip"

echo "=== Starting setup ==="
echo "USERNAME=${USERNAME}"
echo "UIN=${UIN}"
echo "DATA_URL=${DATA_URL}"

# -----------------------------
# Update/Upgrade Ubuntu system
# -----------------------------
echo "Updating and upgrading Ubuntu system..."
apt update -y
apt upgrade -y

# Pre answer MariaDB feedback plugin question to avoid install hang
echo "Preconfiguring MariaDB installation..."
apt install -y debconf-utils
debconf-set-selections <<< "mariadb-server mariadb-server/enable_feedback_plugin boolean false"

# Install required dependencies
echo "Installing required dependencies..."
apt install -y curl wget unzip ca-certificates gnupg

# Add official MariaDB Community repository
echo "Adding MariaDB repository from mariadb.org..."
curl -LsS https://r.mariadb.com/downloads/mariadb_repo_setup | bash -s -- --mariadb-server-version="mariadb-11.8"

# Update package list to include MariaDB repo
echo "Updating package list with MariaDB repository..."
apt update -y

# Install MariaDB Server
echo "Installing MariaDB server..."
apt install -y mariadb-server

# Enable MariaDB service to start at boot AND start it now
echo "Starting and enabling MariaDB service..."
systemctl enable --now mariadb

# Confirm MariaDB service is active
echo "Verifying MariaDB service status..."
systemctl is-active --quiet mariadb && echo "MariaDB started successfully"

# Display MariaDB version (proof of successful install)
echo "Installed MariaDB version:"
mariadb --version

# -----------------------------
# Create unprivileged Linux user
# -----------------------------
echo "Creating unprivileged Linux user (if needed)..."
if id "${USERNAME}" >/dev/null 2>&1; then
  echo "Linux user ${USERNAME} already exists."
else
  adduser --disabled-password --gecos "" "${USERNAME}"
  echo "Created Linux user ${USERNAME}."
fi

# -----------------------------
# Ensure LOCAL INFILE works
# -----------------------------
echo "Enabling local_infile..."
cat >/etc/mysql/mariadb.conf.d/99-local-infile.cnf <<EOF
[mariadb]
local_infile=1
EOF
systemctl restart mariadb

# -----------------------------
# Create matching MariaDB user (unix_socket, no password)
# -----------------------------
echo "Creating MariaDB user to match Linux user..."
mariadb <<SQL
CREATE OR REPLACE USER '${USERNAME}'@'localhost' IDENTIFIED VIA unix_socket;
GRANT CREATE, DROP, ALTER, INDEX, INSERT, UPDATE, DELETE, SELECT, REFERENCES
ON *.* TO '${USERNAME}'@'localhost';
FLUSH PRIVILEGES;
SQL

# -----------------------------
# Download + unzip dataset as unprivileged user
# -----------------------------
echo "Downloading and unzipping dataset as ${USERNAME}..."
sudo -u "${USERNAME}" bash -lc "
set -e
cd '${HOME_DIR}'
wget -O '${ZIP_PATH}' '${DATA_URL}'
unzip -o '${ZIP_PATH}' -d '${HOME_DIR}'
echo 'Extracted files:'
ls -l '${HOME_DIR}'
"

# -----------------------------
# Generate FULL etl.sql
# -----------------------------
echo "Writing FULL etl.sql..."
cat > "${HOME_DIR}/etl.sql" <<'EOF'
-- ============================================================
-- ETL Milestone - UIN 437007345
-- Source CSVs in /home/cfrierson:
--   customers.csv   : ID,FN,LN,CT,ST,ZP,S1,S2,EM,BD
--   orders.csv      : OID,CID,Ordered,Shipped
--   orderlines.csv  : OID,PID
--   products.csv    : ID,Name,Price,"Quantity on Hand"
-- ============================================================

DROP DATABASE IF EXISTS POS;
CREATE DATABASE POS;
USE POS;

-- -----------------------------
-- Final tables (required 6)
-- -----------------------------
CREATE TABLE City (
  zip DECIMAL(5) ZEROFILL PRIMARY KEY,
  city VARCHAR(32),
  state VARCHAR(4)
);

CREATE TABLE Customer (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  firstName VARCHAR(32),
  lastName VARCHAR(30),
  email VARCHAR(128),
  address1 VARCHAR(100),
  address2 VARCHAR(50),
  phone VARCHAR(32),
  birthdate DATE,
  zip DECIMAL(5) ZEROFILL,
  CONSTRAINT fk_customer_city
    FOREIGN KEY (zip) REFERENCES City(zip)
);

CREATE TABLE `Order` (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  datePlaced DATE,
  dateShipped DATE,
  customer_id BIGINT UNSIGNED,
  CONSTRAINT fk_order_customer
    FOREIGN KEY (customer_id) REFERENCES Customer(id)
);

CREATE TABLE Product (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(128),
  currentPrice DECIMAL(6,2),
  availableQuantity INT
);

CREATE TABLE Orderline (
  order_id BIGINT UNSIGNED NOT NULL,
  product_id BIGINT UNSIGNED NOT NULL,
  quantity INT,
  PRIMARY KEY (order_id, product_id),
  CONSTRAINT fk_orderline_order
    FOREIGN KEY (order_id) REFERENCES `Order`(id),
  CONSTRAINT fk_orderline_product
    FOREIGN KEY (product_id) REFERENCES Product(id)
);

CREATE TABLE PriceHistory (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  oldPrice DECIMAL(6,2),
  newPrice DECIMAL(6,2),
  ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  product_id BIGINT UNSIGNED,
  CONSTRAINT fk_pricehistory_product
    FOREIGN KEY (product_id) REFERENCES Product(id)
);

-- -----------------------------
-- Staging tables
-- -----------------------------
CREATE TABLE temp_customers (
  ID VARCHAR(50),
  FN VARCHAR(64),
  LN VARCHAR(64),
  CT VARCHAR(64),
  ST VARCHAR(16),
  ZP VARCHAR(16),
  S1 VARCHAR(255),
  S2 VARCHAR(255),
  EM VARCHAR(255),
  BD VARCHAR(64)
);

CREATE TABLE temp_orders (
  OID VARCHAR(50),
  CID VARCHAR(50),
  Ordered VARCHAR(64),
  Shipped VARCHAR(64)
);

CREATE TABLE temp_orderlines (
  OID VARCHAR(50),
  PID VARCHAR(50)
);

CREATE TABLE temp_products (
  ID VARCHAR(50),
  Name VARCHAR(255),
  Price VARCHAR(64),
  QOH VARCHAR(64)
);

-- -----------------------------
-- Load CSVs into staging
-- -----------------------------
LOAD DATA LOCAL INFILE '/home/cfrierson/customers.csv'
INTO TABLE temp_customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/cfrierson/orders.csv'
INTO TABLE temp_orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/cfrierson/orderlines.csv'
INTO TABLE temp_orderlines
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/cfrierson/products.csv'
INTO TABLE temp_products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(ID, Name, Price, QOH);

-- -----------------------------
-- Transform + insert
-- -----------------------------

-- City derived from customers
INSERT INTO City (zip, city, state)
SELECT DISTINCT
  CAST(NULLIF(TRIM(TRAILING '\r' FROM ZP), '') AS DECIMAL(5)) AS zip,
  NULLIF(TRIM(TRAILING '\r' FROM CT), '') AS city,
  NULLIF(TRIM(TRAILING '\r' FROM ST), '') AS state
FROM temp_customers
WHERE NULLIF(TRIM(TRAILING '\r' FROM ZP), '') IS NOT NULL;

-- Customer insert with safe date parsing (no STR_TO_DATE crashes)
INSERT INTO Customer (id, firstName, lastName, email, address1, address2, phone, birthdate, zip)
SELECT
  CAST(NULLIF(TRIM(TRAILING '\r' FROM ID), '') AS UNSIGNED) AS id,
  NULLIF(TRIM(TRAILING '\r' FROM FN), '') AS firstName,
  NULLIF(TRIM(TRAILING '\r' FROM LN), '') AS lastName,
  NULLIF(TRIM(TRAILING '\r' FROM EM), '') AS email,
  NULLIF(TRIM(TRAILING '\r' FROM S1), '') AS address1,
  NULLIF(TRIM(TRAILING '\r' FROM S2), '') AS address2,
  NULL AS phone,

  CASE
    WHEN NULLIF(TRIM(TRAILING '\r' FROM BD), '') IS NULL THEN NULL
    WHEN TRIM(TRAILING '\r' FROM BD) IN ('0000-00-00','00/00/0000') THEN NULL
    WHEN TRIM(TRAILING '\r' FROM BD) REGEXP '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$'
      THEN STR_TO_DATE(TRIM(TRAILING '\r' FROM BD), '%Y-%m-%d')
    WHEN TRIM(TRAILING '\r' FROM BD) REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
      THEN STR_TO_DATE(TRIM(TRAILING '\r' FROM BD), '%m/%d/%Y')
    WHEN TRIM(TRAILING '\r' FROM BD) REGEXP '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$'
      THEN STR_TO_DATE(TRIM(TRAILING '\r' FROM BD), '%m-%d-%Y')
    ELSE NULL
  END AS birthdate,

  CAST(NULLIF(TRIM(TRAILING '\r' FROM ZP), '') AS DECIMAL(5)) AS zip
FROM temp_customers;

-- Product: cleanse price ($ and commas) + quantity
INSERT INTO Product (id, name, currentPrice, availableQuantity)
SELECT
  CAST(NULLIF(TRIM(TRAILING '\r' FROM ID), '') AS UNSIGNED) AS id,
  NULLIF(TRIM(TRAILING '\r' FROM Name), '') AS name,
  CAST(
    NULLIF(
      REPLACE(REPLACE(TRIM(TRAILING '\r' FROM Price), '$', ''), ',', ''),
      ''
    ) AS DECIMAL(6,2)
  ) AS currentPrice,
  CAST(NULLIF(TRIM(TRAILING '\r' FROM QOH), '') AS SIGNED) AS availableQuantity
FROM temp_products;

-- Order: safe parsing for Ordered/Shipped
INSERT INTO `Order` (id, datePlaced, dateShipped, customer_id)
SELECT
  CAST(NULLIF(TRIM(TRAILING '\r' FROM OID), '') AS UNSIGNED) AS id,

  CASE
    WHEN NULLIF(TRIM(TRAILING '\r' FROM Ordered), '') IS NULL THEN NULL
    WHEN TRIM(TRAILING '\r' FROM Ordered) IN ('0000-00-00','00/00/0000') THEN NULL
    WHEN TRIM(TRAILING '\r' FROM Ordered) REGEXP '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$'
      THEN STR_TO_DATE(TRIM(TRAILING '\r' FROM Ordered), '%Y-%m-%d')
    WHEN TRIM(TRAILING '\r' FROM Ordered) REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
      THEN STR_TO_DATE(TRIM(TRAILING '\r' FROM Ordered), '%m/%d/%Y')
    WHEN TRIM(TRAILING '\r' FROM Ordered) REGEXP '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$'
      THEN STR_TO_DATE(TRIM(TRAILING '\r' FROM Ordered), '%m-%d-%Y')
    ELSE NULL
  END AS datePlaced,

  CASE
    WHEN NULLIF(TRIM(TRAILING '\r' FROM Shipped), '') IS NULL THEN NULL
    WHEN TRIM(TRAILING '\r' FROM Shipped) IN ('0000-00-00','00/00/0000') THEN NULL
    WHEN TRIM(TRAILING '\r' FROM Shipped) REGEXP '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$'
      THEN STR_TO_DATE(TRIM(TRAILING '\r' FROM Shipped), '%Y-%m-%d')
    WHEN TRIM(TRAILING '\r' FROM Shipped) REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
      THEN STR_TO_DATE(TRIM(TRAILING '\r' FROM Shipped), '%m/%d/%Y')
    WHEN TRIM(TRAILING '\r' FROM Shipped) REGEXP '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$'
      THEN STR_TO_DATE(TRIM(TRAILING '\r' FROM Shipped), '%m-%d-%Y')
    ELSE NULL
  END AS dateShipped,

  CAST(NULLIF(TRIM(TRAILING '\r' FROM CID), '') AS UNSIGNED) AS customer_id
FROM temp_orders;

-- Orderline: aggregate duplicates into quantity
INSERT INTO Orderline (order_id, product_id, quantity)
SELECT
  CAST(NULLIF(TRIM(TRAILING '\r' FROM OID), '') AS UNSIGNED) AS order_id,
  CAST(NULLIF(TRIM(TRAILING '\r' FROM PID), '') AS UNSIGNED) AS product_id,
  COUNT(*) AS quantity
FROM temp_orderlines
WHERE NULLIF(TRIM(TRAILING '\r' FROM OID), '') IS NOT NULL
  AND NULLIF(TRIM(TRAILING '\r' FROM PID), '') IS NOT NULL
GROUP BY
  CAST(NULLIF(TRIM(TRAILING '\r' FROM OID), '') AS UNSIGNED),
  CAST(NULLIF(TRIM(TRAILING '\r' FROM PID), '') AS UNSIGNED);

-- PriceHistory exists (required), but dataset has no separate history feed.

-- -----------------------------
-- Cleanup: drop staging tables (required)
-- -----------------------------
DROP TABLE temp_orderlines;
DROP TABLE temp_orders;
DROP TABLE temp_products;
DROP TABLE temp_customers;
EOF

chown "${USERNAME}:${USERNAME}" "${HOME_DIR}/etl.sql"

# -----------------------------
# Execute ETL as unprivileged user
# -----------------------------
echo "Executing etl.sql as ${USERNAME}..."
sudo -u "${USERNAME}" bash -lc "
set -e
mariadb < '${HOME_DIR}/etl.sql'
echo 'ETL executed successfully.'
"

echo "=== Finished user-data script ==="
