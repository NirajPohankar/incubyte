CREATE PROCEDURE createCountryTable(IN CountryName VARCHAR(40))
BEGIN
SET @table := 'CountryName';
SET @sqlcreate_text:=CONCAT('CREATE TABLE ',@table,'(customer_Name varchar(255) not null,
Customer_ID bigint(18) auto_increment,
Open_Date date not null,
Last_Consulted_Date date,
Vaccination_Id varchar(5),
Dr_Name varchar(255),
State varchar(5),
Country varchar(5),
DOB date,
Is_Active char(1),
primary key(customerName))');
PREPARE stmt from @sqlcreate_text;
EXECUTE stmt;
END $$