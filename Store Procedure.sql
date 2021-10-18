CREATE PROCEDURE createCountryTable(IN CountryName VARCHAR(40))
BEGIN
SET @table := 'CountryName';
SET @sqlcreate_text:=CONCAT('CREATE TABLE ',@table,'(customerName varchar(255) not null,
customerID bigint(18) auto_increment,
customerOpenDate date not null,
lastConsultedDate date,
vaccinationType varchar(5),
doctorConsulted varchar(255),
state varchar(5),
country varchar(5),
postCode integer(5),
dateOfBirth date,
activeCustomer char(1),
primary key(customerName))');
PREPARE stmt from @sqlcreate_text;
EXECUTE stmt;
END $$