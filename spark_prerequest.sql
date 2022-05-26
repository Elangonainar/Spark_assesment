create database DB1;

use DB1;

Create table users
(
userId int NOT NULL AUTO_INCREMENT,
Hash_column varchar(50),
createdAt datetime,
updatedAt datetime,
city varchar(50),
country varchar(50),
zipcode varchar(50),
email_domain varchar(50),
gender varchar(50),
issmoking varchar(50),
profession varchar(50),
income varchar(50),
PRIMARY KEY (userId)
);

Create table subscription
(
Hash_column varchar(50),
createdAt datetime,
startDate datetime,
endDate datetime,
status varchar(50),
amount varchar(50)
);

Create table message
(
Id int,
createdAt datetime,
senderId int,
receiverId int
);


select * from users;
select * from subscription;
select * from message;
