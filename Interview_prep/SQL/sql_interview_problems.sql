-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/uberorders',True)

-- COMMAND ----------

CREATE or Replace TABLE EdibleItems (
    Category VARCHAR(50),
    Subcategory VARCHAR(50),
    Price DECIMAL(10, 2)
);

INSERT INTO EdibleItems (Category, Subcategory, Price) VALUES
('Fruits', 'Apple', 50.00),
('Fruits', 'Banana', 40.00),
('Fruits', 'Orange', 60.00),
('Fruits', 'Grapes', 120.00),
('Vegetables', 'Carrot', 35.00),
('Vegetables', 'Broccoli', 80.00),
('Vegetables', 'Spinach', 70.00),
('Vegetables', 'Potato', 45.00),
('Dairy', 'Milk', 90.00),
('Dairy', 'Cheese', 150.00),
('Dairy', 'Yogurt', 120.00),
('Dairy', 'Butter', 180.00),
('Grains', 'Rice', 110.00),
('Grains', 'Bread', 150.00),
('Grains', 'Pasta', 130.00),
('Grains', 'Oats', 100.00),
('Meat', 'Chicken', 150.00),
('Seafood', 'Salmon', 200.00),
('Seafood', 'Shrimp', 180.00),
('Seafood', 'Tuna', 170.00),
('Seafood', 'Crab', 190.00),
('Beverages', 'Coffee', 100.00),
('Beverages', 'Tea', 80.00),
('Beverages', 'Juice', 60.00),
('Beverages', 'Soda', 50.00),
('Snacks', 'Chips', 50.00),
('Snacks', 'Cookies', 70.00),
('Snacks', 'Nuts', 90.00),
('Snacks', 'Popcorn', 40.00);

-- COMMAND ----------

select * from EdibleItems

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##For each catoegery find 2 sub cateogies based on its price
-- MAGIC ##If for a catoegry we have only 1 sub cateogry then we have to check price if price > 50.0 thenwe need to include

-- COMMAND ----------

with tbl1 as(
  select 
  Category,
  Subcategory,
  Price,
  dense_rank() over(partition by Category order by Price) as rnk,
  count(Category) over(partition by Category) as cnt
  from edibleitems
),
tbl2 as(
select Category,Subcategory from tbl1 where (cnt >2 and rnk in (1,2)) or (cnt=1 and price >50))
select * from tbl2

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Find the avgergae price of fruits from the EdibleItems 

-- COMMAND ----------

select Category,avg(Price) from EdibleItems
group by Category
having Category="Vegetables"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Maximum and Minimum Prices from each Category

-- COMMAND ----------

with tbl1 as (
  select Category,max(Price) as Maximun_price,min(price) as Minimum_price 
  from EdibleItems
  group by Category
)select * from tbl1

-- COMMAND ----------

CREATE TABLE Employees (
    SNo INT,
    Name VARCHAR(100),
    DateOfBirth DATE,
    HireDate DATE,
    DateOfRelieving DATE
);

INSERT INTO Employees (SNo, Name, DateOfBirth, HireDate, DateOfRelieving) VALUES
(1, 'John Doe', '1985-05-15', '2010-06-01', '2020-05-31'),
(2, 'Jane Smith', '1990-07-20', '2012-08-15', '2022-07-14'),
(3, 'Emily Johnson', '1988-03-10', '2015-01-10', '2023-01-09'),
(4, 'Michael Brown', '1975-11-25', '2005-09-05', '2015-09-04'),
(5, 'Sarah Davis', '1992-02-18', '2018-03-01', '2023-02-28'),
(6, 'David Wilson', '1983-12-12', '2008-07-01', '2018-06-30'),
(7, 'Laura Martinez', '1987-09-09', '2011-04-15', '2021-04-14'),
(8, 'James Anderson', '1991-01-22', '2013-11-01', '2023-10-31'),
(9, 'Patricia Thomas', '1984-06-30', '2009-02-20', '2019-02-19'),
(10, 'Robert Jackson', '1978-08-25', '2003-05-10', '2013-05-09'),
(11, 'Linda White', '1986-03-15', '2011-01-05', '2021-01-04'),
(12, 'Christopher Harris', '1993-07-07', '2016-09-01', '2026-08-31'),
(13, 'Barbara Clark', '1982-11-11', '2007-03-20', '2017-03-19'),
(14, 'Daniel Lewis', '1989-04-04', '2014-06-15', '2024-06-14'),
(15, 'Elizabeth Walker', '1995-10-10', '2018-12-01', '2028-11-30'),
(16, 'Matthew Hall', '1981-02-02', '2006-08-10', '2016-08-09'),
(17, 'Jennifer Allen', '1994-05-05', '2017-10-01', '2027-09-30'),
(18, 'Anthony Young', '1980-03-03', '2005-11-20', '2015-11-19'),
(19, 'Susan King', '1988-12-12', '2013-07-01', '2023-06-30'),
(20, 'Charles Wright', '1979-06-06', '2004-04-15', '2014-04-14');

-- COMMAND ----------

select DATEDIFF(YEAR,DateOfBirth,HireDate) as date_when_joined from Employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##How to get distinct values of rows with out using distinct keyword

-- COMMAND ----------

select * from (select *,row_number() over(partition by SNo,Name,DateOfBirth,HireDate,DateOfRelieving order by SNo DESC) as rnk from Employees)n
where rnk=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##First letter from the Name of the employees in Vowels

-- COMMAND ----------

select * from Employees where lower(LEFT(Name,1)) IN ('a','e','i','o','u','s')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Seller table creation

-- COMMAND ----------

CREATE TABLE Sellers (
    SELLER_ID INT,
    COUNTRY VARCHAR(50),
    JOINING_DATE DATE
);

INSERT INTO Sellers (SELLER_ID, COUNTRY, JOINING_DATE) VALUES
(1, 'USA', '2020-01-15'),
(2, 'Canada', '2019-03-22'),
(3, 'UK', '2021-07-10'),
(4, 'Australia', '2018-11-05'),
(5, 'India', '2022-02-28'),
(6, 'Germany', '2020-06-17'),
(7, 'France', '2019-09-30'),
(8, 'Japan', '2021-12-01'),
(9, 'Brazil', '2018-04-25'),
(10, 'South Africa', '2022-08-14'),
(11, 'Italy', '2020-10-20'),
(12, 'Spain', '2019-05-15'),
(13, 'Mexico', '2021-03-18'),
(14, 'China', '2018-07-22'),
(15, 'Russia', '2022-01-10'),
(16, 'Netherlands', '2020-09-05'),
(17, 'Sweden', '2019-12-11'),
(18, 'Norway', '2021-06-30'),
(19, 'South Korea', '2018-02-14'),
(20, 'New Zealand', '2022-04-19');

INSERT INTO Sellers (SELLER_ID, COUNTRY, JOINING_DATE) VALUES
(21, 'USA', '2020-01-15'),
(22, 'Canada', '2019-03-22'),
(23, 'South', '2021-07-10'),
(24, 'China', '2018-11-05'),
(25, 'India', '2022-02-28'),
(26, 'USA', '2020-06-17'),
(27, 'South', '2019-09-30'),
(28, 'Japan', '2021-12-01'),
(29, 'Brazil', '2018-04-25'),
(30, 'South Africa', '2022-08-14'),
(31, 'Italy', '2020-10-20'),
(32, 'Brazil', '2019-05-15'),
(33, 'Mexico', '2021-03-18'),
(34, 'China', '2018-07-22'),
(35, 'South', '2022-01-10'),
(36, 'India', '2020-09-05'),
(29, 'India', '2020-01-15');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Given a table SELLERS with 3 columns SELLER_ID, COUNTRY, and JOINING_DATE, write a query to identify a number of sellers per country and order it in descending order of no. of sellers

-- COMMAND ----------

select COUNTRY,count(*) as cnt from Sellers
group by COUNTRY
order by cnt desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Sellers who joined on Monday

-- COMMAND ----------

select *  from Sellers where day(date_format(JOINING_DATE,'DD-MM-yyyy')) ="Tue"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Extrcating alternate rows from a table
-- MAGIC

-- COMMAND ----------

select * from Sellers where MOD(SELLER_ID,2)=0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Creation of workers table with salary column

-- COMMAND ----------

CREATE TABLE Workers (
    id INT,
    name VARCHAR(100),
    salary DECIMAL(10, 2),
    location VARCHAR(100),
    department VARCHAR(50),
    hire_date DATE,
    email VARCHAR(100)
);
INSERT INTO Workers (id, name, salary, location, department, hire_date, email) VALUES
(1, 'Alice Johnson', 75000.00, 'New York', 'Engineering', '2020-01-15', 'alice.johnson@example.com'),
(2, 'Bob Smith', 65000.00, 'San Francisco', 'Marketing', '2019-03-22', 'bob.smith@example.com'),
(3, 'Charlie Brown', 70000.00, 'Chicago', 'Sales', '2021-07-10', 'charlie.brown@example.com'),
(4, 'David Wilson', 80000.00, 'Los Angeles', 'HR', '2018-11-05', 'david.wilson@example.com'),
(5, 'Eve Davis', 72000.00, 'Seattle', 'Finance', '2022-02-28', 'eve.davis@example.com'),
(6, 'Frank Miller', 68000.00, 'Boston', 'Engineering', '2019-06-17', 'frank.miller@example.com'),
(7, 'Grace Lee', 71000.00, 'Austin', 'Marketing', '2020-09-30', 'grace.lee@example.com'),
(8, 'Hannah Taylor', 69000.00, 'Denver', 'Sales', '2021-12-01', 'hannah.taylor@example.com'),
(9, 'Ian Moore', 73000.00, 'Miami', 'HR', '2018-04-25', 'ian.moore@example.com'),
(10, 'Jack White', 76000.00, 'Dallas', 'Finance', '2022-08-14', 'jack.white@example.com'),
(11, 'Karen Harris', 67000.00, 'Phoenix', 'Engineering', '2020-10-20', 'karen.harris@example.com'),
(12, 'Leo Clark', 64000.00, 'San Diego', 'Marketing', '2019-05-15', 'leo.clark@example.com'),
(13, 'Mia Lewis', 78000.00, 'Houston', 'Sales', '2021-03-18', 'mia.lewis@example.com'),
(14, 'Noah Walker', 74000.00, 'Philadelphia', 'HR', '2018-07-22', 'noah.walker@example.com'),
(15, 'Olivia Hall', 77000.00, 'San Jose', 'Finance', '2022-01-10', 'olivia.hall@example.com'),
(16, 'Paul Allen', 66000.00, 'Columbus', 'Engineering', '2020-09-05', 'paul.allen@example.com'),
(17, 'Quinn Young', 70000.00, 'Charlotte', 'Marketing', '2019-12-11', 'quinn.young@example.com'),
(18, 'Rachel King', 75000.00, 'Indianapolis', 'Sales', '2021-06-30', 'rachel.king@example.com'),
(19, 'Sam Wright', 72000.00, 'San Francisco', 'HR', '2018-02-14', 'sam.wright@example.com'),
(20, 'Tina Scott', 69000.00, 'Fort Worth', 'Finance', '2022-04-19', 'tina.scott@example.com'),
(21, 'Uma Green', 68000.00, 'Jacksonville', 'Engineering', '2021-03-15', 'uma.green@example.com'),
(22, 'Victor Adams', 71000.00, 'San Antonio', 'Marketing', '2020-07-22', 'victor.adams@example.com'),
(23, 'Wendy Baker', 73000.00, 'San Diego', 'Sales', '2019-11-10', 'wendy.baker@example.com'),
(24, 'Xander Nelson', 76000.00, 'San Jose', 'HR', '2021-05-05', 'xander.nelson@example.com'),
(25, 'Yara Carter', 74000.00, 'Austin', 'Finance', '2020-12-28', 'yara.carter@example.com'),
(26, 'Zane Mitchell', 67000.00, 'Dallas', 'Engineering', '2019-08-17', 'zane.mitchell@example.com'),
(27, 'Abby Perez', 78000.00, 'Phoenix', 'Marketing', '2021-01-30', 'abby.perez@example.com'),
(28, 'Ben Roberts', 75000.00, 'Houston', 'Sales', '2020-04-01', 'ben.roberts@example.com'),
(29, 'Cara Turner', 72000.00, 'Philadelphia', 'HR', '2019-10-25', 'cara.turner@example.com'),
(30, 'Danielle Edwards', 69000.00, 'San Antonio', 'Finance', '2021-09-14', 'danielle.edwards@example.com'),
(31, 'Ethan Collins', 68000.00, 'Columbus', 'Engineering', '2019-06-20', 'ethan.collins@example.com'),
(32, 'Fiona Stewart', 71000.00, 'Charlotte', 'Marketing', '2020-03-15', 'fiona.stewart@example.com'),
(33, 'George Morris', 73000.00, 'Indianapolis', 'Sales', '2018-12-18', 'george.morris@example.com'),
(34, 'Holly Rogers', 76000.00, 'Fort Worth', 'HR', '2021-07-22', 'holly.rogers@example.com'),
(35, 'Isaac Reed', 74000.00, 'Jacksonville', 'Finance', '2020-02-10', 'isaac.reed@example.com'),
(36, 'Jasmine Cook', 67000.00, 'San Francisco', 'Engineering', '2019-11-05', 'jasmine.cook@example.com'),
(37, 'Kyle Bell', 78000.00, 'San Diego', 'Marketing', '2020-08-11', 'kyle.bell@example.com'),
(38, 'Lily Murphy', 75000.00, 'San Jose', 'Sales', '2019-04-30', 'lily.murphy@example.com'),
(39, 'Mason Bailey', 72000.00, 'Austin', 'HR', '2021-02-14', 'mason.bailey@example.com'),
(40, 'Nina Rivera', 69000.00, 'Dallas', 'Finance', '2020-10-19', 'nina.rivera@example.com');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Finding 3rd highest salary from workers table

-- COMMAND ----------

with tbl1 as (
  select *,
  dense_rank() over(order by salary DESC) as rnk
  from Workers
)
select * from tbl1 where rnk=3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Top 25% records with high slaries

-- COMMAND ----------

with tbl1 as (
  select *,
  count(*) over() as cnt,
  row_number() over(order by salary DESC) as rnk 
  from Workers
)
select * from tbl1 where rnk <=ceil(cnt*0.25)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##salaries of worrkers greater that the average salaries of their departement

-- COMMAND ----------

with tbl1 as (
  select *,
  avg(salary) over(partition by department ) as avergae_salary_of_dept from Workers
),
tbl2 as (
  select id,name from tbl1 where salary > avergae_salary_of_dept
)
select * from tbl2 order by id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Hghest slaried person from each department

-- COMMAND ----------

with tbl1 as (
  select * ,
  dense_rank() over (partition by department order by salary) as rnk
  from Workers
)
  select id ,name from tbl1 where rnk=1 order by id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##CREATING FLIGHT DATA

-- COMMAND ----------

CREATE TABLE Flights (
    P_ID INT ,
    Airline VARCHAR(50),
    FlightNumber VARCHAR(10),
    Source VARCHAR(50),
    Destination VARCHAR(50),
    DepartureTime DATE,
    ArrivalTime DATE
);

INSERT INTO Flights (P_ID, Airline, FlightNumber, Source, Destination, DepartureTime, ArrivalTime)
VALUES 
(1, 'Air India', 'AI101', 'Delhi', 'Mumbai', '2024-12-16 10:00:00', '2024-12-16 12:00:00'),
(1, 'Air India', 'AI102', 'Mumbai', 'Hyderabad', '2024-12-16 14:00:00', '2024-12-16 16:00:00'),
(3, 'IndiGo', '6E203', 'Bengaluru', 'Chennai', '2024-12-16 14:00:00', '2024-12-16 15:30:00'),
(3, 'IndiGo', '6E204', 'Chennai', 'Kerla', '2024-12-16 17:00:00', '2024-12-16 18:30:00'),
(4, 'SpiceJet', 'SG401', 'Kolkata', 'Hyderabad', '2024-12-16 09:00:00', '2024-12-16 11:30:00'),
(4, 'SpiceJet', 'SG402', 'Hyderabad', 'Delhi', '2024-12-16 13:00:00', '2024-12-16 15:30:00');

-- COMMAND ----------

select * from Flights

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##FIND SOURCE AND DESTION

-- COMMAND ----------

with tbl1 as (
  select a.P_ID,a.Source,a.Destination,b.P_ID as b_P_ID,b.Source as b_Source,b.Destination as b_Destination
  from Flights a 
  inner join Flights b on a.P_ID=b.P_ID
  where a.Destination=b.Source
),
tbl2 as (
  select P_ID,Source,b_Destination from tbl1 
)
select * from tbl2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##WORKERS TABLE

-- COMMAND ----------

select * from Workers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##3rd Highest Salary

-- COMMAND ----------

with tbl1 as (
  select 
  id,
  name,
  dense_rank() over (order by salary) as rnk from Workers 
)select * from tbl1 where rnk=3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Last 3 Worker ID's

-- COMMAND ----------

with tbl1 as (
  select *, 
  count(*) over() as cnt,
  row_number() over(order by id) as rn from Workers
)select * from tbl1 where rn in (cnt-1,cnt-2,cnt) order by cnt DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Uber_Data

-- COMMAND ----------

select * from uber

-- COMMAND ----------

CREATE TABLE uber (
    OrderID INT,
    ProviderID INT,
    OrderDate DATE,
    DeliveryDate DATE
);

INSERT INTO uber (OrderID, ProviderID, OrderDate, DeliveryDate)
VALUES 
(1,101, '2024-12-01', '2024-12-02'),
(2,102, '2024-12-02', '2024-12-09'),
(3,103, '2024-12-03', '2024-12-04'),
(4,104, '2024-12-04', '2024-12-05'),
(5,105, '2024-12-05', '2024-12-06'),
(6,106, '2024-12-06', '2024-12-16'),
(7,107, '2024-12-07', '2024-12-09'),
(8,108, '2024-12-08', '2024-12-15'),
(9,109, '2024-12-09', '2024-12-10'),
(10, 110, '2024-12-10', '2024-12-21'),
(11, 111, '2024-12-11', '2024-12-12'),
(12, 112, '2024-12-12', '2024-12-13'),
(13, 113, '2024-12-13', '2024-12-14'),
(14, 114, '2024-12-14', '2024-12-15'),
(15, 115, '2024-12-15', '2024-12-16'),
(16, 116, '2024-12-16', '2024-12-17'),
(17, 117, '2024-12-17', '2024-12-18'),
(18, 118, '2024-12-18', '2024-12-19'),
(19, 119, '2024-12-19', '2024-12-20'),
(20, 120, '2024-12-20', '2024-12-30'),
(21, 121, '2024-12-21', '2024-12-22'),
(22, 122, '2024-12-22', '2024-12-23'),
(23, 123, '2024-12-23', '2024-12-30'),
(24, 124, '2024-12-24', '2024-12-25'),
(25, 125, '2024-12-25', '2024-12-26'),
(26, 126, '2024-12-26', '2024-12-27'),
(27, 127, '2024-12-27', '2024-12-28'),
(28, 128, '2024-12-28', '2024-12-29'),
(29, 129, '2024-12-29', '2024-12-30'),
(30, 130, '2024-12-30', '2024-12-31'),
(31, 131, '2024-12-31', '2025-01-01'),
(32, 132, '2025-01-01', '2025-01-02'),
(33, 133, '2025-01-02', '2025-01-03'),
(34, 134, '2025-01-03', '2025-01-20'),
(35, 135, '2025-01-04', '2025-01-05'),
(36, 136, '2025-01-05', '2025-01-06'),
(37, 137, '2025-01-06', '2025-01-07'),
(38, 138, '2025-01-07', '2025-01-08'),
(39, 139, '2025-01-08', '2025-01-09'),
(40, 140, '2025-01-09', '2025-01-15');

-- COMMAND ----------

WITH tbl1 AS (
  SELECT 
    OrderID,
    ProviderID,
    DATEDIFF(day, OrderDate, DeliveryDate) AS day_di
  FROM Uber
),
tbl2 AS (
  Select 
  *,
  case 
  when day_di > 3 then 0 else 1 end as dd
  from tbl1
),
tbl3 as (
  select 
  sum(dd) as total_sum,
  count(*) as cnt
  FROM tbl2
)select (total_sum/cnt)*100 as percentage_of_uber_orders_done from tbl3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Amazon Data

-- COMMAND ----------

CREATE TABLE AmazonProducts (
    ProductID INT,
    ProductName VARCHAR(255),
    Category VARCHAR(100),
    Price DECIMAL(10, 2),
    Stock INT,
    PriceDate DATE
);
INSERT INTO AmazonProducts (ProductID, ProductName, Category, Price, Stock, PriceDate)
VALUES 
(1, 'Echo Dot (4th Gen)', 'Electronics', 49.99, 100, '2024-11-01'),
(1, 'Echo Dot (4th Gen)', 'Electronics', 44.99, 100, '2024-11-29'), -- Black Friday price
(1, 'Echo Dot (4th Gen)', 'Electronics', 47.99, 100, '2024-12-15'),
(2, 'Fire TV Stick 4K', 'Electronics', 39.99, 200, '2024-11-02'),
(2, 'Fire TV Stick 4K', 'Electronics', 34.99, 200, '2024-11-29'), -- Black Friday price
(2, 'Fire TV Stick 4K', 'Electronics', 37.99, 200, '2024-12-15'),
(3, 'Kindle Paperwhite', 'Books', 129.99, 150, '2024-11-03'),
(3, 'Kindle Paperwhite', 'Books', 119.99, 150, '2024-11-29'), -- Black Friday price
(3, 'Kindle Paperwhite', 'Books', 124.99, 150, '2024-12-15'),
(4, 'Instant Pot Duo', 'Home & Kitchen', 89.99, 50, '2024-11-04'),
(4, 'Instant Pot Duo', 'Home & Kitchen', 79.99, 50, '2024-11-29'), -- Black Friday price
(4, 'Instant Pot Duo', 'Home & Kitchen', 84.99, 50, '2024-12-15'),
(5, 'Apple AirPods Pro', 'Electronics', 249.99, 75, '2024-11-05'),
(5, 'Apple AirPods Pro', 'Electronics', 199.99, 75, '2024-11-29'), -- Black Friday price
(5, 'Apple AirPods Pro', 'Electronics', 229.99, 75, '2024-12-15'),
(6, 'Samsung Galaxy S21', 'Mobile Phones', 799.99, 30, '2024-11-06'),
(6, 'Samsung Galaxy S21', 'Mobile Phones', 749.99, 30, '2024-11-29'), -- Black Friday price
(6, 'Samsung Galaxy S21', 'Mobile Phones', 779.99, 30, '2024-12-15'),
(7, 'Sony WH-1000XM4', 'Electronics', 349.99, 60, '2024-11-07'),
(7, 'Sony WH-1000XM4', 'Electronics', 299.99, 60, '2024-11-29'), -- Black Friday price
(7, 'Sony WH-1000XM4', 'Electronics', 329.99, 60, '2024-12-15'),
(8, 'Dyson V11 Vacuum', 'Home & Kitchen', 599.99, 20, '2024-11-08'),
(8, 'Dyson V11 Vacuum', 'Home & Kitchen', 549.99, 20, '2024-11-29'), -- Black Friday price
(8, 'Dyson V11 Vacuum', 'Home & Kitchen', 579.99, 20, '2024-12-15'),
(9, 'Fitbit Charge 4', 'Wearable Technology', 149.99, 120, '2024-11-09'),
(9, 'Fitbit Charge 4', 'Wearable Technology', 129.99, 120, '2024-11-29'), -- Black Friday price
(9, 'Fitbit Charge 4', 'Wearable Technology', 139.99, 120, '2024-12-15'),
(10, 'Nespresso VertuoPlus', 'Home & Kitchen', 179.99, 40, '2024-11-10'),
(10, 'Nespresso VertuoPlus', 'Home & Kitchen', 159.99, 40, '2024-11-29'), -- Black Friday price
(10, 'Nespresso VertuoPlus', 'Home & Kitchen', 169.99, 40, '2024-12-15');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Find the percentage of reduction of price in Amazon black froday sale
-- MAGIC

-- COMMAND ----------

with tbl1 as(select ProductID,ProductName,
max(case 
when PriceDate='2024-11-29'THEN Price end )as BlackFriday_sales,
max(case
when 
PriceDate <>'2024-11-29' THEN Price end) as Normal_days_price
 from AmazonProducts
group by ProductID,ProductName
order by ProductID),
tbl2 as (
  select ProductID,ProductName,100-((BlackFriday_sales/Normal_days_price)*100) as percenge_of_reduction from tbl1
)select * from tbl2
