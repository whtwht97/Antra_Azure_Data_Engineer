
USE WideWorldImporters;

/*1.List of Persons¡¯ full name, all their fax and phone numbers, as well as the phone number and fax of the company they are working for (if any)*/
SELECT 
	p.FullName, 
	p.FaxNumber,
	p.PhoneNumber,
	c.PhoneNumber AS CompanyPhoneNumber,
	c.FaxNumber AS CompanyFaxNumber
FROM 
	Application.People AS p
LEFT JOIN
	Sales.Customers AS c 
ON 
	p.PersonId = c.PrimaryContactPersonID OR p.PersonID = c.AlternateContactPersonID;

/*2.If the customer's primary contact person has the same phone number as the customer¡¯s phone number, list the customer companies*/
SELECT 
	c.CustomerName
FROM
	Application.People AS p
INNER JOIN
	Sales.Customers AS c
ON 
	p.PersonID = c.PrimaryContactPersonID
WHERE 
	p.PhoneNumber = c.PhoneNumber;
	
/*3.List of customers to whom we made a sale prior to 2016 but no sale since 2016-01-01.*/
SELECT 
	o.CustomerID
FROM  
	Sales.Orders AS o
WHERE 
	o.OrderDate < '2016-01-01' AND o.CustomerID NOT IN (
	SELECT 
		DISTINCT CustomerID
	FROM 
		Sales.Orders
	WHERE 
		Sales.Orders.OrderDate >= '2016-01-01');

/*4.List of Stock Items and total quantity for each stock item in Purchase Orders in Year 2013.*/
SELECT 
     s.StockItemName,
	 COUNT(s.StockItemName) AS quantity
FROM 
	Purchasing.PurchaseOrders AS p
LEFT JOIN 
	Purchasing.PurchaseOrderLines AS pol
ON 
	pol.PurchaseOrderID = p.PurchaseOrderID 
LEFT JOIN 
	Warehouse.StockItems_Archive AS s
ON 
	pol.StockItemID = s.StockItemID
WHERE 
	YEAR(P.OrderDate) ='2013'
GROUP BY 
	s.StockItemName;
	
/*5.List of stock items that have at least 10 characters in description*/
SELECT 
	s.StockItemName 
FROM 
	Warehouse.StockItems AS s
WHERE 
	LEN(s.MarketingComments)>10;

/*6.List of stock items that are not sold to the state of Alabama and Georgia in 2014*/
SELECT DISTINCT si.StockItemName FROM Warehouse.StockItems AS si
LEFT JOIN Sales.OrderLines AS ol 
ON OL.StockItemID = si.StockItemID
LEFT JOIN Sales.Orders AS o 
ON o.OrderID = ol.OrderID
LEFT JOIN Sales.Customers AS c 
ON c.CustomerID = o.CustomerID
LEFT JOIN Application.Cities 
ON Cities.CityID = c.DeliveryCityID
LEFT JOIN Application.StateProvinces AS sp 
ON Cities.StateProvinceID = sp.StateProvinceID
WHERE sp.StateProvinceName NOT IN ('Alabama', 'Georgia') AND YEAR(o.OrderDate) = 2014;

/*7.List of States and Avg dates for processing (confirmed delivery date ¨C order date)*/
SELECT
	sp.StateProvinceName,
	AVG(DATEDIFF(day, o.OrderDate, CONVERT(DATE,i.ConfirmedDeliveryTime))) AS AvgProcessDays 
FROM Sales.Invoices AS i
LEFT JOIN Sales.Orders AS o 
ON i.OrderID = o.OrderID
LEFT JOIN Sales.Customers AS c
ON c.CustomerID = o.CustomerID
LEFT JOIN Application.Cities
ON Cities.CityID = c.DeliveryCityID
LEFT JOIN Application.StateProvinces AS sp
ON Cities.StateProvinceID = sp.StateProvinceID
GROUP BY sp.StateProvinceName
ORDER BY sp.StateProvinceName;

/*8.List of States and Avg dates for processing (confirmed delivery date ¨C order date) by month*/
SELECT
	sp.StateProvinceName,
	MONTH(o.OrderDate) AS month,
	AVG(DATEDIFF(day, o.OrderDate, CONVERT(DATE,i.ConfirmedDeliveryTime))) AS AvgProcessDays 
FROM Sales.Invoices AS i
LEFT JOIN Sales.Orders AS o 
ON i.OrderID = o.OrderID
LEFT JOIN Sales.Customers AS c
ON c.CustomerID = o.CustomerID
LEFT JOIN Application.Cities
ON Cities.CityID = c.DeliveryCityID
LEFT JOIN Application.StateProvinces AS sp
ON Cities.StateProvinceID = sp.StateProvinceID
GROUP BY sp.StateProvinceName, month(o.OrderDate)
ORDER BY sp.StateProvinceName, month(o.OrderDate);

/*9.List of StockItems that the company purchased more than sold in the year of 2015.*/
WITH sold AS (
	SELECT 
		si.StockItemID,
		SUM(ol.Quantity) AS QuantitySold
	FROM
		Warehouse.StockItems AS si
	LEFT JOIN Sales.OrderLines AS ol 
	ON ol.StockItemID = si.StockItemID
	LEFT JOIN Sales.Orders AS o ON o.OrderID = ol.OrderID 
	WHERE YEAR(o.OrderDate) = 2015
	GROUP BY si.StockItemID
),

purchases AS (
	SELECT
		si.StockItemName,
		si.StockItemID,
		SUM(pol.ReceivedOuters*si.QuantityPerOuter) as QuantityPurchased
	FROM Warehouse.StockItems AS si 
	LEFT JOIN Purchasing.PurchaseOrderLines pol 
	ON si.StockItemID = pol.StockItemID
	LEFT JOIN Purchasing.PurchaseOrders AS po 
	ON po.PurchaseOrderID = pol.PurchaseOrderID 
	WHERE YEAR(po.OrderDate) = 2015
	GROUP BY si.StockItemID, si.StockItemName
)

SELECT 
	p.StockItemID, p.StockItemName, p.QuantityPurchased, s.QuantitySold
FROM 
	purchases AS p 
LEFT JOIN sold AS s 
ON s.StockItemID = p.StockItemID
WHERE QuantityPurchased > QuantitySold;

/*10.List of Customers and their phone number, together with the primary contact person¡¯s name, to whom we did not sell more than 10  mugs (search by name) in the year 2016*/
WITH CTE AS (
	SELECT
		sub1.CustomerID
	FROM (
		SELECT 
			o.CustomerID,
			o.OrderID,
			SUM(ol.PickedQuantity) as sold_num
		FROM Sales.Orders AS o
		INNER JOIN Sales.OrderLines AS ol
		ON o.OrderID = ol.OrderID AND YEAR(o.OrderDate) = 2016
		INNER JOIN Warehouse.StockItems AS s
		ON ol.StockItemID = s.StockItemID 
		WHERE s.StockItemName LIKE '%mug%'
		GROUP BY o.CustomerID, o.OrderID
		) AS sub1 
	GROUP BY sub1.CustomerID
	HAVING SUM(sub1.sold_num) <= 10
)

SELECT 
	C.CustomerName,
	C.PhoneNumber,
	P.FullName AS PrimaryContactName
FROM CTE
LEFT JOIN Sales.Customers AS c
ON CTE.CustomerID = c.CustomerID
LEFT JOIN Application.People AS p
ON p.PersonID = c.PrimaryContactPersonID;

/*11.List all the cities that were updated after 2015-01-01*/
SELECT CityName FROM Application.Cities WHERE ValidFrom BETWEEN '2015-01-01' AND GETDATE();

/*12.List all the Order Detail (Stock Item name, delivery address, delivery state, city, country, customer name, customer contact person name, customer phone, quantity) for the date of 2014-07-01. Info should be relevant to that date.*/
SELECT 
	si.StockItemName,
	c.DeliveryAddressLine1,
	c.DeliveryAddressLine2,
	sp.StateProvinceName,
	Cities.CityName,
	Countries.CountryName,
	c.CustomerName,
	c.PhoneNumber AS CustomerPhone,
	p.FullName AS ContactPersonName,
	ol.Quantity
FROM Sales.OrderLines AS ol 
LEFT JOIN Sales.Orders AS o 
ON ol.OrderID = o.OrderID
LEFT JOIN Sales.Customers AS c
ON c.CustomerID = o.CustomerID
LEFT JOIN Warehouse.StockItems AS si
ON si.StockItemID = ol.StockItemID
LEFT JOIN Application.Cities
ON Cities.CityID = c.DeliveryCityID
LEFT JOIN Application.StateProvinces AS sp
ON Cities.StateProvinceID = sp.StateProvinceID
LEFT JOIN Application.Countries
ON Countries.CountryID = sp.CountryID
LEFT JOIN Application.People AS p ON p.PersonID = c.PrimaryContactPersonID
WHERE o.OrderDate = '2014-07-01';

/*13.List of stock item groups and total quantity purchased, total quantity sold, and the remaining stock quantity (quantity purchased ¨C quantity sold)*/
WITH Purchased AS(
	SELECT 
		sg.StockGroupName,
		SUM(pol.ReceivedOuters) AS QuantityPurchased
	FROM Purchasing.PurchaseOrderLines AS pol
	LEFT JOIN Warehouse.StockItemStockGroups AS sisg
	ON pol.StockItemID = sisg.StockItemID
	LEFT JOIN Warehouse.StockGroups AS SG
	ON sisg.StockGroupID = sg.StockGroupID
	GROUP BY sg.StockGroupName
),

Sold AS(
	SELECT 
		sg.StockGroupName,
		SUM(ol.PickedQuantity) AS QuantityOrdered
	FROM Sales.OrderLines AS ol
	LEFT JOIN Warehouse.StockItemStockGroups AS sisg
	ON ol.StockItemID = sisg.StockItemID
	LEFT JOIN Warehouse.StockGroups AS sg
	ON sisg.StockGroupID = sg.StockGroupID
	GROUP BY sg.StockGroupName)

SELECT 
	Purchased.StockGroupName,
	Purchased.QuantityPurchased,
	Sold.QuantityOrdered,
	Purchased.QuantityPurchased-Sold.QuantityOrdered AS QuantityRemaining
FROM Purchased LEFT JOIN Sold
ON Purchased.StockGroupName = Sold.StockGroupName

/*14.List of Cities in the US and the stock item that the city got the most deliveries in 2016. If the city did not purchase any stock items in 2016, print ¡°No Sales¡±.*/
WITH CTE1 AS (
	SELECT ol.StockItemID, c.DeliveryCityID, COUNT(*) AS Delivery
	FROM Sales.OrderLines AS ol
		JOIN Sales.Orders AS o ON o.OrderID = ol.OrderID
		JOIN sales.Customers AS c ON o.CustomerID = c.CustomerID
	WHERE YEAR(o.OrderDate) = 2016
	GROUP BY ol.StockItemID, c.DeliveryCityID),

CTE2 AS(
	SELECT StockItemID, DeliveryCityID
	FROM ( 
		SELECT StockItemID, DeliveryCityID, 
			DENSE_RANK() OVER(PARTITION BY DeliveryCityId ORDER BY Delivery DESC) AS rnk
		FROM CTE1) AS sub
	WHERE rnk = 1
)

SELECT
	Cities.CityName,
	ISNULL(s.StockItemName, 'No Sale') AS MostDeliveryItems
FROM CTE2 JOIN Warehouse.StockItems s 
ON CTE2.StockItemID = s.StockItemID
RIGHT JOIN Application.Cities  
ON CTE2.DeliveryCityID = Cities.CityID


/*15.List any orders that had more than one delivery attempt (located in invoice table)*/
SELECT
	OrderID
FROM Sales.Invoices
WHERE JSON_VALUE(ReturnedDeliveryData, '$.Events[1].Comment') IS NOT NULL

/*16.List all stock items that are manufactured in China. (Country of Manufacture)*/
SELECT
	DISTINCT StockItemName
FROM Warehouse.StockItems 
WHERE JSON_VALUE(CustomFields,'$.CountryOfManufacture') = 'China'

/*17.Total quantity of stock items sold in 2015, group by country of manufacturing*/
SELECT 
	JSON_VALUE(CustomFields, '$.CountryOfManufacture') AS CountryOfManufacture,
	SUM(ol.Quantity) AS TotalQuantityStockSold
FROM Sales.Orders AS o
LEFT join Sales.OrderLines AS ol
ON o.OrderID = ol.OrderID
LEFT JOIN Warehouse.StockItems AS s
ON ol.StockItemID = s.StockItemID
WHERE YEAR(o.OrderDate) = 2015
GROUP BY JSON_VALUE(CustomFields, '$.CountryOfManufacture');

/*18.Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. [Stock Group Name, 2013, 2014, 2015, 2016, 2017]*/
CREATE VIEW StockQuanlityByYear AS
	SELECT StockGroupName,[2013],[2014],[2015],[2016],[2017] FROM(
		SELECT
			sg.StockGroupName, 
			YEAR(o.OrderDate) AS years, 
			SUM(ol.Quantity) AS total_quantity
		FROM Sales.OrderLines AS ol 
		LEFT JOIN sales.Orders AS o 
		ON ol.OrderID = o.OrderID
		LEFT JOIN Warehouse.StockItems AS si 
		ON ol.StockItemID = si.StockItemID
		LEFT JOIN Warehouse.StockItemStockGroups AS ssg
		ON ssg.StockItemID = ol.StockItemID
		LEFT JOIN Warehouse.StockGroups AS sg 
		ON sg.StockGroupID = ssg.StockGroupID
		WHERE YEAR(o.OrderDate) IN (2013, 2014, 2015, 2016, 2017)
		GROUP BY sg.StockGroupName, YEAR(o.OrderDate)
	) AS p
	PIVOT (
	SUM(p.total_quantity) 
	FOR years IN ([2013],[2014],[2015],[2016],[2017]) 
	) AS pivotTable;

/*19.Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. [Year, Stock Group Name1, Stock Group Name2, Stock Group Name3, ¡­ , Stock Group Name10]*/
CREATE VIEW StockQuanlityByYear2 AS
	SELECT 
		Years, [Clothing], [USB Novelties], [Computing Novelties], [Novelty Items],[T-Shirts], [Mugs],[Furry Footwear],[Toys], [Packaging Materials]
	FROM (
		SELECT YEAR(o.OrderDate) AS Years, sg.StockGroupName AS StockGroupName, sum(ol.Quantity) AS TotalQuantity 
		FROM Sales.OrderLines AS ol
		LEFT JOIN sales.Orders AS o 
		ON ol.OrderID = o.OrderID
		LEFT JOIN Warehouse.StockItems AS si 
		ON ol.StockItemID = si.StockItemID
		LEFT JOIN Warehouse.StockItemStockGroups AS ssg
		ON ssg.StockItemID = ol.StockItemID
		LEFT JOIN Warehouse.StockGroups AS sg
		ON sg.StockGroupID = ssg.StockGroupID
		WHERE YEAR(o.OrderDate) IN ( 2013, 2014, 2015, 2016, 2017)
		GROUP BY sg.StockGroupName, YEAR(o.OrderDate)
	) AS p
	PIVOT (
	SUM(p.TotalQuantity)
	FOR StockGroupName IN ([Clothing], [USB Novelties], [Computing Novelties], [Novelty Items],[T-Shirts], [Mugs],[Furry Footwear],[Toys], [Packaging Materials])
	) AS pivotTable
	ORDER BY Years


select *from stockbygroup order by stockyear
/*20.Create a function, input: order id; return: total of that order. List invoices and use that function to attach the order total to the other fields of invoices.*/
CREATE FUNCTION Sales.OrderTotal (@orderid INT)
RETURNS TABLE AS 
RETURN (
	SELECT OrderID, SUM(Quantity * UnitPrice) AS Total
	FROM Sales.OrderLines
	WHERE OrderID = @orderid
	GROUP BY OrderID
)

SELECT * FROM Sales.Invoices CROSS APPLY Sales.OrderTotal(OrderID) 


/*21.Create a new table called ods.Orders. Create a stored procedure, with proper error handling and transactions, that input is a date; when executed, it would find orders of that day, calculate order total, and save the information (order id, order date, order total, customer id) into the new table. If a given date is already existing in the new table, throw an error and roll back. Execute the stored procedure 5 times using different dates.*/
CREATE SCHEMA ods

GO

CREATE TABLE ods.Orders
(OrderID INT PRIMARY KEY,
OrderDate DATE,
OrderTotal DECIMAL(18, 2),
CustomerID INT)

GO

CREATE PROCEDURE ods.OrderTotalOfDate
	@OrderDate DATE
AS 

IF EXISTS (SELECT 1 FROM ods.Orders WHERE OrderDate = @OrderDate)
	BEGIN
		RAISERROR('Date Exists ', 16, 1)
	END
ELSE
	BEGIN
		BEGIN TRANSACTION
			INSERT INTO ods.Orders
			SELECT o.OrderID, o.OrderDate, f.Total, o.CustomerID
			FROM Sales.Orders o
				CROSS APPLY Sales.OrderTotal(OrderID) f
			WHERE o.OrderDate = @OrderDate
		COMMIT
	END
GO
EXEC ods.OrderTotalOfDate '2013-01-01'
EXEC ods.OrderTotalOfDate '2013-01-02'
EXEC ods.OrderTotalOfDate '2013-01-03'
EXEC ods.OrderTotalOfDate '2013-01-04'
EXEC ods.OrderTotalOfDate '2013-01-05'


--For Test
select * from ods.orders
Delete from ods.orders


/*22.Create a new table called ods.StockItem. It has following columns: [StockItemID], [StockItemName] ,[SupplierID] ,[ColorID] ,[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,[LeadTimeDays] ,[QuantityPerOuter] ,[IsChillerStock] ,[Barcode] ,[TaxRate]  ,[UnitPrice],[RecommendedRetailPrice] ,[TypicalWeightPerUnit] ,[MarketingComments]  ,[InternalComments], [CountryOfManufacture], [Range], [Shelflife]. Migrate all the data in the original stock item table.*/

CREATE TABLE ods.StockItem(
	[StockItemID] INT,
	[StockItemName] NVARCHAR(100),
	[SupplierID] INT, 
	[ColorID] INT,
	[UnitPackageID] INT,
	[OuterPackageID] INT,
	[Brand] NVARCHAR(50),
	[Size] NVARCHAR(20),
	[LeadTimeDays] INT ,
	[QuantityPerOuter] INT ,
	[IsChillerStock] BIT ,
	[Barcode]NVARCHAR(50) ,
	[TaxRate]  DECIMAL(18,3) ,
	[UnitPrice]DECIMAL(18,2),
	[RecommendedRetailPrice] DECIMAL(18,2),
	[TypicalWeightPerUnit]  DECIMAL(18,3),
	[MarketingComments] NVARCHAR(MAX)  ,
	[InternalComments] NVARCHAR(MAX), 
	[CountryOfManufacture] NVARCHAR(20), 
	[Range] NVARCHAR(20), 
	[Shelflife] NVARCHAR(20)
)

INSERT INTO ods.StockItem
SELECT
	StockItemID,
	StockItemName,
	SupplierID,
	ColorID,
	UnitPackageID,
	OuterPackageID,
	Brand,
	Size,
	LeadTimeDays,
	QuantityPerOuter,
	IsChillerStock,
	Barcode,
	TaxRate,
	UnitPrice,
	RecommendedRetailPrice,
	TypicalWeightPerUnit,
	MarketingComments,
	InternalComments,
	JSON_VALUE(CustomFields,'$.CountryOfManufacture') AS CountryOfManufacture,
	JSON_VALUE(CustomFields,'$.Range') AS [Range],
	JSON_VALUE(CustomFields,'$.ShelfLife') AS Shelflife
FROM WideWorldImporters.Warehouse.StockItems

/*23.Rewrite your stored procedure in (21). Now with a given date, it should wipe out all the order data prior to the input date and load the order data that was placed in the next 7 days following the input date.*/
CREATE PROCEDURE ods.NewOrderTotalOfDate
	@OrderDate DATE
AS 

BEGIN TRANSACTION
	DELETE FROM ods.Orders
	WHERE OrderDate < @OrderDate
COMMIT

BEGIN TRANSACTION
	MERGE ods.Orders T
	USING (	
			SELECT o.OrderID, o.OrderDate, f.Total, o.CustomerID
			FROM Sales.Orders o
				CROSS APPLY Sales.OrderTotal(OrderID) f
			WHERE DATEDIFF(d, @OrderDate, OrderDate) BETWEEN 1 AND 7
			) R
	ON T.OrderID = R.OrderID
	WHEN NOT MATCHED
		THEN INSERT VALUES (R.OrderID, R.OrderDate, R.Total, R.CustomerID);
COMMIT

AS

DELETE FROM ods.orders WHERE OrderDate<@inputdate;

with Orderinfo(OrderID, OrderTotal)
AS(SELECT ol.OrderID,  sum(ol.Quantity*ol.UnitPrice*(1+ol.TaxRate/100)) OrderTotal from Sales.OrderLines ol group by ol.OrderID)

INSERT into ods.orders (OrderID,OrderDate,OrderTotal,CustomerID )select oi.OrderID,o.OrderDate,oi.OrderTotal,o.CustomerID 
from Orderinfo oi join sales.Orders o on oi.OrderID = o.OrderID 
where o.OrderDate between @inputdate AND DATEADD(day, 7,@inputdate)

EXEC inputdate_load7days '2013-01-01';
select * from ods.orders

/*24.Consider the JSON file: Looks like that it is our missed purchase orders. Migrate these data into Stock Item, Purchase Order and Purchase Order Lines tables. Of course, save the script.*/
DECLARE @json NVARCHAR(MAX) = N'{
   "PurchaseOrders":[
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"7",
         "UnitPackageId":"1",
         "OuterPackageId":[
            6,
            7
         ],
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-01",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"WWI2308"
      },
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"5",
         "UnitPackageId":"1",
         "OuterPackageId":"7",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-025",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"269622390"
      }
   ]
}'


WITH cte AS (
(SELECT *
FROM OPENJSON(@json, '$.PurchaseOrders')
WITH (
	StockItemName NVARCHAR(50),
	Supplier INT,
	UnitPackageId INT,
	OuterPackageId NVARCHAR(MAX) AS JSON,
	Brand NVARCHAR(20),
	LeadTimeDays INT,
	QuantityPerOuter INT,
	TaxRate DECIMAL(18, 3),
	UnitPrice DECIMAL(18, 2),
	RecommendedRetailPrice DECIMAL(18, 2),
	TypicalWeightPerUnit DECIMAL(18, 3),
	CountryOfManufacture NVARCHAR(50),
	Range NVARCHAR(20),
	OrderDate NVARCHAR(20),
	DeliveryMethod NVARCHAR(20),
	ExpectedDeliveryDate NVARCHAR(20),
	SupplierReference NVARCHAR(20)
	)
	CROSS APPLY OPENJSON(OuterPackageId) WITH (NewOuterPackageId INT '$') 
	)
	
	UNION ALL

	(SELECT *
	FROM OPENJSON(@json, '$.PurchaseOrders')
	WITH (

	StockItemName NVARCHAR(50),
	Supplier INT,
	UnitPackageId INT,
	OuterPackageId NVARCHAR(MAX),
	Brand NVARCHAR(20),
	LeadTimeDays INT,
	QuantityPerOuter INT,
	TaxRate DECIMAL(18, 3),
	UnitPrice DECIMAL(18, 2),
	RecommendedRetailPrice DECIMAL(18, 2),
	TypicalWeightPerUnit DECIMAL(18, 3),
	CountryOfManufacture NVARCHAR(50),
	Range NVARCHAR(20),
	OrderDate NVARCHAR(20),
	DeliveryMethod NVARCHAR(20),
	ExpectedDeliveryDate NVARCHAR(20),
	SupplierReference NVARCHAR(20),
	OuterPackageId INT
		)
	)
)

DECLARE @maxid INT = (SELECT MAX(StockItemID)
						FROM Warehouse.StockItems);

SELECT IDENTITY(INT, 1, 1) AS StockItemId, * INTO #stock
FROM cte WHERE OuterPackageId is NOT NULL

INSERT INTO Warehouse.StockItems (StockItemID, StockItemName, SupplierID, UnitPackageId, OuterPackageId, Brand, LeadTimeDays, QuantityPerOuter, IsChillerStock, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit, LastEditedBy)
SELECT StockItemId + @maxid, StockItemName + '(' + CAST(StockItemID AS NVARCHAR) + ')' , Supplier, UnitPackageId, NewOuterPackageId, Brand, LeadTimeDays, QuantityPerOuter, 0, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit, 1
FROM #stock

/*25.Revisit your answer in (19). Convert the result in JSON string and save it to the server using TSQL FOR JSON PATH*/
SELECT Year AS Year,
	[Novelty Items] AS 'StockGroup.Novelty Items',
	[Clothing] AS 'StockGroup.Clothing', 
	[Mugs] AS 'StockGroup.Mugs',
	[T-Shirts] AS 'StockGroup.T-Shirts',
	[Airline Novelties] AS 'StockGroup.Airline Novelties', 
	[Computing Novelties] AS 'StockGroup.Computing Novelties', 
	[USB Novelties] AS 'StockGroup.USB Novelties', 
	[Furry Footwear] AS 'StockGroup.Furry Footwear', 
	[Toys] AS 'StockGroup.Toys', 
	[Packaging Materials] AS 'StockGroup.Packaging Materials'
FROM Sales.StockItemByName 
FOR JSON PATH

/*26.Revisit your answer in (19). Convert the result into an XML string and save it to the server using TSQL FOR XML PATH.*/
SELECT Year AS '@Year',
	[Novelty Items] AS NoveltyItems,
	[Clothing], 
	[Mugs],
	[T-Shirts],
	[Airline Novelties] AS AirlineNovelties, 
	[Computing Novelties] AS ComputingNovelties, 
	[USB Novelties] AS USBNovelties, 
	[Furry Footwear] AS FurryFootwear, 
	[Toys], 
	[Packaging Materials] AS PackagingMaterials
FROM Sales.StockItemByName 
FOR XML PATH('StockItems')

/*27.Create a new table called ods.ConfirmedDeviveryJson with 3 columns (id, date, value) . Create a stored procedure, input is a date. The logic would load invoice information (all columns) as well as invoice line information (all columns) and forge them into a JSON string and then insert into the new table just created. Then write a query to run the stored procedure for each DATE that customer id 1 got something delivered to him.*/
-- ??

/*28.Write a short essay talking about your understanding of transactions, locks and isolation levels. */
-- in pdf file 

/*29.Write a short essay, plus screenshots talking about performance tuning in SQL Server. Must include Tuning Advisor, Extended Events, DMV, Logs and Execution Plan.*/
-- in pdf file
SELECT login_name ,COUNT(session_id) AS session_count  
FROM sys.dm_exec_sessions  
GROUP BY login_name;

SELECT  c.session_id, c.net_transport, c.encrypt_option, c.auth_scheme, s.host_name, s.program_name,   
s.client_interface_name, s.login_name, s.nt_domain, s.nt_user_name, s.original_login_name, c.connect_time,  
s.login_time  
FROM sys.dm_exec_connections AS c   
JOIN sys.dm_exec_sessions AS s  
ON c.session_id = s.session_id   
WHERE c.session_id = @@SPID -- @@SPID returns your current session SPID

/*30-32 group assignment in pdf file/*