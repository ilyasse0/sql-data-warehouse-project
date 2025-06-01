/*
-------------------------------------------------------------------
STORED PROCEDURE :LOAD_BRONZE_LAYER  (source --> Bronze)
Script purpose : 
  This stored procedure is used to load raw data from CSV files located in the local file system 
  into the Bronze layer of a data warehouse. It handles the ingestion of CRM and ERP datasets by 
  truncating the target tables and then using BULK INSERT to load new data. The procedure also logs 
  load durations and handles potential errors gracefully.

Params : 
  NONE

Usage Exemple :
  EXEC bronze.load_bronze; 
--------------------------------------------------------------------
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME  ,@batch_start_time DATETIME ,@batch_end_time DATETIME;
	BEGIN TRY
		
			SET @batch_start_time = GETDATE();
	
		PRINT'-----------------------------------------------------------------------------';
		PRINT 'LOADING THE BRONZE LAYER...';
		PRINT'-----------------------------------------------------------------------------';


		PRINT'==============================================================================';
		PRINT 'LOADING CRM SECTION...';
		PRINT'==============================================================================';



			---- crm_cust_info-----
			SET @start_time =GETDATE();
			TRUNCATE TABLE bronze.crm_cust_info
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\ilyas\OneDrive\Documents\data-analytics\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW =2,
				FIELDTERMINATOR  = ',',
				TABLOCK
			);

			---- crm_prd_info ----
			TRUNCATE TABLE bronze.crm_prd_info
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\ilyas\OneDrive\Documents\data-analytics\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW =2,
				FIELDTERMINATOR  = ',',
				TABLOCK
			);
			SET @end_time =GETDATE();
			PRINT '>> LOAD DURATION : '+ CAST ( DATEDIFF(second , @start_time , @end_time) AS NVARCHAR )+ 'seconds';
			PRINT '_______________________'





			---- crm_sales_details ----
			SET @start_time =GETDATE();
			TRUNCATE TABLE bronze.crm_sales_details
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\ilyas\OneDrive\Documents\data-analytics\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW =2,
				FIELDTERMINATOR  = ',',
				TABLOCK
			);
			SET @end_time =GETDATE();
			PRINT '>> LOAD DURATION : '+ CAST ( DATEDIFF(second , @start_time , @end_time) AS NVARCHAR )+ 'seconds';
			PRINT '_______________________'



		PRINT'==============================================================================';
		PRINT 'LOADING ERP SECTION...';
		PRINT'==============================================================================';

			---- erp_cust_az12 ----
			SET @start_time =GETDATE();
			TRUNCATE TABLE bronze.erp_cust_az12
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\ilyas\OneDrive\Documents\data-analytics\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIRSTROW =2,
				FIELDTERMINATOR  = ',',
				TABLOCK
			);
			SET @end_time =GETDATE();
			PRINT '>> LOAD DURATION : '+ CAST ( DATEDIFF(second , @start_time , @end_time) AS NVARCHAR )+ 'seconds';
			PRINT '_______________________'


			---- erp_loc_a101 ----
			SET @start_time =GETDATE();
			TRUNCATE TABLE bronze.erp_loc_a101
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\ilyas\OneDrive\Documents\data-analytics\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIRSTROW =2,
				FIELDTERMINATOR  = ',',
				TABLOCK
			);
			SET @end_time =GETDATE();
			PRINT '>> LOAD DURATION : '+ CAST ( DATEDIFF(second , @start_time , @end_time) AS NVARCHAR )+ 'seconds';
			PRINT '_______________________'



			---- erp_erp_px_cat_g1v2 ----
			SET @start_time =GETDATE();
			TRUNCATE TABLE bronze.erp_px_cat_g1v2
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\ilyas\OneDrive\Documents\data-analytics\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW =2,
				FIELDTERMINATOR  = ',',
				TABLOCK
			);
			SET @end_time =GETDATE();
			PRINT '>> LOAD DURATION : '+ CAST ( DATEDIFF(second , @start_time , @end_time) AS NVARCHAR )+ 'seconds';
			PRINT '_______________________';


			----- HABDLING THE EXECPTION -----
	END TRY
	BEGIN CATCH
		PRINT '###################################################################';
		PRINT 'ERROR HAS OCCURED DURING THE LOADING THE BRONZE LAYER';
		PRINT 'ERROR MESSAGE -->' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER -->' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR NUMBER -->' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '###################################################################';

	END CATCH

	PRINT '-----------------------------------------------------------------------'
	PRINT'THE LOADING BATCHIS COMPLETE'
	SET @batch_end_time =GETDATE();
			PRINT '>> BATCH COMPLETE LOAD DURATION : '+ CAST ( DATEDIFF(second , @batch_start_time , @batch_end_time) AS NVARCHAR )+ 'seconds';
	PRINT '-----------------------------------------------------------------------'

END;



--- simmle testing the quality of the bulk data 

SELECT * FROM bronze.crm_cust_info;

SELECT * FROM bronze.crm_prd_info;

SELECT * FROM bronze.crm_sales_details;

SELECT * FROM bronze.erp_cust_az12;



