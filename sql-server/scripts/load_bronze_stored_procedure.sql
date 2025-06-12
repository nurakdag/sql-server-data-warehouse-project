CREATE OR ALTER PROCEDURE bronze.load_bronze AS 

BEGIN 

	DECLARE @start_time DATETIME, @end_time DATETIME ,@batch_start_time DATETIME, @batch_end_time DATETIME;
	
	
	BEGIN TRY
	SET @batch_start_time=GETDATE();
		PRINT '========================================';
		PRINT 'Loading bronze layer';
		PRINT '========================================';



		PRINT'-----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT'-----------------------------------------';


		SET @start_time = GETDATE();

		PRINT '>> TRUNCATING TABLE :bronze.crm_cust_info ';
		PRINT '>> INSERTING TABLE :bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\akdag\Masaüstü\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>----------------------------------------------------------------------------------------';


		SET @start_time = GETDATE();

		PRINT '>> TRUNCATING TABLE :bronze.crm_prd_info ';
		PRINT '>> INSERTING TABLE :bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info 
		FROM 'C:\Users\akdag\Masaüstü\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW= 2,
			FIELDTERMINATOR= ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>----------------------------------------------------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE :crm_sales_details ';
		PRINT '>> INSERTING TABLE :crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\akdag\Masaüstü\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>>----------------------------------------------------------------------------------------';



		PRINT'-----------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT'-----------------------------------------';

		
		PRINT '>> TRUNCATING TABLE :bronze.erp_cust_az12 ';
		PRINT '>> INSERTING TABLE :bronze.erp_cust_az12';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\akdag\Masaüstü\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + ' seconds';
		PRINT '>>----------------------------------------------------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE :bronze.erp_cust_az12 ';
		PRINT '>> INSERTING TABLE :bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\akdag\Masaüstü\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) +' seconds';
		PRINT '>>----------------------------------------------------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE :bronze.erp_px_cat_g1V2 ';
		PRINT '>> INSERTING TABLE :bronze.erp_px_cat_g1V2';
		TRUNCATE TABLE bronze.erp_px_cat_g1V2
		BULK INSERT bronze.erp_px_cat_g1V2
		FROM 'C:\Users\akdag\Masaüstü\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration  ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + '  seconds';
		PRINT '>>----------------------------------------------------------------------------------------';

       SET @end_time= GETDATE();

	   PRINT '>> Load duration of batch:  ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH 
		PRINT'==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'==========================================';
	END CATCH
END 





