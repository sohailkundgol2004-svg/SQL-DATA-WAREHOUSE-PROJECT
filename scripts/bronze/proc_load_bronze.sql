/*
================================================================================
 Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================================

 Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.

    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from CSV files into bronze tables.

 Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

 Usage Example:
    EXEC bronze.load_bronze;
================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
  DECLARE @start_time datetime, @end_time DATETIME, @start_time_batch DATETIME, @end_time_batch DATETIME;
  BEGIN TRY
    SET @start_time_batch = GETDATE();
    PRINT '=============================';
    PRINT 'loading Bronze Layer';
    PRINT '=============================';

    PRINT '-------------------------';
    PRINT 'Loading CRM Table';
    PRINT '--------------------------';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT '>> Insertoable: bronze.crm_cust_info';
    BULK INSERT bronze.crm_cust_info
    FROM 'E:\new_project_dataset\source_crm\cust_info.csv'
    WITH (
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
    );
    SET @end_time = GETDATE();
    print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    print '--------------------'


    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    PRINT '>> Insertoable: bronze.crm_prd_info';
    BULK INSERT bronze.crm_prd_info
    FROM 'E:\new_project_dataset\source_crm\prd_info.csv'
    WITH (
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
    );
    SET @end_time = GETDATE();
    print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    print '--------------------'


    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.crm_sales_info';
    TRUNCATE TABLE bronze.crm_sales_details;
    PRINT '>> Insertoable: bronze.crm_sales_info';
    BULK INSERT bronze.crm_sales_details
    FROM 'E:\new_project_dataset\source_crm\sales_details.csv'
    WITH (
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
    );
    SET @end_time = GETDATE();
    print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    print '--------------------'


    PRINT '-------------------------';
    PRINT 'Loading ERP Table';
    PRINT '--------------------------';


   SET @start_time = GETDATE();
   PRINT '>> Truncating Table: bronze.erp_cust_az12';
   TRUNCATE TABLE bronze.erp_cust_az12;
   PRINT '>> Insertoable: bronze.erp_cust_az12';
   BULK INSERT bronze.erp_cust_az12
   FROM  'E:\new_project_dataset\source_erp\CUST_AZ12.csv'                            
   WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
   );
    SET @end_time = GETDATE();
    print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    print '--------------------'

   SET @start_time = GETDATE();
   PRINT '>> Truncating Table: bronze.erp_loc_a101';
   TRUNCATE TABLE bronze.erp_loc_a101;
   PRINT '>> Insertoable: bronze.erp_erp_a101';
   BULK INSERT bronze.erp_loc_a101
   FROM 'E:\new_project_dataset\source_erp\LOC_A101.csv'                        
   WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
   );
    SET @end_time = GETDATE();
    print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    print '--------------------'


   SET @start_time = GETDATE();
   PRINT '>> Truncating Table: bronze.erp_px_cat_giv2';
   TRUNCATE TABLE bronze.erp_px_cat_giv2;
   PRINT '>> Insertoable: bronze.erp_erp_px_cat_giv2';
   BULK INSERT bronze.erp_px_cat_giv2
   FROM 'E:\new_project_dataset\source_erp\PX_CAT_G1V2.csv'                     
   WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
   );
    SET @end_time = GETDATE();
    print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
    print '--------------------'

    SET @end_time_batch = GETDATE();
    print '==============================================='
    print ' Loading Bronze Layer Completed;'
    print ' -- Total load Duration: ' + CAST(DATEDIFF(second, @start_time_batch, @end_time_batch) AS NVARCHAR) + 'seconds';
    print '--------------------'



END TRY
BEGIN CATCH
   PRINT '=============================='
   PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
   PRINT 'ERROR Massage' + ERROR_MESSAGE();
   PRINT 'ERROR Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
   PRINT 'ERROR Message' + CAST (ERROR_STATE() AS NVARCHAR);
   PRINT '=============================='
END CATCH
END
