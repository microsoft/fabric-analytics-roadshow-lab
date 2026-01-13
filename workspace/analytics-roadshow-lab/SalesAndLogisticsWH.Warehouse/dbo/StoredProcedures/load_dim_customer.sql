CREATE PROCEDURE dbo.load_dim_customer (@new_load_datetime DATETIME2(6) = NULL)
AS
BEGIN
    /* Get last update datetime from etl_tracking table and set new load time variable */
    SELECT @new_load_datetime = ISNULL(@new_load_datetime, GETDATE())
    DECLARE @last_load_datetime DATETIME2(6) = (SELECT last_load_datetime FROM dbo.etl_tracking WHERE table_name = 'dim.customer')

    /* Handle the unknown member */
    IF NOT EXISTS (SELECT * FROM dim.customer WHERE customer_ak = 'Unknown')
    INSERT INTO dim.customer VALUES ('Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0, 0, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01', '12-31-2099')

    /* UPSERT the data from the lakehouse bronze tables */
    MERGE dim.customer AS t
    USING 
        (
            SELECT
                [customer_id] AS customer_ak,
                [customer_name],
                [description] AS customer_description,
                [primary_contact_first_name],
                [primary_contact_last_name],
                [primary_contact_email],
                [primary_contact_phone],
                [delivery_city],
                [delivery_country],
                [delivery_latitude],
                [delivery_longitude],
                [delivery_state],
                [delivery_zip_code],
                [billing_city],
                [billing_country],
                [billing_state],
                '1900-01-31' AS start_date,
                '2099-12-31' AS end_date
            FROM SalesAndLogisticsLH.silver.customer
            WHERE
                _processing_timestamp > @last_load_datetime
                AND _processing_timestamp <= @new_load_datetime
        ) AS s
        ON t.customer_ak = s.customer_ak
    WHEN MATCHED THEN
        UPDATE
        SET
            customer_ak                 = s.customer_ak,
            customer_name               = s.customer_name,
            customer_description        = s.customer_description,
            primary_contact_first_name  = s.primary_contact_first_name,
            primary_contact_last_name   = s.primary_contact_last_name,
            primary_contact_email       = s.primary_contact_email,
            primary_contact_phone       = s.primary_contact_phone,
            delivery_city               = s.delivery_city,
            delivery_country            = s.delivery_country,
            delivery_latitude           = s.delivery_latitude,
            delivery_longitude          = s.delivery_longitude,
            delivery_state              = s.delivery_state,
            delivery_zip_code           = s.delivery_zip_code,
            billing_city                = s.billing_city,
            billing_country             = s.billing_country,
            billing_state               = s.billing_state,
            start_date                  = s.start_date,
            end_date                    = s.end_date
    WHEN NOT MATCHED THEN 
        INSERT (customer_ak, customer_name, customer_description, primary_contact_first_name, primary_contact_last_name, primary_contact_email, primary_contact_phone, delivery_city, delivery_country, delivery_latitude, delivery_longitude, delivery_state, delivery_zip_code, billing_city, billing_country, billing_state, start_date, end_date)
        VALUES (s.customer_ak, s.customer_name, s.customer_description, s.primary_contact_first_name, s.primary_contact_last_name, s.primary_contact_email, s.primary_contact_phone, s.delivery_city, s.delivery_country, s.delivery_latitude, s.delivery_longitude, s.delivery_state, s.delivery_zip_code, s.billing_city, s.billing_country, s.billing_state, s.start_date, s.end_date);

    /* Update the etl_tracking table */
    UPDATE dbo.etl_tracking
    SET last_load_datetime = @new_load_datetime
    WHERE table_name = 'dim.customer'
END