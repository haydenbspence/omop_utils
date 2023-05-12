-- Batch size (number of rows to process at a time)
DECLARE @batch_size INT = 1000;
DECLARE @rows_affected INT;

-- Create a cursor to loop through all tables in the Src schema
DECLARE table_cursor CURSOR FOR
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'Src';

DECLARE @src_table_name NVARCHAR(128);
DECLARE @dest_table_name NVARCHAR(128);
DECLARE @sql NVARCHAR(MAX);

OPEN table_cursor;

FETCH NEXT FROM table_cursor INTO @src_table_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Generate the destination table name by removing 'OMOPV5_' prefix
    SET @dest_table_name = 'OMOPV5.' + REPLACE(@src_table_name, 'OMOPV5_', '');

    -- Generate the dynamic SQL query for the current table
    SET @sql = N'INSERT INTO ' + @dest_table_name + ' (';

    SELECT @sql = @sql +
        QUOTENAME(LOWER(COLUMN_NAME)) + ', '
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'Src' AND TABLE_NAME = @src_table_name
    AND LOWER(COLUMN_NAME) IN (
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'OMOPV5' AND TABLE_NAME = REPLACE(@src_table_name, 'OMOPV5_', '')
    );

    -- Check if there are matching columns to insert
    IF LEN(@sql) > LEN(N'INSERT INTO ' + @dest_table_name + ' (')
    BEGIN
        SET @sql = LEFT(@sql, LEN(@sql) - 1) + N') SELECT TOP (' + CAST(@batch_size AS NVARCHAR(10)) + N') ';

        SELECT @sql = @sql +
            QUOTENAME(COLUMN_NAME) + ' AS ' + QUOTENAME(LOWER(COLUMN_NAME)) + ', '
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'Src' AND TABLE_NAME = @src_table_name
        AND LOWER(COLUMN_NAME) IN (
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'OMOPV5' AND TABLE_NAME = REPLACE(@src_table_name, 'OMOPV5_', '')
        );

        SET @sql = LEFT(@sql, LEN(@sql) - 1) + N' FROM Src.' + @src_table_name + ' WITH (INDEX(0)) WHERE NOT EXISTS (SELECT 1 FROM ' + @dest_table_name + ' WHERE ';

        SELECT @sql = @sql +
            'Src.' + @src_table_name + '.' + QUOTENAME(COLUMN_NAME) + ' = ' + @dest_table_name + '.' + QUOTENAME(LOWER(COLUMN_NAME)) + ' AND '
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'Src' AND TABLE_NAME = @src_table_name
        AND LOWER(COLUMN_NAME) IN (
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'OMOPV5' AND TABLE_NAME = REPLACE(@src_table_name, 'OMOPV5_', '')
        );

        SET @sql = LEFT(@sql, LEN(@sql) - 4) + N');';

        -- Batch processing loop
        SET @rows_affected = 1;
        WHILE @

        -- Batch processing loop
        SET @rows_affected = 1;
        WHILE @rows_affected > 0
        BEGIN
            -- Execute the generated SQL query
            EXEC sp_executesql @sql, N'@rows_affected_out INT OUTPUT', @rows_affected_out = @rows_affected OUTPUT;

            -- Update the rows affected count
            SET @rows_affected = @rows_affected;
        END;
    END;

    -- Move to the next table
    FETCH NEXT FROM table_cursor INTO @src_table_name;
END;

CLOSE table_cursor;
DEALLOCATE table_cursor;
