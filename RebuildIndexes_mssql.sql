----------------------------------------------------------------------------------------------------
-- http://technet.microsoft.com/en-us/library/ms188388(v=sql.105).aspx tells us the following:
--
-- Rebuilding or reorganizing small indexes often does not reduce fragmentation. The pages of small 
-- indexes are stored on mixed extents. Mixed extents are shared by up to eight objects, so the 
-- fragmentation in a small index might not be reduced after reorganizing or rebuilding it.
----------------------------------------------------------------------------------------------------

SET NOCOUNT ON;

BEGIN
	--------------------------------------------------------------------------------------------------
	-- Declare variables
	--------------------------------------------------------------------------------------------------

	DECLARE @nUMMFillFactor				SMALLINT	= 75;
	DECLARE @cUMMFileGroupIndexes	SYSNAME		= (SELECT fg.name 
																							 FROM sys.filegroups fg 
																										INNER JOIN sys.data_spaces ds 
																											 ON ds.data_space_id = fg.data_space_id 
																											AND ds.is_default = 1);
	DECLARE @cSQLStatement				NVARCHAR(MAX);

	IF (SELECT COUNT(1) FROM dba.SFIELD WHERE SFIENAME = N'UMMFILLFACTOR') = 1
	BEGIN
		SET @cSQLStatement = 'SELECT @nUMMFillFactor = us.UmmFillFactor,
					 @cUMMFileGroupIndexes = us.UmmFileGroupIndexes
			FROM dba.UmmSettings us
		 WHERE us.UmmId = N''ultimo'';';

		EXECUTE sp_executesql @cSQLStatement, N'@nUMMFillFactor SMALLINT OUTPUT, @cUMMFileGroupIndexes SYSNAME OUTPUT', 
			@nUMMFillFactor = @nUMMFillFactor OUTPUT, @cUMMFileGroupIndexes = @cUMMFileGroupIndexes OUTPUT;
	END;

	DECLARE crsIndex CURSOR LOCAL STATIC READ_ONLY FOR
		SELECT i.name, o.name, 
					 CASE WHEN stat.avg_fragmentation_in_percent < 30 THEN N'REORGANIZED' ELSE N'REBUILD' END AS [Action],
					 stat.avg_fragmentation_in_percent AS Defragmentation, 
					 s.name AS SchemaName, i.fill_factor,  ds.name
			FROM sys.indexes i
					 INNER JOIN sys.objects o
							ON i.object_id = o.object_id
						 AND i.name IS NOT NULL
						 AND i.is_hypothetical = 0
					 INNER JOIN sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, NULL) stat
							ON i.index_id = stat.index_id
						 AND i.object_id = stat.object_id
					 INNER JOIN sys.schemas s
							ON o.schema_id = s.schema_id
						 AND s.name = 'dba'
					 INNER JOIN sys.data_spaces ds
							ON i.data_space_id = ds.data_space_id
		 WHERE stat.avg_fragmentation_in_percent >= 5
			  OR i.fill_factor <> @nUMMFillFactor
			  OR ds.name <> @cUMMFileGroupIndexes
	   ORDER BY 2, 1;

	DECLARE @cIndexName			SYSNAME;
	DECLARE @cTableName			SYSNAME;
	DECLARE @cAction				NVARCHAR(20);
	DECLARE @nDefrag				NUMERIC(8,2);
	DECLARE @cSchemaName		SYSNAME;
	DECLARE @cSAPPSQL				NVARCHAR(32);
	DECLARE @cDBMSVersion		NVARCHAR(240);
	DECLARE @nCount					BIGINT;
	DECLARE @nFillFactor		SMALLINT;
	DECLARE @cFileGroup			SYSNAME;
	DECLARE @cPrintLine			NVARCHAR(240);
	DECLARE @nPrimaryKey		SMALLINT;
	DECLARE @nIsUnique			SMALLINT;
	DECLARE @cColumn				SYSNAME;
	DECLARE @cDescending		SYSNAME;
	DECLARE @cClusterType		SYSNAME;
	DECLARE @nIsIncluded		SMALLINT;
	DECLARE @cColumnList		NVARCHAR(MAX);
	DECLARE @cIncludeList		NVARCHAR(MAX);

	DECLARE @nRecoveryModel				SMALLINT;
	DECLARE @cAvailabilityGroupId	SYSNAME;

	--------------------------------------------------------------------------------------------------
	-- Show script header
	--------------------------------------------------------------------------------------------------

	EXECUTE dba.applsp_GetUltimoVersion @cSAPPSQL OUTPUT;

	SET @cDBMSVersion = SUBSTRING(REPLACE(@@version, N'  ', N' '), 22, PATINDEX(N'%Copyright%', @@version) - 22);

	PRINT N'----------------------------------------------------------------------------------------------------------------------------------------------';
	PRINT N'-- Copyright (c) 2013  ISH Personeelsparticipatie B.V.';
	PRINT N'--';
	PRINT N'-- Script                   : RebuildIndexes.sql';
	PRINT N'-- Part                     : Indexes';
	PRINT N'--';
	PRINT N'-- MS SQL Server version    : ' + REPLACE(@cDBMSVersion, CHAR(10), N'');
	PRINT N'-- Database                 : ' + DB_NAME();
	PRINT N'-- Ultimo database version  : ' + @cSAPPSQL;
	PRINT N'--';
	PRINT N'-- Fill factor              : ' + CONVERT(VARCHAR(100), @nUMMFillFactor);
	PRINT N'-- File group               : ' + @cUMMFileGroupIndexes;
	PRINT N'----------------------------------------------------------------------------------------------------------------------------------------------';
	PRINT N'-- ';

	--------------------------------------------------------------------------------------------------
	-- #0241307: Check correct script order
	--------------------------------------------------------------------------------------------------

	DECLARE @nContinue SMALLINT = (SELECT COUNT(1) FROM sys.objects o WHERE o.name = N'applsp_DatabaseUpdateCheck');

	IF @nContinue = 0
	BEGIN
		RAISERROR(N'Stored procedure ''applsp_DatabaseUpdateCheck'' doesn''t exist. Run the DatabaseRights.sql first.', 16, 1);
	END;

	EXECUTE dba.applsp_DatabaseUpdateCheck N'Indexes', @nContinue OUTPUT;

	IF @nContinue = 1
	BEGIN
		------------------------------------------------------------------------------------------------
		-- #0381995: Set recovery model to SIMPLE
		-- #0422617: Not for availability groups
		------------------------------------------------------------------------------------------------

		SELECT @nRecoveryModel = d.recovery_model,
					 @cAvailabilityGroupId = d.group_database_id
			FROM sys.databases d 
		 WHERE d.database_id = DB_ID();

		IF @nRecoveryModel <> 3 AND @cAvailabilityGroupId IS NULL
		BEGIN
			PRINT N'-- Recovery set to simple';
			PRINT N'--';

			SET @cSQLStatement = N'ALTER DATABASE [' + DB_NAME() + N'] SET RECOVERY SIMPLE;';
			EXECUTE (@cSQLStatement);
		END;

		PRINT N'-- Index rebuild started on : ' + CONVERT(VARCHAR, GETDATE(), 120);
		PRINT N'';
		PRINT N'----------------------------------------------------------------------------------------------------------------------------------------------';
		PRINT N'-- Table                           Index                           Action           Fragmentation  Fill factor  File group';
		PRINT N'-- ------------------------------  ------------------------------  ---------------  -------------  -----------  ------------------------------';

		OPEN crsIndex;
		FETCH NEXT FROM crsIndex INTO @cIndexName, @cTableName, @cAction, @nDefrag, @cSchemaName, @nFillFactor, @cFileGroup;

		WHILE @@FETCH_STATUS = 0
		BEGIN
			IF @cFileGroup <> @cUMMFileGroupIndexes
			BEGIN
				DECLARE crsIndexColumns CURSOR LOCAL STATIC READ_ONLY FOR
					SELECT is_primary_key, i.is_unique, c.name, 
								 CASE WHEN ic.is_descending_key = 0 THEN N'' ELSE N' DESC' END, 
								 i.type_desc, ic.is_included_column
						FROM sys.indexes i
								 INNER JOIN sys.index_columns ic
										ON i.object_id = ic.object_id
									 AND i.index_id = ic.index_id
									 AND i.name = @cIndexName
								 INNER JOIN sys.columns c
										ON ic.object_id = c.object_id
									 AND ic.column_id = c.column_id
					 ORDER BY ic.key_ordinal;

				OPEN crsIndexColumns;
				FETCH NEXT FROM crsIndexColumns 
							INTO @nPrimaryKey, @nIsUnique, @cColumn, @cDescending, @cClusterType, @nIsIncluded;

				IF @@FETCH_STATUS = 0
				BEGIN
					SET @cSQLStatement = N'CREATE ' + CASE WHEN @nIsUnique = 1 THEN N'UNIQUE ' ELSE N'' END + 
						@cClusterType + N' INDEX [' + @cIndexName + N'] ON dba.[' + @cTableName + N'] (';
					SET @cColumnList = N'x';
					SET @cIncludeList = N'x';

					WHILE @@FETCH_STATUS = 0
					BEGIN
						IF @nIsIncluded = 0
						BEGIN
							IF @cColumnList = N'x'
								SET @cColumnList = N'[' + @cColumn + N']' + @cDescending;
							ELSE
								SET @cColumnList = @cColumnList + N', [' + @cColumn + N']' + @cDescending;
						END
						ELSE
						BEGIN
							IF @cIncludeList = N'x'
								SET @cIncludeList = N'[' + @cColumn + N']' + @cDescending;
							ELSE
								SET	@cIncludeList = @cIncludeList + N', [' + @cColumn + N']' + @cDescending;
						END;
					
						FETCH NEXT FROM crsIndexColumns 
									INTO @nPrimaryKey, @nIsUnique, @cColumn, @cDescending, @cClusterType, @nIsIncluded;
					END;

					SET @cSQLStatement = @cSQLStatement + @cColumnList + N')' + 
						CASE WHEN @cIncludeList = N'x' THEN N'' ELSE ' INCLUDE (' + @cIncludeList + N')' END;
						
					IF @nFillFactor <> @nUMMFillFactor
						SET @cSQLStatement = @cSQLStatement + N' WITH (FILLFACTOR = ' + CONVERT(VARCHAR(8), @nUmmFillFactor) + 
							N', DROP_EXISTING = ON) ';
					ELSE 
						SET @cSQLStatement = @cSQLStatement + ' WITH (DROP_EXISTING = ON) ';

					SET @cSQLStatement = @cSQLStatement + N'ON [' + @cUMMFileGroupIndexes + N']';
				END;

				CLOSE crsIndexColumns;
				DEALLOCATE crsIndexColumns;
			END;
			ELSE
			BEGIN
				SET @cSQLStatement = N'ALTER INDEX [' + @cIndexName + N'] ON [' + @cSchemaName + N'].[' + @cTableName + N'] ';

				IF @cAction = N'REBUILD' or @nFillFactor <> @nUMMFillFactor
					SET @cSQLStatement = @cSQLStatement + N'REBUILD ';
				ELSE
				SET @cSQLStatement = @cSQLStatement + N'REORGANIZE ';

				IF @nFillFactor <> @nUMMFillFactor
					SET @cSQLStatement = @cSQLStatement + N'WITH (FILLFACTOR = ' + CONVERT(VARCHAR(8), @nUmmFillFactor) + ') ';
			END;

			BEGIN TRY
				EXECUTE (@cSQLStatement);

				SET @cPrintLine = N'-- ' + 
					@cTableName + SPACE(32 - LEN(@cTableName)) + 
					@cIndexName + SPACE(32 - LEN(@cIndexName)) + 
					LOWER(@cAction) + SPACE(17 - LEN(@cAction)) + 
					SPACE(13 - LEN(CONVERT(VARCHAR(8), @nDefrag) + N' %')) + CONVERT(VARCHAR(8), @nDefrag) + N' %' + 
					SPACE(13 - LEN(CONVERT(VARCHAR(8), @nFillFactor))) + CONVERT(VARCHAR(8), @nFillFactor) + N'  ' +
					LEFT(@cFileGroup, 30);

				PRINT @cPrintLine;

			END TRY
			BEGIN CATCH
				PRINT (@cSQLStatement);
				RAISERROR(N'The statement above is incorrect.', 16, 1);
			END CATCH

			FETCH NEXT FROM crsIndex INTO @cIndexName, @cTableName, @cAction, @nDefrag, @cSchemaName, @nFillFactor, @cFileGroup;
		END;

		CLOSE crsIndex;
		DEALLOCATE crsIndex;

		------------------------------------------------------------------------------------------------
		-- #0381995: Set recovery model to SIMPLE
		-- #0422617: Not for availability groups
		------------------------------------------------------------------------------------------------

		IF @nRecoveryModel <> 3 AND @cAvailabilityGroupId IS NULL
		BEGIN
			PRINT N'-- Recovery set to ' + CASE @nRecoveryModel WHEN 1 THEN N'full' WHEN 2 THEN N'bulk_logged' WHEN 3 THEN N'simple' END;
			PRINT N'--';

			SET @cSQLStatement = N'ALTER DATABASE [' + DB_NAME() + N'] SET RECOVERY ' + 
				CASE @nRecoveryModel WHEN 1 THEN N'FULL' WHEN 2 THEN N'BULK_LOGGED' WHEN 3 THEN N'SIMPLE' END + N';';
			EXECUTE (@cSQLStatement);
		END;

		PRINT N'-- ';
		PRINT N'-- Index rebuild ended on  : ' + CONVERT(VARCHAR, GETDATE(), 120);
		PRINT N'----------------------------------------------------------------------------------------------------------------------------------------------';
	END;
END;