-- Add all customer descriptions to the descriptions in HS_TaxableDescriptionAccumulated (should execute periodically from within a sql scheduled job)
USE AvaTaxAccount;
BEGIN TRANSACTION;
DECLARE @cur bigint
SET @cur = IDENT_CURRENT('HS_CustomCodeMap')
UPDATE hsda
SET hsda.Description = CONCAT(hsda.Description, hs_concat.Descr)
FROM dbo.HS_TaxableDescriptionAccumulated AS hsda
INNER JOIN 
(SELECT HSCodeId, Descr = (SELECT N' ' + Description FROM dbo.HS_CustomCodeMap WHERE Id <= @cur 
FOR XML PATH(N''))
FROM dbo.HS_CustomCodeMap hscm
GROUP BY hscm.HSCodeId) AS hs_concat
ON hsda.HSCodeId = hs_concat.HSCodeId

INSERT INTO dbo.HS_CustomCodeMapHistory (HSCodeId, Description) SELECT HSCodeId, Description from dbo.HS_CustomCodeMap WHERE Id <= @cur

DELETE FROM dbo.HS_CustomCodeMap WHERE Id <= @cur
COMMIT

USE AvaTaxAccount;
CREATE TABLE [dbo].[HS_TaxableDescriptionAccumulated](
	[HSCodeId] [bigint] NOT NULL,
	[Description] [varchar](max) NULL
 CONSTRAINT [PK_Description_HSCodeId] PRIMARY KEY CLUSTERED 
(
	[HSCodeId] DESC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]


-- check if fulltext search is installed
IF FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') = 0
BEGIN
RAISERROR('Full text is not installed.', 20, 1) WITH LOG
END

-- enable fulltext search on the database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE database_id = DB_ID() AND is_fulltext_enabled = 1)
BEGIN
exec sp_fulltext_database 'enable'
END;

-- create default fulltext catalog
IF NOT EXISTS (SELECT 1 from sys.fulltext_catalogs WHERE is_default = 1)
BEGIN
CREATE FULLTEXT CATALOG ft_catalog WITH ACCENT_SENSITIVITY = OFF AS DEFAULT
END;

-- create text index on Description using system stop-word list and tracked changes OFF (populate periodically via a scheduled sql job); index will be added to the default catalog; database must have a default fulltext catalog created
CREATE FULLTEXT INDEX ON [AvaTaxAccount]..[HS_TaxableDescriptionAccumulated](Description Language 1033)   
   KEY INDEX [PK_Description_HSCodeId]   
   WITH CHANGE_TRACKING = MANUAL, STOPLIST = SYSTEM; 
   
-- populate text index (should execute from within a sql scheduled job using SQL Server Agent)
ALTER FULLTEXT INDEX ON [AvaTaxAccount]..[HS_TaxableDescriptionAccumulated]
START UPDATE POPULATION



CREATE TABLE [dbo].[HS_CustomCodeMap](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[HSCodeId] [bigint] NOT NULL,
	[Description] [varchar](8000) NULL
 CONSTRAINT [PK_Id] PRIMARY KEY CLUSTERED 
(
	[Id] DESC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]


-- not really needed
ALTER TABLE [dbo].[HS_CustomCodeMap]  WITH CHECK ADD  CONSTRAINT [FK_HS_CustomCodeMap_HSCodeId] FOREIGN KEY([HSCodeId])
REFERENCES [dbo].[HS_Master] ([HSCodeId])

CREATE TABLE [dbo].[HS_CustomCodeMapHistory](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[HSCodeId] [bigint] NOT NULL,
	[Description] [varchar](8000) NULL
 CONSTRAINT [PK_HS_Id] PRIMARY KEY CLUSTERED 
(
	[Id] DESC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

-- not really needed
ALTER TABLE [dbo].[HS_CustomCodeMapHistory]  WITH CHECK ADD  CONSTRAINT [FK_HS_CustomCodeMapHistory_HSCodeId] FOREIGN KEY([HSCodeId])
REFERENCES [dbo].[HS_Master] ([HSCodeId])



-- HS_Master hierarchy closure; could be used for fast tree (branches) UI display (though the same can be achieved by caching the entire HS codes tree)
WITH hs_cte AS
(
    SELECT
        hs.ParentHSCodeId AS ancestor,
        hs.HSCodeId AS descendant,
        0  AS depth
    FROM AvaTaxAccount..HS_Master hs

    UNION ALL

    SELECT
        CTE.ancestor  AS ancestor,
        hs.HSCodeId    AS descendant,
        CTE.depth + 1 AS depth
    FROM AvaTaxAccount..HS_Master AS hs
    JOIN hs_cte AS CTE
        ON hs.ParentHSCodeId = CTE.descendant
)

--SELECT COUNT(*) FROM hs_cte


-- Update HS_TaxableDescriptionAccumulated IsTaxable codes with the cumulated descriptions of all their ancestor codes; execute ONCE prior to first text search index population
USE AvaTaxAccount;
WITH hs_cte AS
(
    SELECT
        ParentHSCodeId,
        HSCodeId,
		IsTaxable,
        CAST('' AS varchar(max)) AS Description
    FROM AvaTaxAccount..HS_Master WHERE ParentHSCodeId = (SELECT MIN(ParentHSCodeId) FROM [AvaTaxAccount].[dbo].[HS_Master])

    UNION ALL

    SELECT
        hs_cte.ParentHSCodeId,
        hs.HSCodeId,
		hs.IsTaxable,
        hs_cte.Description + ' ' + hs.Description
    FROM AvaTaxAccount..HS_Master AS hs
    JOIN hs_cte
        ON hs.ParentHSCodeId = hs_cte.HSCodeId
)

MERGE INTO AvaTaxAccount..HS_TaxableDescriptionAccumulated AS hsta
USING hs_cte ON hsta.HSCodeId = hs_cte.HSCodeId
WHEN NOT MATCHED AND hs_cte.IsTaxable=1 THEN
INSERT (HSCodeId, Description) VALUES (hs_cte.HSCodeId, hs_cte.Description)
WHEN MATCHED AND hs_cte.IsTaxable=1 THEN
UPDATE SET hsta.Description = hs_cte.Description;

--SELECT hda.[HSCodeId],hda.[Description],hs.[SystemId] FROM [dbo].[HS_TaxableDescriptionAccumulated] hda
--INNER JOIN [dbo].[HS_Master] hs ON hda.[HSCodeId] = hs.HSCodeId
--INNER JOIN FREETEXTTABLE([HS_TaxableDescriptionAccumulated], Description, 'cows beef', 2) AS KEY_TBL ON --hda.[HSCodeId] = KEY_TBL.[KEY]
--WHERE hs.SystemId = 1
--ORDER BY KEY_TBL.[RANK] DESC
