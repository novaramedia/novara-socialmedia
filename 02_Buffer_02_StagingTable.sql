CREATE DEFINER=`fwatt`@`%` PROCEDURE `nm_spSocialMediaBufferQueue`()
BEGIN
	DECLARE CurrentDate DATE;
    SET CurrentDate = CAST(current_date() AS DATE);
	
	CREATE TEMPORARY TABLE SocMediaStagingTable	(			RandID				DECIMAL,
															PageID				INT,
                  											PageTitle			VARCHAR(1000),
                  											PageTypePrimary		INT,
								                  			PageTypeSecondary	INT,
                  											PublishDate			DATETIME,
						                  					PostingDate			DATE,
                                                            NovaraArchive		INT
    											);

	CREATE TEMPORARY TABLE SocMediaStagingTable2	(		PageID				INT,
															RandID 				DECIMAL,
                  											PageTitle			VARCHAR(1000),
                  											PageTypePrimary		INT,
								                  			PageTypeSecondary	INT,
                  											PublishDate			DATETIME,
                                                            PostingDate			DATE,
						                  					NovaraArchive		INT,
                                                            Platform			VARCHAR(100),
                                                            PostType			VARCHAR(100)
												);

	CREATE TEMPORARY TABLE SocMediaStagingTable3	(		RandID				DECIMAL,
															PageID				INT,
                  											PageTitle			VARCHAR(1000),
                  											PageTypePrimary		INT,
								                  			PageTypeSecondary	INT,
                  											PublishDate			DATETIME,
                                                            PostingDate			DATE,
						                  					NovaraArchive		INT,
                                                            Platform			VARCHAR(100),
                                                            PostType			VARCHAR(100)
												);

	INSERT INTO SocMediaStagingTable (RandID, PageID, PageTitle, PageTypePrimary, PageTypeSecondary, PostingDate, PublishDate, NovaraArchive)
	SELECT		RAND() AS RandID,
				ID AS PageID,
				PageTitle,
				PageTypePrimary,
				PageTypeSecondary,
				CAST(current_date() AS DATE) AS PostingDate,
				PublishDate,
                1 AS NovaraArchive
	FROM		WebsitePages
	WHERE 		PageTypePrimary != 828 AND PageTypeSecondary != 828
	AND			PublishDate < CAST(date_add(current_date(), INTERVAL -50 DAY) AS DATE)
	AND			PublishDate > CAST(date_add(current_date(), INTERVAL -1 YEAR) AS DATE)
	AND			IFNULL(LastPosted,'2000-01-01') < CAST(date_add(current_date(), INTERVAL -6 WEEK) AS DATE)
	ORDER BY	RAND();

    INSERT INTO SocMediaStagingTable2 (PageID, PageTitle, PageTypePrimary, PageTypeSecondary, PublishDate, PostingDate, NovaraArchive, Platform, PostType)
	SELECT 		ID AS PageID,
				PageTitle,
				PageTypePrimary,
				PageTypeSecondary,
				PublishDate,
				CAST(current_date() AS DATE) AS PostingDate,
				0 AS NovaraArchive,
                'Twitter',
                'Repost'
	FROM		WebsitePages
	WHERE 		PageTypePrimary != 828 AND PageTypeSecondary != 828
	AND			DATEDIFF(PublishDate,CAST(current_date() AS DATE))  IN (-3, -6)
	ORDER BY	PublishDate DESC;

    INSERT INTO SocMediaStagingTable2 (PageID, PageTitle, PageTypePrimary, PageTypeSecondary, PublishDate, PostingDate, NovaraArchive, Platform, PostType)
	SELECT 		ID AS PageID,
				PageTitle,
				PageTypePrimary,
				PageTypeSecondary,
				PublishDate,
				CAST(current_date() AS DATE) AS PostingDate,
				0 AS NovaraArchive,
                'Facebook',
                'Repost'
	FROM		WebsitePages
	WHERE 		PageTypePrimary != 828 AND PageTypeSecondary != 828
	AND			DATEDIFF(PublishDate,CAST(current_date() AS DATE))  IN (-3, -6)
	ORDER BY	PublishDate DESC;

    INSERT INTO SocMediaStagingTable2 (PageID, RandID, PageTitle, PageTypePrimary, PageTypeSecondary, PublishDate, PostingDate, NovaraArchive, Platform, PostType)
	SELECT 		PageID,
				RandID,
				PageTitle,
				PageTypePrimary,
				PageTypeSecondary,
				PublishDate,
				PostingDate,
				1 AS NovaraArchive,
                'Twitter',
                'Archive'
	FROM		SocMediaStagingTable
	WHERE 		PageTypePrimary = 2
	ORDER BY	RandID DESC;

	INSERT INTO SocMediaStagingTable3 (RandID, PageID, PageTitle, PageTypePrimary, PageTypeSecondary, PublishDate, NovaraArchive, Platform, PostType)
	SELECT 		RandID, PageID, PageTitle, PageTypePrimary, PageTypeSecondary, PublishDate, NovaraArchive, Platform, PostType
	FROM 		SocMediaStagingTable2
	ORDER BY 	RandID
	LIMIT		1;
    
    INSERT INTO WebsiteSocialMediaHistory (PageID, PageTitle, PageTypePrimary, PageTypeSecondary, PublishDate, PostingDate, NovaraArchive, Platform, PostType)
	SELECT		PageID, PageTitle, PageTypePrimary, PageTypeSecondary, PublishDate, CAST(current_date() AS DATE), NovaraArchive, Platform, PostType
	FROM		SocMediaStagingTable3;

	UPDATE 	WebsitePages w
	SET		LastPosted = @CurrentDate,
			PostCount = IFNULL(PostCount, 0) + 1
	WHERE	ID IN (	SELECT	PageID
					FROM	WebsiteSocialMediaHistory 
					WHERE	PostingDate = @CurrentDate);

END
