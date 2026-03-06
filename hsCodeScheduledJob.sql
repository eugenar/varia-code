USE [msdb]
GO

/****** Object:  Job [HSCustomDescriptionText]    Script Date: 3/23/2018 10:29:42 AM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [AvaTaxAccount]    Script Date: 3/23/2018 10:29:42 AM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'AvaTaxAccount' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'AvaTaxAccount'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'HSCustomDescriptionText', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Add custom description HS code map and re-populate the full text index', 
		@category_name=N'AvaTaxAccount', 
		@owner_login_name=N'AvaService', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Add HS code custom description]    Script Date: 3/23/2018 10:29:43 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Add HS code custom description', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=1, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE AvaTaxAccount;
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
COMMIT', 
		@database_name=N'AvaTaxAccount', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Populate full text index]    Script Date: 3/23/2018 10:29:43 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Populate full text index', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'ALTER FULLTEXT INDEX ON [AvaTaxAccount]..[HS_TaxableDescriptionAccumulated]
START INCREMENTAL POPULATION
', 
		@database_name=N'AvaTaxAccount', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'HSCodeFullTextIndex', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20180323, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'41c19dd0-ab99-4646-a2fe-214b3472f02b'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

