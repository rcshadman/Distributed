set search_path to :'schema_name';
SELECT NULL AS "Letter Templates";
SELECT NULL AS "Count of templates created by user [user defined template]";

select count(*) from letter_template where createdon between now() - interval '6 month' and now();
	
SELECT NULL AS "Count of templates added by user from template gallery NA";
SELECT NULL AS "Number of times a template has been edited [Unique by templates]";
select additionalinfo, count(additionalinfo) from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and additionalinfo like '%Update%' and code like '%letter_template_add%' and auditdate between now() - interval '6 month' and now() group by additionalinfo;

SELECT NULL AS "Count of users who have setup letter templates (Created a new template)";
select distinct count(userid) from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and additionalinfo like '%Added%' and code like '%letter_template_add%' and auditdate between now() - interval '6 month' and now() group by userid, additionalinfo;

SELECT NULL AS "Number of templates which has been marked as ‘disabled’ along with count of users";
select updatedby,count(isenabled)  from letter_template where isenabled is False and createdon between now() - interval '6 month' and now() group by updatedby;

SELECT NULL AS "Count of users who have created a new serial number NA";
select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and code like '%sequence_config_add%' and auditdate between now() - interval '6 month' and now()) t;


SELECT NULL AS "Count of users who have downloaded a template NA";

SELECT NULL AS "Count of users who have configured a mail template (i.e. don’t have ‘None’ as mail template) along with number of templates NA";
       

SELECT NULL AS "Count of users who have configured ‘None’ as the mail template along with Number of templates";
select count( updatedby), count(*) from letter_template,mailtemplate, mailtemplatecategory where templatecategory = mailtemplatecategory.cid and mailtemplateid = mailtemplate.cid  and mailtemplatecategory.code in ('letter')  and createdon between now() - interval '6 month' and now() group by updatedby;










 

SELECT NULL AS "Prepare Letters";
SELECT NULL AS "Count of letters generated  [month wise breakdown] - letter_ letters";
select count(*), date_trunc( 'month', letterdate ) from letter_letters where ispublished is True and letterdate between now() - interval '6 month' and now() group by date_trunc( 'month', letterdate );

SELECT NULL AS "Count of users who have accessed letters module - tblactiveservice";
SELECT NULL AS "Count of letters generated without authorized signatory - letter_ letters";
select count(*) from letter_letters where authorizedsignatory is NULL and letterdate between now() - interval '6 month' and now();

SELECT NULL AS "Count of users who downloaded a letter - tblauditlog";
select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and additionalinfo like '%download%' and additionalinfo like '%letter%' and auditdate between now() - interval '6 month' and now()) t;

SELECT NULL AS "Count of users who published the letter to the employee portal - letter_letters : published field";
select count(*) from letter_letters where ispublished is True and letterdate between now() - interval '6 month' and now();

SELECT NULL AS "Count of users who published the letter via email - isemailed .. like above";
select count(*) from letter_letters where ispublished is True and sendemail is True  and letterdate between now() - interval '6 month' and now();

SELECT NULL AS "Number of times 'multiple employees' has been selected for publishing a letter - NA";
select count(*) from letter_letters where ispublished is True and mailother is True  and letterdate between now() - interval '6 month' and now();
 










SELECT NULL AS "Custom Fields";
SELECT NULL AS "Count of users who have used custom fields - letter_letters.. customfiledjson not null .. for updated by";
select count(updatedby) from letter_template where customfieldjson is not null  and createdon between now() - interval '6 month' and now();

SELECT NULL AS "Count of custom fields grouped by type - same as above !!!! last";

SELECT NULL AS "Number of times custom fields have been used in letter templates - letter_template.. customfieldjson";
select count(*) from letter_template where customfieldjson is not null and createdon between now() - interval '6 month' and now();


SELECT NULL AS "Number of times a letter has been generated containing custom fields - letter_letters.. customfiledjson not null";
select count(*) from letter_letters where customdatajson is not null  and letterdate between now() - interval '6 month' and now();

SELECT NULL AS "List of all custom fields used in templates - letter_template customfieldjson !!!! last";












 

SELECT NULL AS "Letter Options";

SELECT NULL AS "Count of users who have header defined - tblauditlog";
select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and code like '%letter_options_add%' and additionalinfo like '%Letter option added header data%' and auditdate between now() - interval '6 month' and now()) t;

SELECT NULL AS "Count of users who have footer defined - tblauditlog";
select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and code like '%letter_options_add%' and additionalinfo like '%Letter option added footer data%' and auditdate between now() - interval '6 month' and now()) t;

SELECT NULL AS "Count of users who have added a new authorized signatory - letter_authorizedsignatory -NA ";