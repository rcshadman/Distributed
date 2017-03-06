# get_schemas = "SELECT table_schema FROM information_schema.tables WHERE table_name like 'letter_letters';"
def set_schema_query(schema_name):
    return "set search_path to %s;".format(schema_name)

extract_query = {

    
    0:[
        "Count of templates created by user [user defined template]",
        'select count(*) from letter_template;'
        ]
    ,
    1:[
        "Count of templates added by user from template gallery NA",
        'SELECT NULL AS "Empty";'
    ],

    2:[
        "Number of times a template has been edited [Unique by templates]",
        "select additionalinfo, count(additionalinfo) from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and additionalinfo like '%Update%' and code like '%letter_template_add%'  group by additionalinfo;"
     ],

    3:[
        "Count of users who have setup letter templates (Created a new template)",
        "select distinct count(userid) from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and additionalinfo like '%Added%' and code like '%letter_template_add%'  group by userid, additionalinfo;"
     ],
    4:[
        "Number of templates which has been marked as Disabled along with count of users",
        "select updatedby,count(isenabled)  from letter_template where isenabled is False  group by updatedby;"
     ],

    5:[
        "Count of users who have created a new serial number NA",
        "select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and code like '%sequence_config_add%' ) t;"
     ],
    
    6:[
        "Count of users who have downloaded a template NA",
        'SELECT NULL AS "Empty";'
     ],

    7:[
        "Count of users who have configured a mail template ie dont have None as mail template along with number of templates NA",
        'SELECT NULL AS "Empty";'
     ],

    8:[
        "Count of users who have configured None as the mail template along with Number of templates",
        "select count( updatedby), count(*) from letter_template,mailtemplate, mailtemplatecategory where templatecategory = mailtemplatecategory.cid and mailtemplateid = mailtemplate.cid  and mailtemplatecategory.code in ('letter')   group by updatedby;"
     ],

    9:[
        "Count of letters generated  [month wise breakdown] - letter_ letters",
        "select count(*), date_trunc( 'month', letterdate ) from letter_letters where ispublished is True  group by date_trunc( 'month', letterdate );"
     ],

    10:[
        "Count of users who have accessed letters module - tblactiveservice",
        "SELECT NULL AS 'Empty';"
     ],

    11:[
        "Count of letters generated without authorized signatory - letter_ letters",
        "select count(*) from letter_letters where authorizedsignatory is NULL ;"
        ],

    12:[
        "Count of users who downloaded a letter - tblauditlog",
        "select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and additionalinfo like '%download%' and additionalinfo like '%letter%' ) t;"
        ],

    13:[
        "Count of users who published the letter to the employee portal - letter_letters : published field",
        "select count(*) from letter_letters where ispublished is True ;"
        ],

    14:[
        "Count of users who published the letter via email - isemailed .. like above",
        "select count(*) from letter_letters where ispublished is True and sendemail is True  ;"
        ],

    15:[
        "Number of times 'multiple employees' has been selected for publishing a letter - NA",
        "select count(*) from letter_letters where ispublished is True and mailother is True;"
        ],


    16:[
        "Count of users who have used custom fields - letter_letters.. customfiledjson not null .. for updated by",
        "select count(updatedby) from letter_template where customfieldjson is not null  ;"
        ],

    17:[
        "Count of custom fields grouped by type - same as above !!!! last",
        "SELECT NULL AS 'Empty';"
        ],

    18:[
        "Number of times custom fields have been used in letter templates - letter_template.. customfieldjson",
        "select count(*) from letter_template where customfieldjson is not null ;"
        ],


    19:[
        "Number of times a letter has been generated containing custom fields - letter_letters.. customfiledjson not null",
        "select count(*) from letter_letters where customdatajson is not null  ;"
        ],

    19:[
        "List of all custom fields used in templates - letter_template customfieldjson !!!! last",
        "SELECT NULL AS 'Empty';"
        ],

    20:[
        "Count of users who have header defined - tblauditlog",
        "select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and code like '%letter_options_add%' and additionalinfo like '%Letter option added header data%' ) t;"
        ],

    21:[
        "Count of users who have footer defined - tblauditlog",
        "select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and code like '%letter_options_add%' and additionalinfo like '%Letter option added footer data%' ) t;"
        ],

    22:[
        "Count of users who have added a new authorized signatory - letter_authorizedsignatory -NA",
        "SELECT NULL AS 'Empty';"
        ],




}
