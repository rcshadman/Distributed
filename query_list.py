# get_schemas = "SELECT table_schema FROM information_schema.tables WHERE table_name like 'letter_letters';"
def set_schema_query(schema_name):
    return "set search_path to %s;".format(schema_name)

extract_query = {


    0: {
        "description": "Count of templates created by user [user defined template]",
        "query": 'select count(*) from letter_template;',
        "result_format":[{"data_type":"long","action":"sum"}],
        "result": {}
    },
    1: {"description":
        "Count of templates added by user from template gallery NA",
        "query": 'SELECT NULL AS "Empty";',
        "result_format":[{"data_type":"long","action":"sum"}],
        "result": {}
        },

    2: {"description":
        "Number of times a template has been edited [Unique by templates]",
        "query": "select additionalinfo, count(additionalinfo) from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and additionalinfo like '%Update%' and code like '%letter_template_add%'  group by additionalinfo;",
        "result_format":[{"data_type":"string","action":"groupby"},{"data_type":"long","action":"sum"}],
        "result": {}
        },

    3: {"description":
        "Count of users who have setup letter templates (Created a new template)",
        "query": "select distinct count(userid) from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and additionalinfo like '%Added%' and code like '%letter_template_add%'  group by userid, additionalinfo;",
        "result_format":[{"data_type":"long","action":"sum"}],
        "result": {}
        },

    4: {"description":# updated by is id, change to names.. count could be adding id's of users
        "Number of templates which has been marked as Disabled along with count of users",
        # "query": "select updatedby,count(isenabled)  from letter_template where isenabled is False  group by updatedby;",
        "query": "select count(t.updatedby) as updatedby_count, enabled_count from (select updatedby , count(isenabled) as enabled_count from letter_template,mailtemplate, mailtemplatecategory where templatecategory = mailtemplatecategory.cid and mailtemplateid = mailtemplate.cid  and mailtemplatecategory.code in ('letter') group by updatedby) t group by t.enabled_count;",
        "result_format":[{"data_type":"long","action":"sum"},{"data_type":"long","action":"sum"}],
        "result": {}
        },

    5: {"description": "Count of users who have created a new serial number NA",
        "query": "select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and code like '%sequence_config_add%' ) t;",
        "result_format":[{"data_type":"long","action":"sum"}],
        "result": {}
        },

    6: {"description":
        "Count of users who have downloaded a template NA",
        "query": 'SELECT NULL AS "Empty";',
        "result_format":[{"data_type":"long","action":"sum"}],
        "result": {}
        },

    7: {"description":
        "Count of users who have configured a mail template ie dont have None as mail template along with number of templates NA",
        "query": 'SELECT NULL AS "Empty";',
        "result_format":[{"data_type":"long","action":"sum"}],
        "result": {}
        },

    8: {"description":
        "Count of users who have configured None as the mail template along with Number of templates",
        "query": "select count( updatedby), count(*) from letter_template,mailtemplate, mailtemplatecategory where templatecategory = mailtemplatecategory.cid and mailtemplateid = mailtemplate.cid  and mailtemplatecategory.code in ('letter')   group by updatedby;",
        "result_format":[{"data_type":"long","action":"sum"},{"data_type":"long","action":"sum"}],
        "result": {}
        },

    9: {"description":
        "Count of letters generated  [month wise breakdown] - letter_ letters",
        "query": "select date_trunc( 'month', letterdate ),count(*) from letter_letters where ispublished is True  group by date_trunc( 'month', letterdate );",
        "result_format":[{"data_type":"date","action":"groupby"},{"data_type":"long","action":"sum"}],
        "result": {}
        },

    10: {"description":
         "Count of users who have accessed letters module - tblactiveservice",
         "query": 'SELECT NULL AS "Empty";',
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    11: {"description":
         "Count of letters generated without authorized signatory - letter_ letters",
         "query": "select count(*) from letter_letters where authorizedsignatory is NULL ;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    12: {"description":
         "Count of users who downloaded a letter - tblauditlog",
         "query": "select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and additionalinfo like '%download%' and additionalinfo like '%letter%' ) t;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    13: {"description":
         "Count of users who published the letter to the employee portal - letter_letters : published field",
         "query": "select count(*) from letter_letters where ispublished is True ;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    14: {"description":
         "Count of users who published the letter via email - isemailed .. like above",
         "query": "select count(*) from letter_letters where ispublished is True and sendemail is True  ;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    15: {"description":
         "Number of times 'multiple employees' has been selected for publishing a letter - NA",
         "query": "select count(*) from letter_letters where ispublished is True and mailother is True;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },


    16: {"description":
         "Count of users who have used custom fields - letter_letters.. customfiledjson not null .. for updated by",
         "query": "select count(updatedby) from letter_template where customfieldjson is not null  ;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    17: {"description":
         "Count of custom fields grouped by type - same as above !!!! last",
         "query": 'SELECT NULL AS "Empty";',
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    18: {"description":
         "Number of times custom fields have been used in letter templates - letter_template.. customfieldjson",
         "query": "select count(*) from letter_template where customfieldjson is not null ;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },


    19: {"description":
         "Number of times a letter has been generated containing custom fields - letter_letters.. customfiledjson not null",
         "query": "select count(*) from letter_letters where customdatajson is not null  ;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    19: {"description":
         "List of all custom fields used in templates - letter_template customfieldjson !!!! last",
         "query": 'SELECT NULL AS "Empty";',
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    20: {"description":
         "Count of users who have header defined - tblauditlog",
         "query": "select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and code like '%letter_options_add%' and additionalinfo like '%Letter option added header data%' ) t;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    21: {"description":
         "Count of users who have footer defined - tblauditlog",
         "query": "select count(*) from (select userid from tblauditlog, tblauditcategory where auditcategory = tblauditcategory.cid and code like '%letter_options_add%' and additionalinfo like '%Letter option added footer data%' ) t;",
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },

    22: {"description":
         "Count of users who have added a new authorized signatory - letter_authorizedsignatory -NA",
         "query": 'SELECT NULL AS "Empty";',
         "result_format":[{"data_type":"long","action":"sum"}],
         "result": {}
         },




}
