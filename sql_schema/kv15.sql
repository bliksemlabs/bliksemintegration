create schema kv15;

CREATE TABLE kv15.stopmessage (
    dataownercode character varying(10) NOT NULL,
    messagecodedate date NOT NULL,
    messagecodenumber numeric(4,0) NOT NULL,
    messagepriority character varying(10) NOT NULL,
    messagetype character varying(10) NOT NULL,
    messagedurationtype character varying(10) NOT NULL,
    messagestarttime timestamp without time zone,
    messageendtime timestamp without time zone,
    messagecontent character varying(1024),
    reasontype numeric(3,0),
    subreasontype character varying(10),
    reasoncontent character varying(255),
    effecttype numeric(3,0),
    subeffecttype character varying(10),
    effectcontent character varying(255),
    measuretype numeric(3,0),
    submeasuretype character varying(10),
    measurecontent character varying(255),
    advicetype numeric(3,0),
    subadvicetype character varying(10),
    advicecontent character varying(255),
    messagetimestamp timestamp without time zone NOT NULL,
    isdeleted boolean DEFAULT false NOT NULL
);

CREATE TABLE kv15.stopmessage_lineplanningnumber (
    dataownercode character varying(10) NOT NULL,
    messagecodedate date NOT NULL,
    messagecodenumber numeric(4,0) NOT NULL,
    lineplanningnumber character varying(10) NOT NULL
);

CREATE TABLE kv15.stopmessage_userstopcode (
    dataownercode character varying(10) NOT NULL,
    messagecodedate date NOT NULL,
    messagecodenumber numeric(4,0) NOT NULL,
    userstopcode character varying(10) NOT NULL
);
