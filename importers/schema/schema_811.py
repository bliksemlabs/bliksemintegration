schema = """
CREATE TEMPORARY TABLE "dest" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"                INTEGER    NOT NULL,
	"implicit"               CHAR(1)       NOT NULL,
	"dataownercode"          VARCHAR(10)   NOT NULL,
	"destcode"               VARCHAR(10)   NOT NULL,
	"destnamefull"           VARCHAR(50)   NOT NULL,
	"destnamemain"           VARCHAR(24)   NOT NULL,
	"destnamedetail"         VARCHAR(24),
	"relevantdestnamedetail" VARCHAR(5),
	PRIMARY KEY ("dataownercode", "destcode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "line" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"            INTEGER    NOT NULL,
	"implicit"           CHAR(1)       NOT NULL,
	"dataownercode"      VARCHAR(10)   NOT NULL,
	"lineplanningnumber" VARCHAR(10)   NOT NULL,
	"linepublicnumber"   VARCHAR(4)    NOT NULL,
	"linename"           VARCHAR(50)   NOT NULL,
	"linevetagnumber"    DECIMAL(3)    NOT NULL,
	"description"        VARCHAR(255),
	"transporttype"        VARCHAR(5),
	PRIMARY KEY ("dataownercode", "lineplanningnumber")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "conarea" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"            INTEGER    NOT NULL,
	"implicit"           CHAR(1)       NOT NULL,
	"dataownercode"      VARCHAR(10)   NOT NULL,
	"concessionareacode" VARCHAR(10)   NOT NULL,
	"description"        VARCHAR(255)  NOT NULL,
	PRIMARY KEY ("dataownercode", "concessionareacode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "confinrel" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"            INTEGER    NOT NULL,
	"implicit"           CHAR(1)       NOT NULL,
	"dataownercode"      VARCHAR(10)   NOT NULL,
	"confinrelcode"      VARCHAR(10)   NOT NULL,
	"concessionareacode" VARCHAR(10)   NOT NULL,
	"financercode"       VARCHAR(10),
	PRIMARY KEY ("dataownercode", "confinrelcode"),
	FOREIGN KEY ("dataownercode", "concessionareacode") REFERENCES "conarea" ("dataownercode", "concessionareacode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "usrstar" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"                 INTEGER    NOT NULL,
	"implicit"                CHAR(1)       NOT NULL,
	"dataownercode"           VARCHAR(10)   NOT NULL,
	"userstopareacode"        VARCHAR(10)   NOT NULL,
	"name"                    VARCHAR(50)   NOT NULL,
	"town"                    VARCHAR(50)   NOT NULL,
	"roadsideeqdataownercode" VARCHAR(10),
	"roadsideequnitnumber"    DECIMAL(5),
	"description"             VARCHAR(255),
	 PRIMARY KEY ("dataownercode", "userstopareacode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "usrstop" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"                 INTEGER    NOT NULL,
	"implicit"                CHAR(1)       NOT NULL,
	"dataownercode"           VARCHAR(10)   NOT NULL,
	"userstopcode"            VARCHAR(10)   NOT NULL,
	"timingpointcode"         VARCHAR(10),
	"getin"                   BOOLEAN       NOT NULL,
	"getout"                  BOOLEAN       NOT NULL,
	"deprecated"              CHAR(1),
	"name"                    VARCHAR(50)   NOT NULL,
	"town"                    VARCHAR(50)   NOT NULL,
	"userstopareacode"        VARCHAR(10),
	"stopsidecode"            VARCHAR(10),
	"roadsideeqdataownercode" VARCHAR(10),
	"roadsideequnitnumber"    DECIMAL(5),
	"minimalstoptime"         DECIMAL(5)    NOT NULL,
	"stopsidelength"          DECIMAL(3),
	"description"             VARCHAR(255),
	"userstoptype"            VARCHAR(10),
	PRIMARY KEY ("dataownercode", "userstopcode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "point" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"              INTEGER    NOT NULL,
	"implicit"             CHAR(1)       NOT NULL,
	"dataownercode"        VARCHAR(10)   NOT NULL,
	"pointcode"            VARCHAR(20)   NOT NULL,
	"validfrom"            DATE          NOT NULL,
	"pointtype"            VARCHAR(10)   NOT NULL,
	"coordinatesystemtype" VARCHAR(10)   NOT NULL,
	"locationx_ew"         DECIMAL(10)   NOT NULL,
	"locationy_ns"         DECIMAL(10)   NOT NULL,
	"locationz"            DECIMAL(3),
	"description"          VARCHAR(255),
	PRIMARY KEY ("dataownercode", "pointcode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "tili" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"           INTEGER    NOT NULL,
	"implicit"          CHAR(1)       NOT NULL,
	"dataownercode"     VARCHAR(10)   NOT NULL,
	"userstopcodebegin" VARCHAR(10)   NOT NULL,
	"userstopcodeend"   VARCHAR(10)   NOT NULL,
	"minimaldrivetime"  DECIMAL(5),
	"description"       VARCHAR(255),
	PRIMARY KEY ("dataownercode", "userstopcodebegin", "userstopcodeend"),
	FOREIGN KEY ("dataownercode", "userstopcodeend") REFERENCES "usrstop" ("dataownercode", "userstopcode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "link" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"           INTEGER    NOT NULL,
	"implicit"          CHAR(1)       NOT NULL,
	"dataownercode"     VARCHAR(10)   NOT NULL,
	"userstopcodebegin" VARCHAR(10)   NOT NULL,
	"userstopcodeend"   VARCHAR(10)   NOT NULL,
	"validfrom"         DATE          NOT NULL,
	"distance"          DECIMAL(6)    NOT NULL,
	"description"       VARCHAR(255),
	"transporttype"        VARCHAR(5),
	PRIMARY KEY ("dataownercode", "userstopcodebegin", "userstopcodeend", "validfrom", "transporttype"),
	FOREIGN KEY ("dataownercode", "userstopcodebegin", "userstopcodeend") REFERENCES "tili" ("dataownercode", "userstopcodebegin", 
"userstopcodeend")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "pool" (
	"tablename"         VARCHAR(10)   NOT NULL,
	version INTEGER NOT NULL, 
	implicit CHAR(1) NOT NULL, 
	dataownercode VARCHAR(10) NOT NULL,
	userStopCodeBegin VARCHAR(10) NOT NULL,
	UserStopCodeEnd VARCHAR(10) NOT NULL,
	LinkValidFrom DATE NOT NULL,
	PointDataOwnerCode VARCHAR(10) NOT NULL,
	PointCode VARCHAR(20) NOT NULL,
	DistanceSinceStartOfLink NUMERIC(5) NOT NULL,
	SegmentSpeed DECIMAL(4),
	LocalPointSpeed DECIMAL(4),
	Description VARCHAR(255),
	"transporttype"        VARCHAR(5),
	PRIMARY KEY (Version,DataOwnerCode, UserStopCodeBegin, UserStopCodeEnd, LinkValidFrom, PointDataOwnerCode, PointCode, TransportType),
	FOREIGN KEY (DataOwnerCode, UserStopCodeBegin, UserStopCodeEnd, LinkValidFrom, TransportType) REFERENCES link 
(DataOwnerCode,UserStopCodeBegin, UserStopCodeEnd, ValidFrom, TransportType),
        FOREIGN KEY (PointDataOwnerCode, PointCode) REFERENCES point (DataOwnerCode, PointCode)
) ON COMMIT DROP;

CREATE TEMPORARY TABLE "jopa" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"            INTEGER    NOT NULL,
	"implicit"           CHAR(1)       NOT NULL,
	"dataownercode"      VARCHAR(10)   NOT NULL,
	"lineplanningnumber" VARCHAR(10)   NOT NULL,
	"journeypatterncode" VARCHAR(10)   NOT NULL,
	"journeypatterntype" VARCHAR(10)   NOT NULL,
	"direction"          int4    NOT NULL,
	"description"        VARCHAR(255),
	PRIMARY KEY ("dataownercode", "lineplanningnumber", "journeypatterncode"),
	FOREIGN KEY ("dataownercode", "lineplanningnumber") REFERENCES "line" ("dataownercode", "lineplanningnumber")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "jopatili" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"            INTEGER    NOT NULL,
	"implicit"           CHAR(1)       NOT NULL,
	"dataownercode"      VARCHAR(10)   NOT NULL,
	"lineplanningnumber" VARCHAR(10)   NOT NULL,
	"journeypatterncode" VARCHAR(10)   NOT NULL,
	"timinglinkorder"    DECIMAL(3)    NOT NULL,
	"userstopcodebegin"  VARCHAR(10)   NOT NULL,
	"userstopcodeend"    VARCHAR(10)   NOT NULL,
	"confinrelcode"      VARCHAR(10)   NOT NULL,
	"destcode"           VARCHAR(10)   NOT NULL,
	"deprecated"         VARCHAR(10),
	"istimingstop"       BOOLEAN        NOT NULL,
	"displaypublicline"  VARCHAR(4),
	"productformulatype"    DECIMAL(4),
	PRIMARY KEY ("dataownercode", "lineplanningnumber", "journeypatterncode", "timinglinkorder"),
	FOREIGN KEY ("dataownercode", "confinrelcode") REFERENCES "confinrel" ("dataownercode", "confinrelcode"),
	FOREIGN KEY ("dataownercode", "destcode") REFERENCES "dest" ("dataownercode", "destcode"),
	FOREIGN KEY ("dataownercode", "lineplanningnumber", "journeypatterncode") REFERENCES "jopa" ("dataownercode", "lineplanningnumber", 
"journeypatterncode"),
	FOREIGN KEY ("dataownercode", "userstopcodebegin", "userstopcodeend") REFERENCES "tili" ("dataownercode", "userstopcodebegin", 
"userstopcodeend")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "orun" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"                INTEGER    NOT NULL,
	"implicit"               CHAR(1)       NOT NULL,
	"dataownercode"          VARCHAR(10)   NOT NULL,
	"organizationalunitcode" VARCHAR(10)   NOT NULL,
	"name"                   VARCHAR(50)   NOT NULL,
	"organizationalunittype" VARCHAR(10)   NOT NULL,
	"description"            VARCHAR(255),
	PRIMARY KEY ("dataownercode", "organizationalunitcode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "orunorun" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"                      INTEGER    NOT NULL,
	"implicit"                     CHAR(1)       NOT NULL,
	"dataownercode"                VARCHAR(10)   NOT NULL,
	"organizationalunitcodeparent" VARCHAR(10)   NOT NULL,
	"organizationalunitcodechild"  VARCHAR(10)   NOT NULL,
	"validfrom"                    DATE          NOT NULL,
	PRIMARY KEY ("dataownercode", "organizationalunitcodeparent", "organizationalunitcodechild", "validfrom"),
	FOREIGN KEY ("dataownercode", "organizationalunitcodechild") REFERENCES "orun" ("dataownercode", "organizationalunitcode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "specday" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"         INTEGER    NOT NULL,
	"implicit"        CHAR(1)       NOT NULL,
	"dataownercode"   VARCHAR(10)   NOT NULL,
	"specificdaycode" VARCHAR(10)   NOT NULL,
	"name"            VARCHAR(50)   NOT NULL,
	"description"     VARCHAR(255),
	PRIMARY KEY ("dataownercode", "specificdaycode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "pegr" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"         INTEGER    NOT NULL,
	"implicit"        CHAR(1)       NOT NULL,
	"dataownercode"   VARCHAR(10)   NOT NULL,
	"periodgroupcode" VARCHAR(10)   NOT NULL,
	"description"     VARCHAR(255),
	PRIMARY KEY ("dataownercode", "periodgroupcode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "excopday" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"                INTEGER    NOT NULL,
	"implicit"               CHAR(1)       NOT NULL,
	"dataownercode"          VARCHAR(10)   NOT NULL,
	"organizationalunitcode" VARCHAR(10)   NOT NULL,
	"validdate"              TIMESTAMP     NOT NULL,
	"daytypeason"            DECIMAL(7)    NOT NULL,
	"specificdaycode"        VARCHAR(10)   NOT NULL,
	"periodgroupcode"        VARCHAR(10),
	"description"            VARCHAR(255),
	PRIMARY KEY ("dataownercode", "organizationalunitcode", "validdate"),
	FOREIGN KEY ("dataownercode", "periodgroupcode") REFERENCES "pegr" ("dataownercode", "periodgroupcode"),
	FOREIGN KEY ("dataownercode", "specificdaycode") REFERENCES "specday" ("dataownercode", "specificdaycode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "pegrval" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"                INTEGER    NOT NULL,
	"implicit"               CHAR(1)       NOT NULL,
	"dataownercode"          VARCHAR(10)   NOT NULL,
	"organizationalunitcode" VARCHAR(10)   NOT NULL,
	"periodgroupcode"        VARCHAR(10)   NOT NULL,
	"validfrom"              DATE          NOT NULL,
	"validthru"              DATE          NOT NULL,
	PRIMARY KEY ("dataownercode", "organizationalunitcode", "periodgroupcode", "validfrom"),
	FOREIGN KEY ("dataownercode", "organizationalunitcode") REFERENCES "orun" ("dataownercode", "organizationalunitcode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "tive" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"                INTEGER    NOT NULL,
	"implicit"               CHAR(1)       NOT NULL,
	"dataownercode"          VARCHAR(10)   NOT NULL,
	"organizationalunitcode" VARCHAR(10)   NOT NULL,
	"timetableversioncode"   VARCHAR(10)   NOT NULL,
	"periodgroupcode"        VARCHAR(10)   NOT NULL,
	"specificdaycode"        VARCHAR(10)   NOT NULL,
	"validfrom"              DATE          NOT NULL,
	"timetableversiontype"   VARCHAR(10)   NOT NULL,
	"validthru"              DATE,
	"description"            VARCHAR(255),
	PRIMARY KEY ("dataownercode", "organizationalunitcode", "timetableversioncode", "periodgroupcode", "specificdaycode"),
	FOREIGN KEY ("dataownercode", "organizationalunitcode") REFERENCES "orun" ("dataownercode", "organizationalunitcode"),
	FOREIGN KEY ("dataownercode", "periodgroupcode") REFERENCES "pegr" ("dataownercode", "periodgroupcode"),
	FOREIGN KEY ("dataownercode", "specificdaycode") REFERENCES "specday" ("dataownercode", "specificdaycode")
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "timdemgrp" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"             INTEGER    NOT NULL,
	"implicit"            CHAR(1)       NOT NULL,
	"dataownercode"       VARCHAR(10)   NOT NULL,
	"lineplanningnumber"  VARCHAR(10)   NOT NULL,
	"journeypatterncode"  VARCHAR(10)   NOT NULL,
	"timedemandgroupcode" VARCHAR(10)   NOT NULL,
	PRIMARY KEY ("dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode"),
	FOREIGN KEY ("dataownercode", "lineplanningnumber", "journeypatterncode") REFERENCES "jopa" ("dataownercode", "lineplanningnumber", 
"journeypatterncode") ON UPDATE CASCADE
) ON COMMIT DROP;
CREATE TEMPORARY TABLE "timdemrnt" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"             INTEGER    NOT NULL,
	"implicit"            CHAR(1)       NOT NULL,
	"dataownercode"       VARCHAR(10)   NOT NULL,
	"lineplanningnumber"  VARCHAR(10)   NOT NULL,
	"journeypatterncode"  VARCHAR(10)   NOT NULL,
	"timedemandgroupcode" VARCHAR(10)   NOT NULL,
	"timinglinkorder"     DECIMAL(3)    NOT NULL,
	"userstopcodebegin"   VARCHAR(10)   NOT NULL,
	"userstopcodeend"     VARCHAR(10)   NOT NULL,
	"totaldrivetime"      DECIMAL(5)    NOT NULL,
	"drivetime"           DECIMAL(5)    NOT NULL,
	"expecteddelay"       DECIMAL(5),
	"layovertime"         DECIMAL(5),
	"stopwaittime"        DECIMAL(5)    NOT NULL,
	"minimumstoptime"     DECIMAL(5),
	PRIMARY KEY ("dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode", "timinglinkorder"),
	FOREIGN KEY ("dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode") REFERENCES "timdemgrp" 
("dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode"),
	FOREIGN KEY ("dataownercode", "lineplanningnumber", "journeypatterncode", "timinglinkorder") REFERENCES "jopatili" ("dataownercode", 
"lineplanningnumber", "journeypatterncode", "timinglinkorder")
) ON COMMIT DROP;

CREATE TEMPORARY TABLE "pujo" (
	"tablename"         VARCHAR(10)   NOT NULL,
	"version"                INTEGER    NOT NULL,
	"implicit"               CHAR(1)       NOT NULL,
	"dataownercode"          VARCHAR(10)   NOT NULL,
	"organizationalunitcode" VARCHAR(10)   NOT NULL,
	"timetableversioncode"   VARCHAR(10)   NOT NULL,
	"periodgroupcode"        VARCHAR(10)   NOT NULL,
	"specificdaycode"        VARCHAR(10)   NOT NULL,
	"daytype"                CHAR(7)       NOT NULL,
	"lineplanningnumber"     VARCHAR(10)   NOT NULL,
	"journeynumber"          DECIMAL(6)    NOT NULL,
	"timedemandgroupcode"    VARCHAR(10)   NOT NULL,
	"journeypatterncode"     VARCHAR(10)   NOT NULL,
	"departuretime"          CHAR(8)       NOT NULL,
	"wheelchairaccessible"   VARCHAR(13)   NOT NULL,
	"dataownerisoperator"    BOOLEAN       NOT NULL,
	 PRIMARY KEY ("dataownercode", "timetableversioncode", "organizationalunitcode", "periodgroupcode", "specificdaycode", "daytype", 
"lineplanningnumber", "journeynumber"),
	 FOREIGN KEY ("dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode") REFERENCES "timdemgrp" 
("dataownercode", "lineplanningnumber", "journeypatterncode", "timedemandgroupcode"),
	 FOREIGN KEY ("dataownercode", "organizationalunitcode", "timetableversioncode", "periodgroupcode", "specificdaycode") REFERENCES 
"tive" ("dataownercode", "organizationalunitcode", "timetableversioncode", "periodgroupcode", "specificdaycode")
) ON COMMIT DROP;

CREATE TEMPORARY TABLE schedvers (
    "tablename"         VARCHAR(10)   NOT NULL,
    version INTEGER NOT NULL,
    implicit CHAR(1) NOT NULL,
    dataownercode character varying(10) NOT NULL,
    organizationalunitcode character varying(10) NOT NULL,
    schedulecode character varying(10) NOT NULL,
    scheduletypecode character varying(10) NOT NULL,
    validfrom date NOT NULL,
    validthru date,
    description character varying(255),
    PRIMARY KEY (dataownercode, organizationalunitcode, schedulecode, scheduletypecode),
    FOREIGN KEY (dataownercode, organizationalunitcode) REFERENCES orun (dataownercode, organizationalunitcode) ON UPDATE CASCADE
) ON COMMIT DROP;

CREATE TEMPORARY TABLE operday (
    "tablename"         VARCHAR(10)   NOT NULL,
    version INTEGER NOT NULL,
    implicit CHAR(1) NOT NULL,
    dataownercode character varying(10) NOT NULL,
    organizationalunitcode character varying(10) NOT NULL,
    schedulecode character varying(10) NOT NULL,
    scheduletypecode character varying(10) NOT NULL,
    validdate date NOT NULL,
    description character varying(255),
    PRIMARY KEY (dataownercode, organizationalunitcode, schedulecode, scheduletypecode, validdate),
    FOREIGN KEY (dataownercode, organizationalunitcode, schedulecode, scheduletypecode) REFERENCES schedvers (dataownercode, 
organizationalunitcode, schedulecode, scheduletypecode)
) ON COMMIT DROP;

CREATE TEMPORARY TABLE pujopass (
    "tablename"         VARCHAR(10)   NOT NULL,
    version INTEGER NOT NULL,
    implicit CHAR(1) NOT NULL,
    dataownercode character varying(10) NOT NULL,
    organizationalunitcode character varying(10) NOT NULL,
    schedulecode character varying(10) NOT NULL,
    scheduletypecode character varying(10) NOT NULL,
    lineplanningnumber character varying(10) NOT NULL,
    journeynumber numeric(6,0) NOT NULL,
    stoporder numeric(4,0) NOT NULL,
    journeypatterncode character varying(10) NOT NULL,
    userstopcode character varying(10) NOT NULL,
    targetarrivaltime char(8),
    targetdeparturetime char(8),
    wheelchairaccessible VARCHAR(13),
    dataownerisoperator boolean NOT NULL,
    PRIMARY KEY (dataownercode, organizationalunitcode, schedulecode, scheduletypecode, lineplanningnumber, journeynumber, stoporder),
    FOREIGN KEY (dataownercode, organizationalunitcode, schedulecode, scheduletypecode) REFERENCES schedvers (dataownercode, 
organizationalunitcode, schedulecode, scheduletypecode),
    FOREIGN KEY (dataownercode, userstopcode) REFERENCES usrstop (dataownercode, userstopcode),
    FOREIGN KEY (dataownercode, lineplanningnumber, journeypatterncode) REFERENCES jopa (dataownercode, lineplanningnumber, journeypatterncode)
) ON COMMIT DROP;

create view patternpass as (
SELECT
j.version,j.dataownercode,lineplanningnumber,journeypatterncode,
cast(timinglinkorder as integer) as stoporder,
userstopcodebegin as userstopcode
FROM jopatili as j
UNION (
SELECT DISTINCT ON (j.version,j.dataownercode,lineplanningnumber,journeypatterncode)
j.version,j.dataownercode,lineplanningnumber,journeypatterncode,
cast(timinglinkorder+1 as integer) as stoporder,
userstopcodeend as userstopcode
FROM jopatili as j
ORDER BY j.version,j.dataownercode,lineplanningnumber,journeypatterncode,timinglinkorder DESC)
ORDER BY dataownercode,lineplanningnumber,journeypatterncode,stoporder);
"""
