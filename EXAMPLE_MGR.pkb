CREATE OR REPLACE PACKAGE BODY EXAMPLE_MGR AS
/*
    This package is designed to manage ...
    
    Created by: Ivaylo Shalev
    Created on: 01 January 2014
    Last updated on:
 
*/

---------------------------
-- Help functions
---------------------------
  FUNCTION TABLE_EXIST(pTableName IN VARCHAR2) RETURN NUMBER -- 0 - exist, -1 doesn't exist
  IS
    tbl_count   number;
  BEGIN
    tbl_count := 0;
    select count(*) into tbl_count from all_tables where table_name = pTableName;
    
    IF tbl_count = 0
    THEN
      LOG_ERROR(pTableName,'Table "'||pTableName||'" does NOT exist!');
      RETURN -1;
    ELSE
      RETURN 0;
    END IF;
  END TABLE_EXIST;
  
  FUNCTION VIEW_EXIST(pViewName IN VARCHAR2) RETURN NUMBER -- 0 - exist, -1 doesn't exist
  IS
    vw_count   number;
  BEGIN
    vw_count := 0;
    select count(*) into vw_count from all_views where view_name = pViewName;
    
    IF vw_count = 0
    THEN
      LOG_ERROR(pViewName,'View "'||pViewName||'" does NOT exist!');
      RETURN -1;
    ELSE
      RETURN 0;
    END IF;
  END VIEW_EXIST;
  
  FUNCTION IS_RUNNING(pObjectName IN VARCHAR2, pFinishStr IN VARCHAR2) RETURN NUMBER
  IS
    vLastID NUMBER;
 vResult NUMBER;
  BEGIN
    
 SELECT MAX(ID) INTO vLastID
 FROM EXAMPLE_MGR_LOG
 WHERE OBJECT_NAME = pObjectName;
 
 SELECT 1 - COUNT(*) INTO vResult
 FROM EXAMPLE_MGR_LOG
 WHERE ID = vLastID
   AND (
     (TYPE = 'INFO' AND MESSAGE LIKE pFinishStr)
  OR
  (TYPE = 'ERROR')
   );
 
 RETURN vResult;
 
  END IS_RUNNING;

--------------------
-- LOG procedures
--------------------
  PROCEDURE WRITE_LOG(pLogType IN VARCHAR2, pObjectName IN VARCHAR2, pMessage IN VARCHAR2) AS
  BEGIN
    INSERT INTO EXAMPLE_MGR_LOG (TYPE, OBJECT_NAME, MESSAGE)
    VALUES (pLogType, pObjectName, pMessage);
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END WRITE_LOG;
  
  PROCEDURE LOG_INFO(pObjectName IN VARCHAR2, pMessage IN VARCHAR2) AS
  BEGIN
    WRITE_LOG('INFO',pObjectName, pMessage);
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END LOG_INFO;
  
  PROCEDURE LOG_ERROR(pObjectName IN VARCHAR2, pMessage IN VARCHAR2) AS
  BEGIN
    WRITE_LOG('ERROR',pObjectName, pMessage);
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END LOG_ERROR;

---------------------------
-- PARTITION procedures and functions
---------------------------
  PROCEDURE P_UPDATE_PARTITIONS(pObjectName IN VARCHAR2,
                             pTableName IN VARCHAR2,
                             pPeriodDaysPast IN NUMBER,
                             pPeriodDaysFuture IN NUMBER) AS
  vRes NUMBER;
  BEGIN
      vRes := UPDATE_PARTITIONS(pObjectName,
                              pTableName,
                              pPeriodDaysPast,
                              pPeriodDaysFuture);
  END P_UPDATE_PARTITIONS;
  
           
  FUNCTION UPDATE_PARTITIONS(pObjectName IN VARCHAR2,
                              pTableName IN VARCHAR2,
                              pPeriodDaysPast IN NUMBER,
                              pPeriodDaysFuture IN NUMBER) RETURN NUMBER
  IS
  v_SQL VARCHAR2(2000);
  BEGIN
    LOG_INFO(pObjectName, 'Updating partitions...');
    
    IF BUILD_PARTITIONS(pTableName, SYSDATE + pPeriodDaysFuture) = -1
        THEN
            RETURN -1;
    END IF;
    
    IF DROP_PARTITIONS(pTableName, SYSDATE - pPeriodDaysPast) = -1
        THEN
            RETURN -1;
    END IF;
    
 LOG_INFO(pObjectName, 'Finished updating partitions...');
    RETURN 0;
  EXCEPTION
    WHEN OTHERS THEN
      LOG_ERROR(pObjectName,'Error at UPDATE_PARTITIONS() for table "'||pTableName||
                          '", for period of days past = '||pPeriodDaysPast||
                          ', future = '||pPeriodDaysFuture||' :'||chr(10)||
                          'SQL=>'||v_SQL||'<='||chr(10)||
                          'Error Message: '||SUBSTR(SQLERRM,1,4000));
      ROLLBACK;
      RETURN -1;
  END UPDATE_PARTITIONS;
  
  FUNCTION DROP_PARTITIONS(pTableName IN VARCHAR2, pMinPartDate IN DATE) RETURN NUMBER
  IS
    sql_stmt    varchar2(1000);
    day_count   number;
  BEGIN
  
    IF TABLE_EXIST(pTableName) = -1 THEN
      RETURN -1;
    END IF;
    
    FOR j IN (SELECT table_name, min(partition_name) v_partition_name
            FROM user_tab_partitions p
            WHERE table_name = pTableName GROUP BY table_name)
    LOOP
     day_count:=0;
     while to_date(j.v_partition_name,'YYYY_MM_DD')+day_count < pMinPartDate
     loop

        sql_stmt := 'ALTER TABLE ' || j.table_name  ||
                 ' DROP PARTITION "' ||
                    to_char(to_date(j.v_partition_name,'YYYY_MM_DD')+day_count,'YYYY_MM_DD') ||
                    '"';
        
        Execute Immediate sql_stmt;
        day_count:=day_count+1;  
     END LOOP;

    END LOOP;

    COMMIT;
    
    RETURN 0;
  EXCEPTION
    WHEN OTHERS THEN
      LOG_ERROR(pTableName,'Error at DROP_PARTITIONS() for table "'||pTableName||
                          '", with min partition date = "'||pMinPartDate||'" :'||chr(10)||
                          'SQL=>'||sql_stmt||'<='||chr(10)||
                          'Error Message: '||SUBSTR(SQLERRM,1,4000));
      ROLLBACK;
      RETURN -1;
  END DROP_PARTITIONS;
  
  FUNCTION BUILD_PARTITIONS(pTableName IN VARCHAR2, pMaxPartDate IN DATE) RETURN NUMBER
  IS
    sql_stmt    varchar2(1000);
    day_count   number;
  BEGIN
  
    IF TABLE_EXIST(pTableName) = -1 THEN
      RETURN -1;
    END IF;
    
    FOR j IN (SELECT table_name, max(partition_name) v_partition_name
              FROM user_tab_partitions p
              WHERE table_name = pTableName
                GROUP BY table_name)
    LOOP
      day_count:=1;      
      
      while to_date(j.v_partition_name,'YYYY_MM_DD')+day_count < pMaxPartDate
      loop
        sql_stmt := 'ALTER TABLE ' || j.table_name  ||
                    ' ADD PARTITION "' ||
                    to_char(to_date(j.v_partition_name,'YYYY_MM_DD')+day_count,'YYYY_MM_DD') ||
                    '" VALUES LESS THAN (TO_DATE(''' ||
                    to_char(to_date(j.v_partition_name,'YYYY_MM_DD')+day_count+1,'YYYY-MM-DD') ||
                    ' 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN'')) '||
                    'NOLOGGING '||
                    'TABLESPACE LARGE_TABLES_TS '||
                    'PCTFREE    10 ' ||
                    'INITRANS   1 ' ||
                    'MAXTRANS   255 ' ||
                    'STORAGE    (' ||
                    '      INITIAL          200K'||
                    '       BUFFER_POOL      DEFAULT' ||
                    '             )';

        Execute Immediate sql_stmt;

        day_count:=day_count+1;  
      END LOOP;
    END LOOP;
  
    COMMIT;
    
    RETURN 0;
  EXCEPTION
    WHEN OTHERS THEN
      LOG_ERROR(pTableName,'Error at BUILD_PARTITIONS() for table "'||pTableName||
                           '", with max partition date = "'||pMaxPartDate||'" :'||chr(10)||
                           'SQL=>'||sql_stmt||'<='||chr(10)||
                           'Error Message: '||SUBSTR(SQLERRM,1,4000));
      ROLLBACK;
      RETURN -1;
  END BUILD_PARTITIONS;
  
  
---------------------------
-- Custom procedures and functions
---------------------------
  
  
END EXAMPLE_MGR;
/