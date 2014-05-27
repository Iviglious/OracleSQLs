CREATE OR REPLACE PACKAGE EXAMPLE_MGR AS
/*
    This package is designed to manage ...
    
    Created by: Ivaylo Shalev
    Created on: 01 January 2014
    Last updated on:
 
*/

---------------------------
-- Help functions
---------------------------
  FUNCTION TABLE_EXIST(pTableName IN VARCHAR2) RETURN NUMBER;
  FUNCTION VIEW_EXIST(pViewName IN VARCHAR2) RETURN NUMBER;
  FUNCTION IS_RUNNING(pObjectName IN VARCHAR2, pFinishStr IN VARCHAR2) RETURN NUMBER;
  
---------------------------
-- LOG procedures
---------------------------
  PROCEDURE WRITE_LOG(pLogType IN VARCHAR2, pObjectName IN VARCHAR2, pMessage IN VARCHAR2);
  PROCEDURE LOG_INFO(pObjectName IN VARCHAR2, pMessage IN VARCHAR2);
  PROCEDURE LOG_ERROR(pObjectName IN VARCHAR2, pMessage IN VARCHAR2);
  
---------------------------
-- PARTITION procedures
---------------------------
  PROCEDURE P_UPDATE_PARTITIONS(pObjectName IN VARCHAR2,
                              pTableName IN VARCHAR2,
                              pPeriodDaysPast IN NUMBER,
                              pPeriodDaysFuture IN NUMBER);
         
  FUNCTION UPDATE_PARTITIONS(pObjectName IN VARCHAR2,
                              pTableName IN VARCHAR2,
                              pPeriodDaysPast IN NUMBER,
                              pPeriodDaysFuture IN NUMBER) RETURN NUMBER;
  FUNCTION DROP_PARTITIONS(pTableName IN VARCHAR2, pMinPartDate IN DATE) RETURN NUMBER;
  FUNCTION BUILD_PARTITIONS(pTableName IN VARCHAR2, pMaxPartDate IN DATE) RETURN NUMBER;

---------------------------
-- Custom procedures and functions
---------------------------
  
END EXAMPLE_MGR;
/