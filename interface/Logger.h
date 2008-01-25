#ifndef CONDCORE_DBOUTPUTSERVICE_LOGGER_H
#define CONDCORE_DBOUTPUTSERVICE_LOGGER_H

#include <string>
//#include <iostream>
//
// Package:    
// Class  :     
// 
/**\class 
*/
//
// Author:      Zhen Xie
//
#include "LogEntry.h"
#include "UserLogInfo.h"
namespace coral{
  //class ISchema;  
  class IQuery;
}
namespace cond{
  namespace service{
    class UserLogInfo;
  }
  class CoralTransaction;
  class Connection;
  class SequenceManager;
  class Logger{
  public:
    explicit Logger(Connection* connectionHandle);
    ~Logger();
    bool getWriteLock() throw ();
    bool releaseWriteLock() throw ();
    //NB. for oracle only schema owner can do this 
    void createLogDBIfNonExist();
    //
    //the current local time will be registered as execution time
    //
    void logOperationNow(const std::string& containerName,
			 const cond::service::UserLogInfo& userlogInfo,
			 const std::string& destDB,
			 const std::string& payloadName,
			 const std::string& payloadToken
			 );
    //
    //the current local time will be registered as execution time
    //
    void logFailedOperationNow(const std::string& containerName,
			       const cond::service::UserLogInfo& userlogInfo,
			       const std::string& destDB,
			       const std::string& payloadName,
			       const std::string& payloadToken,
			       const std::string& exceptionMessage
			       );
  private:
    void insertLogRecord(unsigned long long logId,
			const std::string& localtime,
			const std::string& containerName,
			const std::string& destDB,
			const std::string& payloadName, 
			const std::string& payloadToken,
			const cond::service::UserLogInfo& userLogInfo,
			const std::string& exceptionMessage);
    Connection* m_connectionHandle;
    CoralTransaction& m_coraldb;
    //coral::ISchema& m_schema;
    bool m_locked;
    coral::IQuery* m_statusEditorHandle;
    SequenceManager* m_sequenceManager;
    bool m_logTableExists;
  };
}//ns cond
#endif
