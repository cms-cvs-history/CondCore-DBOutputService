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
  class ISchema;  
  class IQuery;
}
namespace cond{
  namespace service{
    class UserLogInfo;
  }
  class CoralTransaction;
  class SequenceManager;
  class Logger{
  public:
    explicit Logger(CoralTransaction& coraldb);
    virtual ~Logger();
    bool getWriteLock() throw ();
    bool releaseWriteLock() throw ();
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
    //CoralTransaction& m_coraldb;
    coral::ISchema& m_schema;
    bool m_locked;
    coral::IQuery* m_statusEditorHandle;
    SequenceManager* m_sequenceManager;
  };
}//ns cond
#endif
