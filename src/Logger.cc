#include "CondCore/DBOutputService/interface/Logger.h"
#include "CondCore/DBOutputService/interface/UserLogInfo.h"
#include "CondCore/DBCommon/interface/CoralTransaction.h"
#include "CondCore/DBCommon/interface/SequenceManager.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/ITableDataEditor.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/ICursor.h"
#include "CoralBase/Attribute.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/AttributeSpecification.h"
#include "LogDBNames.h"
#include <boost/date_time/posix_time/posix_time.hpp>
cond::Logger::Logger(CoralTransaction& coraldb):m_schema(coraldb.nominalSchema()),m_locked(false),m_statusEditorHandle(0){
}
bool
cond::Logger::getWriteLock()throw() {
  try{
    coral::ITable& statusTable=m_schema.tableHandle(LogDBNames::LogTableName());
    //Instructs the server to lock the rows involved in the result set.
    m_statusEditorHandle=statusTable.newQuery();
    m_statusEditorHandle->setForUpdate();
  }catch(const std::exception& er){
    delete m_statusEditorHandle;
    m_statusEditorHandle=0;
    return false;
  }
  m_locked=true;
  return true;
}
bool
cond::Logger::releaseWriteLock()throw() {
  if(m_locked){
    delete m_statusEditorHandle;
    m_statusEditorHandle=0;
  }
  m_locked=false;
  return false;
}
void 
cond::Logger::logOperationNow(const std::string& containerName,
			      const cond::service::UserLogInfo& userlogInfo,
			      const std::string& destDB,
			      const std::string& payloadName,
			      const std::string& payloadToken
			      ){
  //aquirelocaltime
  boost::posix_time::ptime p=boost::posix_time::microsec_clock::local_time();
  std::string now=boost::posix_time::to_simple_string(p);
  //aquireentryid
  unsigned long long targetLogId=m_sequenceManager->incrementId(LogDBNames::LogTableName());
  //insert log record with the new id
  this->insertLogRecord(targetLogId,now,containerName,destDB,payloadName,payloadToken,userlogInfo,"");
}
void 
cond::Logger::logFailedOperationNow(const std::string& containerName,
				    const cond::service::UserLogInfo& userlogInfo,
			       const std::string& destDB,
			       const std::string& payloadName,
			       const std::string& payloadToken,
			       const std::string& exceptionMessage
				    ){
  //aquirelocaltime
  boost::posix_time::ptime p=boost::posix_time::microsec_clock::local_time();
  std::string now=boost::posix_time::to_simple_string(p);
  //aquireentryid
  unsigned long long targetLogId=m_sequenceManager->incrementId(LogDBNames::LogTableName());
  //insert log record with the new id
  this->insertLogRecord(targetLogId,now,containerName,destDB,payloadName,payloadToken,userlogInfo,exceptionMessage);
}
void
cond::Logger::insertLogRecord(unsigned long long logId,
			      const std::string& localtime,
			      const std::string& containerName,
			      const std::string& destDB,
			      const std::string& payloadName, 
			      const std::string& payloadToken,
			      const cond::service::UserLogInfo& userLogInfo,
			      const std::string& exceptionMessage){
  coral::AttributeList rowData;
  rowData.extend<unsigned long long>("LOGID");
  rowData.extend<unsigned long long>("EXECTIME");
  rowData.extend<std::string>("PAYLOADCONTAINER");
  rowData.extend<std::string>("DESTINATIONDB");
  rowData.extend<std::string>("PAYLOADNAME");
  rowData.extend<std::string>("PAYLOADTOKEN");
  rowData.extend<std::string>("PROVENANCE");
  rowData.extend<std::string>("COMMENT");
  rowData.extend<std::string>("ERRORMESSAGE");
  rowData["LOGID"].data< unsigned long long >() = logId;
  rowData["EXECTIME"].data< std::string >() = localtime;
  rowData["PAYLOADCONTAINER"].data< std::string >() = containerName;
  rowData["DESTINATIONDB"].data< std::string >() = destDB;
  rowData["PAYLOADNAME"].data< std::string >() = payloadName;
  rowData["PAYLOADTOKEN"].data< std::string >() = payloadToken;
  rowData["PROVENANCE"].data< std::string >() = userLogInfo.provenance;
  rowData["COMMENT"].data< std::string >() = userLogInfo.comment;
  rowData["ERRORMESSAGE"].data< std::string >() = exceptionMessage;
  m_schema.tableHandle(cond::LogDBNames::LogTableName()).dataEditor().insertRow(rowData);
}
