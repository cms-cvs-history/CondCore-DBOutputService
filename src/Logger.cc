#include "CondCore/DBOutputService/interface/Logger.h"
#include "CondCore/DBOutputService/interface/UserLogInfo.h"
#include "CondCore/DBCommon/interface/CoralTransaction.h"
#include "CondCore/DBCommon/interface/SequenceManager.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/ITableDataEditor.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/ICursor.h"
#include "RelationalAccess/TableDescription.h"
#include "RelationalAccess/ITablePrivilegeManager.h"
#include "CoralBase/Attribute.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/AttributeSpecification.h"
#include "LogDBNames.h"
#include <boost/date_time/posix_time/posix_time_types.hpp> //no i/o just types
//#include "boost/date_time/gregorian/gregorian_types.hpp" //no i/o just types
#include <sstream>
#include <exception>
namespace cond{
  template <class T> 
  std::string to_string(const T& t){
    std::stringstream ss;
    ss<<t;
    return ss.str();
  }
}
cond::Logger::Logger(CoralTransaction& coraldb):m_coraldb(coraldb),m_schema(coraldb.nominalSchema()),m_locked(false),m_statusEditorHandle(0),m_sequenceManager(0),m_logTableExists(false){
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
cond::Logger::createLogDBIfNonExist(){
  if(m_logTableExists) return;
  if(m_schema.existsTable(cond::LogDBNames::SequenceTableName())&&m_schema.existsTable(cond::LogDBNames::LogTableName())){
    m_logTableExists=true;
    return;
  }
  //create sequence table
  cond::SequenceManager sequenceGenerator(m_coraldb,cond::LogDBNames::SequenceTableName());
  if( !sequenceGenerator.existSequencesTable() ){
    sequenceGenerator.createSequencesTable();
  }
  //create log table
  coral::TableDescription description( "CONDLOG" );
  description.setName(cond::LogDBNames::LogTableName());
  description.insertColumn(std::string("LOGID"),
			   coral::AttributeSpecification::typeNameForType<unsigned long long>() );
  description.setPrimaryKey( std::vector<std::string>( 1, std::string("LOGID")));
  description.insertColumn(std::string("EXECTIME"),
			   coral::AttributeSpecification::typeNameForType<std::string>() );
  description.setNotNullConstraint(std::string("EXECTIME"));
  description.insertColumn(std::string("PAYLOADCONTAINER"),
	  coral::AttributeSpecification::typeNameForType<std::string>() );
  description.setNotNullConstraint(std::string("PAYLOADCONTAINER"));
  description.insertColumn(std::string("PAYLOADNAME"),
	  coral::AttributeSpecification::typeNameForType<std::string>() );
  description.setNotNullConstraint(std::string("PAYLOADNAME"));
  description.insertColumn(std::string("PAYLOADTOKEN"),
	  coral::AttributeSpecification::typeNameForType<std::string>() );
  description.setNotNullConstraint(std::string("PAYLOADTOKEN"));
  description.insertColumn(std::string("DESTINATIONDB"),
	  coral::AttributeSpecification::typeNameForType<std::string>() );
  description.setNotNullConstraint(std::string("DESTINATIONDB"));
  description.insertColumn(std::string("PROVENANCE"),
	  coral::AttributeSpecification::typeNameForType<std::string>() );
  description.insertColumn(std::string("COMMENT"),
	  coral::AttributeSpecification::typeNameForType<std::string>() );
  description.insertColumn(std::string("ERRORMESSAGE"),
	  coral::AttributeSpecification::typeNameForType<std::string>() );
  m_schema.createTable( description ).privilegeManager().grantToPublic( coral::ITablePrivilegeManager::Select );
  m_logTableExists=true;
}
void 
cond::Logger::logOperationNow(const std::string& containerName,
			      const cond::service::UserLogInfo& userlogInfo,
			      const std::string& destDB,
			      const std::string& payloadName,
			      const std::string& payloadToken
			      ){
  //aquirelocaltime
  //using namespace boost::posix_time;
  boost::posix_time::ptime p=boost::posix_time::microsec_clock::local_time();
  std::string now=cond::to_string(p.date().year())+"-"+cond::to_string(p.date().month())+"-"+cond::to_string(p.date().day())+"-"+cond::to_string(p.time_of_day().hours())+":"+cond::to_string(p.time_of_day().minutes())+":"+cond::to_string(p.time_of_day().seconds());
  //aquireentryid
  if(!m_sequenceManager){
    m_sequenceManager=new cond::SequenceManager(m_coraldb,cond::LogDBNames::SequenceTableName());
  }
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
  std::string now=cond::to_string(p.date().year())+"-"+cond::to_string(p.date().month())+"-"+cond::to_string(p.date().day())+"-"+cond::to_string(p.time_of_day().hours())+":"+cond::to_string(p.time_of_day().minutes())+":"+cond::to_string(p.time_of_day().seconds());
  //aquireentryid
  if(!m_sequenceManager){
    m_sequenceManager=new cond::SequenceManager(m_coraldb,cond::LogDBNames::SequenceTableName());
  }
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
  try{
    coral::AttributeList rowData;
    rowData.extend<unsigned long long>("LOGID");
    rowData.extend<std::string>("EXECTIME");
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
  }catch(const std::exception& er){
    throw cond::Exception(std::string(er.what()));
  }
}

cond::Logger::~Logger(){
  if( m_sequenceManager ){
    delete m_sequenceManager;
    m_sequenceManager=0;
  }
}
