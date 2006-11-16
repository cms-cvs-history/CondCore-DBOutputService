#ifndef CondCore_PoolDBOutputService_h
#define CondCore_PoolDBOutputService_h
#include "FWCore/ServiceRegistry/interface/ActivityRegistry.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/PoolStorageManager.h"
#include "CondCore/DBCommon/interface/RelationalStorageManager.h"
#include "CondCore/DBCommon/interface/Ref.h"
//#include "CondCore/IOVService/interface/IOVService.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "serviceCallbackRecord.h"
#include <string>
#include <map>
namespace edm{
  class Event;
  class EventSetup;
  class ParameterSet;
}
namespace cond{
  namespace service {
    class serviceCallbackToken;
    class PoolDBOutputService{
    public:
      PoolDBOutputService( const edm::ParameterSet & iConfig, 
			   edm::ActivityRegistry & iAR );
      //use these to control connections
      //void  postBeginJob();
      void  postEndJob();
      //
      //use these to control transaction interval
      //
      void preEventProcessing( const edm::EventID & evtID, 
      			       const edm::Timestamp & iTime );
      //
      // return the database session in use
      //
      cond::DBSession& session() const;
      std::string tag( const std::string& EventSetupRecordName );
      bool isNewTagRequest( const std::string& EventSetupRecordName );
      //
      // insert the payload and its valid till time into the database
      // Note: user looses the ownership of the pointer to the payloadObj
      // The payload object will be stored as well
      // 
      template<typename T>
	void createNewIOV( T* firstPayloadObj, 
			   unsigned long long firstTillTime,
			   const std::string& EventSetupRecordName
			   ){
	cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
	if (!m_dbstarted) this->initDB();
	m_pooldb->connect(cond::ReadWriteCreate);
	m_pooldb->startTransaction(false);    
	cond::Ref<T> myPayload(*m_pooldb,firstPayloadObj);
	myPayload.markWrite(EventSetupRecordName);
	std::string payloadToken=myPayload.token();
	std::string iovToken=this->insertIOV(myrecord,payloadToken,firstTillTime,EventSetupRecordName);
	m_pooldb->commit();
	m_pooldb->disconnect();	
	cond::MetaData metadata(*m_coraldb);
	m_coraldb->connect(cond::ReadWriteCreate);
	m_coraldb->startTransaction(false);
	metadata.addMapping(myrecord.m_tag,iovToken);
	m_coraldb->commit();
	m_coraldb->disconnect();
	myrecord.m_isNewTag=false;
      }
      void createNewIOV( const std::string& firstPayloadToken, 
			unsigned long long firstTillTime,
			const std::string& EventSetupRecordName );
      template<typename T>
	void appendTillTime( T* payloadObj, 
			     unsigned long long tillTime,
			     const std::string& EventSetupRecordName
			     ){
	cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
	if (!m_dbstarted) this->initDB();
	m_pooldb->connect(cond::ReadWriteCreate);
	m_pooldb->startTransaction(false);    
	cond::Ref<T> myPayload(*m_pooldb,payloadObj);
	myPayload.markWrite(EventSetupRecordName);
	std::string payloadToken=myPayload.token();
	std::string iovToken=this->insertIOV(myrecord,payloadToken,tillTime,EventSetupRecordName);
	m_pooldb->commit();    
	m_pooldb->disconnect();
      }
      void appendTillTime( const std::string& payloadToken, 
			   unsigned long long tillTime,
			   const std::string& EventSetupRecordName
			    );
      
      template<typename T>
	void appendSinceTime( T* payloadObj, 
			      unsigned long long sinceTime,
			      const std::string& EventSetupRecordName ){
	cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
	std::cout<<"got record "<<EventSetupRecordName<<std::endl;
	if (!m_dbstarted) this->initDB();
	std::cout<<"start again transaction"<<std::endl;
	m_pooldb->connect(cond::ReadWriteCreate);
	m_pooldb->startTransaction(false);    
	cond::Ref<T> myPayload(*m_pooldb,payloadObj);
	std::cout<<"got ref "<<myPayload.token()<<std::endl;
	std::cout<<"markWrite to "<<EventSetupRecordName<<std::endl;
	myPayload.markWrite(EventSetupRecordName);
	std::cout<<"markWrite to "<<EventSetupRecordName<<std::endl;
	std::string payloadToken=myPayload.token();
	std::cout<<"got token "<<payloadToken<<std::endl;
	this->appendIOV(myrecord,payloadToken,sinceTime);
	std::cout<<"about to commit"<<std::endl;
	m_pooldb->commit();
	m_pooldb->disconnect();
      }
      //
      // Append the payload and its valid sinceTime into the database
      // Note: user looses the ownership of the pointer to the payloadObj
      // Note: the iov index appended to MUST pre-existing and the existing 
      // conditions data are retrieved from EventSetup 
      // 
      void appendSinceTime( const std::string& payloadToken, 
			   unsigned long long sinceTime,
			   const std::string& EventSetupRecordName );

      //
      // Service time utility callback method 
      // return the infinity value according to the given timetype
      // It is the IOV closing boundary
      //
      unsigned long long endOfTime() const;
      //
      // Service time utility callback method 
      // return the current conditions time value according to the 
      // given timetype
      //
      unsigned long long currentTime() const;
      virtual ~PoolDBOutputService();
    private:
      void connect();    
      void disconnect();
      void initDB();
      size_t callbackToken(const std::string& EventSetupRecordName ) const ;
      void appendIOV(cond::service::serviceCallbackRecord& record,
		     const std::string& payloadToken, 
		     unsigned long long sinceTime);
      std::string insertIOV(cond::service::serviceCallbackRecord& record,
			    const std::string& payloadToken, 
			    unsigned long long tillTime, const std::string& EventSetupRecordName);
      serviceCallbackRecord& lookUpRecord(const std::string& EventSetupRecordName);
    private:
      //std::string m_connect;
      std::string m_timetype;
      std::string m_catalog;
      unsigned long long m_endOfTime;
      unsigned long long m_currentTime;
      cond::DBSession* m_session;
      cond::PoolStorageManager* m_pooldb;
      cond::RelationalStorageManager* m_coraldb;
      std::map<size_t, cond::service::serviceCallbackRecord> m_callbacks;
      bool m_dbstarted;
      //edm::ParameterSet m_connectionPset;
    };//PoolDBOutputService
  }//ns service
}//ns cond
#endif
