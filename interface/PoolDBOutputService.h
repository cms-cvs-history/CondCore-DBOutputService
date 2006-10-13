#ifndef CondCore_PoolDBOutputService_h
#define CondCore_PoolDBOutputService_h
#include "FWCore/ServiceRegistry/interface/ActivityRegistry.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Ref.h"
#include "serviceCallbackRecord.h"
#include <string>
#include <map>
namespace edm{
  class Event;
  class EventSetup;
  class ParameterSet;
}
namespace cond{
  class ServiceLoader;
  class MetaData;
  class DBSession;
  class IOVService;
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
      cond::IOVService& iovService() const;
      cond::MetaData& metadataService() const;
      std::string tag( const std::string& EventSetupRecordName );
      bool isNewtag( const std::string& EventSetupRecordName );
      //
      // insert the payload and its valid till time into the database
      // Note: user looses the ownership of the pointer to the payloadObj
      // The payload object will be stored as well
      // 
      template<typename T>
	void buildNewIOV( T* payloadObj, 
			  unsigned long long tillTime,
			  const std::string& EventSetupRecordName ){
	cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
	if (!m_dbstarted) this->initDB();
	cond::Ref<T> myPayload(*m_session,payloadObj);
	myPayload.markWrite(EventSetupRecordName);
	std::string payloadToken=myPayload.token();
	this->insertIOV(payloadToken,tillTime,EventSetupRecordName);
      }
      void buildNewIOV( const std::string& payloadToken, 
			unsigned long long tillTime,
			const std::string& EventSetupRecordName );
      //
      // Append the payload and its valid sinceTime into the database
      // Note: user looses the ownership of the pointer to the payloadObj
      // Note: the iov index appended to MUST pre-existing and the existing 
      // conditions data are retrieved from EventSetup 
      // 
      template<typename T>
	void extendOldIOV( T* payloadObj, 
			   unsigned long long sinceTime,
			   const std::string& EventSetupRecordName ){
	cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
	if (!m_dbstarted) this->initDB();
	cond::Ref<T> myPayload(*m_session,payloadObj);
	myPayload.markWrite(EventSetupRecordName);
	std::string payloadToken=myPayload.token();
	this->appendIOV(myrecord,payloadToken,sinceTime);
      }
      
      void extendOldIOV( const std::string& payloadToken, 
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
      void insertIOV(cond::service::serviceCallbackRecord& record,
		     const std::string& payloadToken, 
		     unsigned long long tillTime, const std::string& EventSetupRecordName);
      serviceCallbackRecord& lookUpRecord(const std::string& EventSetupRecordName);
    private:
      std::string m_connect;
      std::string m_timetype;
      std::string m_catalog;
      unsigned long long m_endOfTime;
      unsigned long long m_currentTime;
      cond::ServiceLoader* m_loader;
      cond::IOVService* m_iovService;
      cond::MetaData* m_metadata;
      cond::DBSession* m_session;
      std::map<size_t, cond::service::serviceCallbackRecord> m_callbacks;
      bool m_dbstarted;
    };//PoolDBOutputService
  }//ns service
}//ns cond
#endif
