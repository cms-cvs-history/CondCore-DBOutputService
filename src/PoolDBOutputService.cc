#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "DataFormats/Common/interface/EventID.h"
#include "DataFormats/Common/interface/Timestamp.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/IOVService/interface/IOVService.h"
#include "CondCore/IOVService/interface/IOVEditor.h"
#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "CondCore/DBCommon/interface/AuthenticationMethod.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBOutputService/interface/Exception.h"
#include "serviceCallbackToken.h"
#include <iostream>
#include <vector>
cond::service::PoolDBOutputService::PoolDBOutputService(const edm::ParameterSet & iConfig,edm::ActivityRegistry & iAR ): 
  m_connect( iConfig.getParameter< std::string > ("connect") ),
  m_timetype( iConfig.getParameter< std::string >("timetype") ),
  m_loader( new cond::ServiceLoader ),
  m_iovService( 0 ),
  m_metadata( 0 ),
  m_session( 0 ),
  m_dbstarted( false )
{
  if( m_timetype=="runnumber" ){
    m_endOfTime=(unsigned long long)edm::IOVSyncValue::endOfTime().eventID().run();
  }else{
    m_endOfTime=edm::IOVSyncValue::endOfTime().time().value();
  }
  std::string catalogcontact=iConfig.getUntrackedParameter< std::string >("catalog","");
  bool loadBlobStreamer=iConfig.getUntrackedParameter< bool >("loadBlobStreamer",false);
  unsigned int authenticationMethod=iConfig.getUntrackedParameter< unsigned int >("authenticationMethod",0);
  unsigned int messageLevel=iConfig.getUntrackedParameter<unsigned int>("messagelevel",0);
  typedef std::vector< edm::ParameterSet > Parameters;
  Parameters toPut=iConfig.getParameter<Parameters>("toPut");
  for(Parameters::iterator itToPut = toPut.begin(); itToPut != toPut.end(); ++itToPut) {
    cond::service::serviceCallbackRecord thisrecord;
    ///fix me!
    thisrecord.m_containerName = itToPut->getParameter<std::string>("record");
    thisrecord.m_tag = itToPut->getParameter<std::string>("tag");
    m_callbacks.insert(std::make_pair(cond::service::serviceCallbackToken::build(thisrecord.m_containerName),thisrecord));
  }
  try{
    if( authenticationMethod==1 ){
      m_loader->loadAuthenticationService( cond::XML );
    }else{
      m_loader->loadAuthenticationService( cond::Env );
    }
    
    switch (messageLevel) {
    case 0 :
      m_loader->loadMessageService(cond::Error);
      break;    
    case 1:
      m_loader->loadMessageService(cond::Warning);
      break;
    case 2:
      m_loader->loadMessageService( cond::Info );
      break;
    case 3:
      m_loader->loadMessageService( cond::Debug );
      break;  
    default:
      m_loader->loadMessageService();
    }
    if( loadBlobStreamer ){
      m_loader->loadBlobStreamingService();
    }
  }catch( const cond::Exception& e){
    throw e;
  }catch( const std::exception& e){
    throw cond::Exception( "PoolDBOutputService::PoolDBOutputService ")<<e.what();
  }catch ( ... ) {
    throw cond::Exception("PoolDBOutputService::PoolDBOutputService unknown error");
  }
  iAR.watchPreProcessEvent(this,&cond::service::PoolDBOutputService::preEventProcessing);
  iAR.watchPostEndJob(this,&cond::service::PoolDBOutputService::postEndJob);
  std::cout<<"m_connect "<<m_connect<<std::endl;
  m_metadata=new cond::MetaData(m_connect, *m_loader);
  m_session=new cond::DBSession(m_connect);
  m_iovService=new cond::IOVService(*m_session);
  m_session->setCatalog(catalogcontact);
}
cond::DBSession& cond::service::PoolDBOutputService::session() const{
  return *m_session;
}
cond::IOVService& cond::service::PoolDBOutputService::iovService() const{
  return *m_iovService;
}
cond::MetaData& cond::service::PoolDBOutputService::metadataService() const{
  return *m_metadata;
}
std::string cond::service::PoolDBOutputService::tag( const std::string& EventSetupRecordName ){
  return this->lookUpRecord(EventSetupRecordName).m_tag;
}
bool cond::service::PoolDBOutputService::isNewTagRequest( const std::string& EventSetupRecordName ){
  cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
  if(!m_dbstarted) this->initDB();
  return myrecord.m_isNewTag;
}
void 
cond::service::PoolDBOutputService::initDB()
{
  std::cout<<"initDB"<<std::endl;
  if(m_dbstarted) return;
  try{
    m_metadata->connect();
    for(std::map<size_t,cond::service::serviceCallbackRecord>::iterator it=m_callbacks.begin(); it!=m_callbacks.end(); ++it){
      it->second.m_iovEditor=m_iovService->newIOVEditor(m_metadata->getToken(it->second.m_tag));
      if( !m_metadata->hasTag(it->second.m_tag) ){
	it->second.m_isNewTag=true;
      }else{
	it->second.m_isNewTag=false;
      }
    }
    m_metadata->disconnect();
    m_session->connect( cond::ReadWriteCreate );
    //m_session->startUpdateTransaction();
  }catch( const cond::RefException& er){
    throw cms::Exception( er.what() );
  }catch( const cond::Exception& er ){
    throw cms::Exception( er.what() );
  }catch( const std::exception& er ){
    throw cms::Exception( er.what() );
  }catch(...){
    throw cms::Exception( "Unknown error" );
  }
  m_dbstarted=true;
}
void 
cond::service::PoolDBOutputService::postEndJob()
{
  if(!m_dbstarted) return;
  //m_session->commit();
  /*for(std::map<size_t,cond::service::serviceCallbackRecord>::iterator it=m_callbacks.begin(); it!=m_callbacks.end(); ++it){
    if(it->second.m_isNewTag){
    m_metadata->addMapping(it->second.m_tag,it->second.m_iovEditor->token());
    }
    }
  */
  //this->disconnect();
  //m_session->commit();
  m_session->disconnect();
}
void 
cond::service::PoolDBOutputService::preEventProcessing(const edm::EventID& iEvtid, const edm::Timestamp& iTime)
{
  if( m_timetype=="runnumber" ){
    m_currentTime=iEvtid.run();
  }else{ //timestamp
    m_currentTime=iTime.value();
  }
}
cond::service::PoolDBOutputService::~PoolDBOutputService(){
  for(std::map<size_t,cond::service::serviceCallbackRecord>::iterator it=m_callbacks.begin(); it!=m_callbacks.end(); ++it){
    if(it->second.m_iovEditor){
      delete it->second.m_iovEditor;
      it->second.m_iovEditor=0;
    }
  }
  m_callbacks.clear();
  delete m_session;
  delete m_metadata;
  delete m_iovService;
  delete m_loader;  
}
size_t cond::service::PoolDBOutputService::callbackToken(const std::string& EventSetupRecordName ) const {
  return cond::service::serviceCallbackToken::build(EventSetupRecordName);
}
/*
void
cond::service::PoolDBOutputService::connect()
{
  try{
      m_session->connect( cond::ReadWriteCreate );
      m_metadata->connect();
  }catch( const cond::Exception& e){
    throw e;
  }catch( const std::exception& e){
    throw cond::Exception(std::string("PoolDBOutputService::connect ")+e.what());
  }catch(...) {
    throw cond::Exception(std::string("PoolDBOutputService::connect unknown error") );
  }
}
*/
/*
void
cond::service::PoolDBOutputService::disconnect()
{
  m_metadata->disconnect();
  m_session->disconnect();
}
*/
unsigned long long cond::service::PoolDBOutputService::endOfTime() const{
  return m_endOfTime;
}
unsigned long long cond::service::PoolDBOutputService::currentTime() const{
  return m_currentTime;
}
void cond::service::PoolDBOutputService::createNewIOV( const std::string& firstPayloadToken, unsigned long long firstTillTime,const std::string& EventSetupRecordName){
  cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
  if (!m_dbstarted) this->initDB();
  std::string iovToken=this->insertIOV(myrecord,firstPayloadToken,firstTillTime,EventSetupRecordName);
  m_metadata->addMapping(myrecord.m_tag,iovToken);
  myrecord.m_isNewTag=false;
}
void cond::service::PoolDBOutputService::appendTillTime( const std::string& payloadToken, 
		     unsigned long long tillTime,
		     const std::string& EventSetupRecordName
		     ){
  cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
  if (!m_dbstarted) this->initDB();
  this->insertIOV(myrecord,payloadToken,tillTime,EventSetupRecordName);
}
void cond::service::PoolDBOutputService::appendSinceTime( const std::string& payloadToken, unsigned long long sinceTime,const std::string& EventSetupRecordName ){
  cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
  if (!m_dbstarted) this->initDB();
  this->appendIOV(myrecord,payloadToken,sinceTime);
}
void cond::service::PoolDBOutputService::appendIOV(cond::service::serviceCallbackRecord& record, const std::string& payloadToken, unsigned long long sinceTime){
  // if( record.m_isNewTag ) throw cond::Exception(std::string("PoolDBOutputService::appendIOV: cannot append to non-existing tag ")+record.m_tag );  
  record.m_iovEditor->append(payloadToken,sinceTime);
}
std::string cond::service::PoolDBOutputService::insertIOV(cond::service::serviceCallbackRecord& record, const std::string& payloadToken, unsigned long long tillTime, const std::string& EventSetupRecordName){
  std::cout<<"insertIOV payloadToken"<<payloadToken<<std::endl;
  std::cout<<"tillTime "<<tillTime<<std::endl;
  std::cout<<"record "<<EventSetupRecordName<<std::endl;
  record.m_iovEditor->insert(payloadToken,tillTime);
  std::string iovToken=record.m_iovEditor->token();
  return iovToken;
}
cond::service::serviceCallbackRecord& cond::service::PoolDBOutputService::lookUpRecord(const std::string& EventSetupRecordName){
  size_t callbackToken=this->callbackToken( EventSetupRecordName );
  std::map<size_t,cond::service::serviceCallbackRecord>::iterator it=m_callbacks.find(callbackToken);
  if(it==m_callbacks.end()) throw cond::UnregisteredRecordException(EventSetupRecordName);
  return it->second;
}
