#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "DataFormats/Common/interface/EventID.h"
#include "DataFormats/Common/interface/Timestamp.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondCore/DBCommon/interface/PoolStorageManager.h"
#include "CondCore/DBCommon/interface/RelationalStorageManager.h"
#include "CondCore/DBCommon/interface/ConnectionConfiguration.h"
#include "CondCore/DBCommon/interface/SessionConfiguration.h"
#include "CondCore/IOVService/interface/IOVService.h"
#include "CondCore/IOVService/interface/IOVEditor.h"
#include "CondCore/DBCommon/interface/AuthenticationMethod.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBOutputService/interface/Exception.h"
#include "serviceCallbackToken.h"
#include <iostream>
#include <vector>
cond::service::PoolDBOutputService::PoolDBOutputService(const edm::ParameterSet & iConfig,edm::ActivityRegistry & iAR ): 
  m_timetype( iConfig.getParameter< std::string >("timetype") ),
  m_endOfTime( 0 ),
  m_currentTime( 0 ),
  m_session( 0 ),
  m_pooldb( 0 ),
  m_coraldb( 0 ),
  m_dbstarted( false )
{
  if( m_timetype=="runnumber" ){
    m_endOfTime=(cond::Time_t)edm::IOVSyncValue::endOfTime().eventID().run();
  }else{
    m_endOfTime=edm::IOVSyncValue::endOfTime().time().value();
  }
  edm::ParameterSet connectionPset = iConfig.getParameter<edm::ParameterSet>("DBParameters");  
  typedef std::vector< edm::ParameterSet > Parameters;
  Parameters toPut=iConfig.getParameter<Parameters>("toPut");
  for(Parameters::iterator itToPut = toPut.begin(); itToPut != toPut.end(); ++itToPut) {
    cond::service::serviceCallbackRecord thisrecord;
    thisrecord.m_containerName = itToPut->getParameter<std::string>("record");
    thisrecord.m_tag = itToPut->getParameter<std::string>("tag");
    m_callbacks.insert(std::make_pair(cond::service::serviceCallbackToken::build(thisrecord.m_containerName),thisrecord));
  }
  iAR.watchPreProcessEvent(this,&cond::service::PoolDBOutputService::preEventProcessing);
  iAR.watchPostEndJob(this,&cond::service::PoolDBOutputService::postEndJob);
  std::string connect=connectionPset.getParameter<std::string>("connect");
  m_session=new cond::DBSession(connect);
  m_catalog=connectionPset.getUntrackedParameter<std::string>("catalog","file::PoolFileCatalog.xml");
  std::string authMethod=connectionPset.getUntrackedParameter<std::string>("authenticationMethod","XML");
  int messageLevel=connectionPset.getUntrackedParameter<int>("messageLevel",0);
  bool enableConnectionSharing=connectionPset.getUntrackedParameter<bool>("enableConnectionSharing",true);
  int connectionTimeOut=connectionPset.getUntrackedParameter<int>("connectionTimeOut",600);
  bool enableReadOnlySessionOnUpdateConnection=connectionPset.getUntrackedParameter<bool>("enableReadOnlySessionOnUpdateConnection",true);
  bool loadBlobStreamer=connectionPset.getUntrackedParameter<bool>("loadBlobStreamer",false);
  int connectionRetrialPeriod=connectionPset.getUntrackedParameter<int>("connectionRetrialPeriod",30);
  int connectionRetrialTimeOut=connectionPset.getUntrackedParameter<int>("connectionRetrialTimeOut",180);
  if(authMethod=="ENV"||authMethod=="Env"){
    m_session->sessionConfiguration().setAuthenticationMethod(cond::Env);
  }else{
    m_session->sessionConfiguration().setAuthenticationMethod(cond::XML);
  }  
  switch (messageLevel) {
  case 0 :
    m_session->sessionConfiguration().setMessageLevel( cond::Error );
      break;    
  case 1:
    m_session->sessionConfiguration().setMessageLevel( cond::Warning );
    break;
  case 2:
    m_session->sessionConfiguration().setMessageLevel( cond::Info );
    break;
  case 3:
    m_session->sessionConfiguration().setMessageLevel( cond::Debug );
    break;  
  default:
    m_session->sessionConfiguration().setMessageLevel( cond::Error );
  }
  if(enableConnectionSharing){
    m_session->connectionConfiguration().enableConnectionSharing();
  }
  m_session->connectionConfiguration().setConnectionTimeOut(connectionTimeOut);
  if(enableReadOnlySessionOnUpdateConnection){
    m_session->connectionConfiguration().enableReadOnlySessionOnUpdateConnections();
  }
  if(loadBlobStreamer){
    m_session->sessionConfiguration().setBlobStreamer("");
  }
  m_session->connectionConfiguration().setConnectionRetrialPeriod(connectionRetrialPeriod);
  m_session->connectionConfiguration().setConnectionRetrialTimeOut(connectionRetrialTimeOut);
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
  if(m_dbstarted) return;
  m_session->open(true);
  m_pooldb=&(m_session->poolStorageManager(m_catalog));
  m_coraldb=&(m_session->relationalStorageManager());
  try{
    cond::MetaData metadata(*m_coraldb);
    cond::IOVService iovservice(*m_pooldb);
    m_coraldb->connect(cond::ReadOnly);
    m_coraldb->startTransaction(true);
    for(std::map<size_t,cond::service::serviceCallbackRecord>::iterator it=m_callbacks.begin(); it!=m_callbacks.end(); ++it){
      std::string iovtoken;
      if( !metadata.hasTag(it->second.m_tag) ){
	it->second.m_isNewTag=true;
      }else{
	iovtoken=metadata.getToken(it->second.m_tag);
	it->second.m_isNewTag=false;
      }
      it->second.m_iovEditor=iovservice.newIOVEditor(iovtoken);
    }
    m_coraldb->commit();
    m_coraldb->disconnect();
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
  /*dummy
   */
  if(m_dbstarted) m_session->close();
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
}
size_t cond::service::PoolDBOutputService::callbackToken(const std::string& EventSetupRecordName ) const {
  return cond::service::serviceCallbackToken::build(EventSetupRecordName);
}
cond::Time_t cond::service::PoolDBOutputService::endOfTime() const{
  return m_endOfTime;
}
cond::Time_t cond::service::PoolDBOutputService::currentTime() const{
  return m_currentTime;
}
void cond::service::PoolDBOutputService::createNewIOV( const std::string& firstPayloadToken, cond::Time_t firstTillTime,const std::string& EventSetupRecordName){
  cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
  if (!m_dbstarted) this->initDB();
  std::string iovToken=this->insertIOV(myrecord,firstPayloadToken,firstTillTime,EventSetupRecordName);
  cond::MetaData metadata(*m_coraldb);
  m_coraldb->connect(cond::ReadWriteCreate);
  m_coraldb->startTransaction(false);
  metadata.addMapping(myrecord.m_tag,iovToken);
  m_coraldb->commit();
  m_coraldb->disconnect();
  myrecord.m_isNewTag=false;
}
void cond::service::PoolDBOutputService::appendTillTime( const std::string& payloadToken, cond::Time_t tillTime,const std::string& EventSetupRecordName){
  cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
  if (!m_dbstarted) this->initDB();
  m_pooldb->connect(cond::ReadWrite);
  m_pooldb->startTransaction(false);    
  this->insertIOV(myrecord,payloadToken,tillTime,EventSetupRecordName);
  m_pooldb->commit();    
  m_pooldb->disconnect();
}
void cond::service::PoolDBOutputService::appendSinceTime( const std::string& payloadToken, cond::Time_t sinceTime,const std::string& EventSetupRecordName ){
  cond::service::serviceCallbackRecord& myrecord=this->lookUpRecord(EventSetupRecordName);
  if (!m_dbstarted) this->initDB();
  m_pooldb->connect(cond::ReadWrite);
  m_pooldb->startTransaction(false);    
  this->appendIOV(myrecord,payloadToken,sinceTime);
  m_pooldb->commit();    
  m_pooldb->disconnect();
}
void cond::service::PoolDBOutputService::appendIOV(cond::service::serviceCallbackRecord& record, const std::string& payloadToken, cond::Time_t sinceTime){
  // if( record.m_isNewTag ) throw cond::Exception(std::string("PoolDBOutputService::appendIOV: cannot append to non-existing tag ")+record.m_tag );  
  record.m_iovEditor->append(payloadToken,sinceTime);
}
std::string cond::service::PoolDBOutputService::insertIOV(cond::service::serviceCallbackRecord& record, const std::string& payloadToken, cond::Time_t tillTime, const std::string& EventSetupRecordName){
  //std::cout<<"insertIOV payloadToken"<<payloadToken<<std::endl;
  //std::cout<<"tillTime "<<tillTime<<std::endl;
  //std::cout<<"record "<<EventSetupRecordName<<std::endl;
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
