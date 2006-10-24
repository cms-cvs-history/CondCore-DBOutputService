#include "FWCore/Framework/interface/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "IOVPayloadAnalyzer.h"
IOVPayloadAnalyzer::IOVPayloadAnalyzer(const edm::ParameterSet& iConfig ):
  m_record(iConfig.getParameter< std::string >("record")){
  std::cout<<"IOVPayloadAnalyzer::IOVPayloadAnalyzer"<<std::endl;
}
IOVPayloadAnalyzer::~IOVPayloadAnalyzer(){
  std::cout<<"IOVPayloadAnalyzer::~IOVPayloadAnalyzer"<<std::endl;
}
void IOVPayloadAnalyzer::analyze( const edm::Event& evt, const edm::EventSetup& evtSetup){
  std::cout<<"IOVPayloadAnalyzer::analyze "<<std::endl;
  edm::Service<cond::service::PoolDBOutputService> mydbservice;
  if( !mydbservice.isAvailable() ){
    std::cout<<"Service is unavailable"<<std::endl;
    return;
  }
  unsigned int irun=evt.id().run();
  try{
    std::string tag=mydbservice->tag(m_record);
    std::cout<<"tag "<<tag<<std::endl;
    if(irun%2==0){ //runs new ped occurs
      std::cout<<"run "<<irun<<std::endl;
      Pedestals* myped=new Pedestals;
      for(int ichannel=1; ichannel<=5; ++ichannel){
	Pedestals::Item item;
	item.m_mean=1.11*ichannel+irun;
	item.m_variance=1.12*ichannel+irun;
	myped->m_pedestals.push_back(item);
      }
      if( mydbservice->isNewTagRequest(m_record) ){
	//create mode
	std::cout<<"creating"<<std::endl;
	unsigned long long firstTillTime=mydbservice->endOfTime();
	mydbservice->createNewIOV<Pedestals>(myped,firstTillTime,m_record);
	std::cout<<"created"<<std::endl;
      }else{
	//append mode. Note: correct PoolDBESSource must be loaded
	std::cout<<"appending"<<std::endl;
	mydbservice->appendSinceTime<Pedestals>(myped,mydbservice->currentTime(),m_record);
	std::cout<<"done"<<std::endl;
      }
    }
  }catch(const cond::Exception& er){
    std::cout<<er.what()<<std::endl;
  }catch(const std::exception& er){
    std::cout<<"caught std::exception "<<er.what()<<std::endl;
  }catch(...){
    std::cout<<"Unknown error"<<std::endl;
  }
}
void IOVPayloadAnalyzer::endJob(){ 
}
DEFINE_FWK_MODULE(IOVPayloadAnalyzer)