#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "testPoolDBOutputServiceClient.h"
testPoolDBOutputServiceClient::testPoolDBOutputServiceClient(const edm::ParameterSet& iConfig)
{
  std::cout<<"testPoolDBOutputServiceClient::testPoolDBOutputServiceClient"<<std::endl;
}


testPoolDBOutputServiceClient::~testPoolDBOutputServiceClient()
{
  std::cout<<"testPoolDBOutputServiceClient::~testPoolDBOutputServiceClient"<<std::endl;
}

void
testPoolDBOutputServiceClient::analyze(const edm::Event& iEvent, const edm::EventSetup& iSetup)
{
//see if service is accessible
  std::cout<<"testPoolDBOutputServiceClient::analyze"<<std::endl;
  edm::Service<cond::service::PoolDBOutputService> mydbservice;
  if( mydbservice.isAvailable() ){
    //unsigned int newValidity=iEvent.id().run();
    //mydbservice->newValidityForOldPayload("myoldpayloadToken",
    //				  newValidity);
    std::cout<<"current time "<<mydbservice->currentTime()<<std::endl;
    std::cout<<"end of time "<<mydbservice->endOfTime()<<std::endl;
    //mydbservice->newValidityForNewPayload(&a, newValidity);
  }else{
    std::cout<<"Service dummyDBOutput is unavailable"<<std::endl;
  }
  std::cout<<"about to get out of testPoolDBOutputServiceClient::analyze"<<std::endl;
}
void
testPoolDBOutputServiceClient::endJob()
{
  std::cout<<"testPoolDBOutputServiceClient::endJob"<<std::endl;
  Pedestals* myped=new Pedestals;
  for(int ichannel=1; ichannel<=5; ++ichannel){
    Pedestals::Item item;
    item.m_mean=1.11*ichannel;
    item.m_variance=1.12*ichannel;
    myped->m_pedestals.push_back(item);
  }
  edm::Service<cond::service::PoolDBOutputService> mydbservice;
  if( mydbservice.isAvailable() ){
    //unsigned int newValidity=iEvent.id().run();
    //mydbservice->newValidityForOldPayload("myoldpayloadToken",
    //				  newValidity);
    //std::cout<<"current time "<<mydbservice->currentTime()<<std::endl;
    //std::cout<<"end of time "<<mydbservice->endOfTime()<<std::endl;
    try{
      mydbservice->newValidityForNewPayload<Pedestals>(myped,mydbservice->endOfTime());
    }catch(const cond::Exception& er){
      std::cout<<er.what()<<std::endl;
    }catch(const std::exception& er){
      std::cout<<"caught std::exception "<<er.what()<<std::endl;
    }catch(...){
      std::cout<<"Funny error"<<std::endl;
    }
  }else{
    std::cout<<"Service dummyDBOutput is unavailable"<<std::endl;
  }
  std::cout<<"about to get out of testPoolDBOutputServiceClient::analyze"<<std::endl;
}
#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(testPoolDBOutputServiceClient)
