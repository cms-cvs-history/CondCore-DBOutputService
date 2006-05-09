#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "createPayloads.h"
createPayloads::createPayloads(const edm::ParameterSet& iConfig)
{
  std::cout<<"createPayloads::createPayloads"<<std::endl;
}

createPayloads::~createPayloads()
{
  std::cout<<"createPayloads::createPayloads"<<std::endl;
}

void
createPayloads::analyze( const edm::Event& evt, const edm::EventSetup& evtSetup)
{
  std::cout<<"createPayloads::analyze "<<std::endl;
  unsigned int irun=evt.id().run();
  if(irun%2==0){
    Pedestals* myped=new Pedestals;
    for(int ichannel=1; ichannel<=5; ++ichannel){
      Pedestals::Item item;
      item.m_mean=1.11*ichannel;
      item.m_variance=1.12*ichannel;
      myped->m_pedestals.push_back(item);
    }
    edm::Service<cond::service::PoolDBOutputService> mydbservice;
    if( mydbservice.isAvailable() ){
      try{
	std::cout<<"current time "<<mydbservice->currentTime()<<std::endl;
	mydbservice->newValidityForNewPayload<Pedestals>(myped,mydbservice->currentTime());
      }catch(const cond::Exception& er){
	std::cout<<er.what()<<std::endl;
      }catch(const std::exception& er){
	std::cout<<"caught std::exception "<<er.what()<<std::endl;
      }catch(...){
	std::cout<<"Funny error"<<std::endl;
    }
    }else{
      std::cout<<"Service is unavailable"<<std::endl;
    }
  }
}
#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(createPayloads)
