#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "createManyPayloads.h"
createManyPayloads::createManyPayloads(const edm::ParameterSet& iConfig)
{
  std::cout<<"createManyPayloads::createManyPayloads"<<std::endl;
}

createManyPayloads::~createManyPayloads()
{
  std::cout<<"createManyPayloads::createManyPayloads"<<std::endl;
}

void
createManyPayloads::analyze( const edm::Event& evt, const edm::EventSetup& evtSetup)
{
  std::cout<<"createManyPayloads::analyze "<<std::endl;
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
	size_t callbackToken=mydbservice->callbackToken("Pedestals");
	mydbservice->newValidityForNewPayload<Pedestals>(myped,mydbservice->currentTime(),callbackToken);
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
DEFINE_FWK_MODULE(createManyPayloads)
