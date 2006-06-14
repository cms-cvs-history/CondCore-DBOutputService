#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "CondFormats/DataRecord/interface/PedestalsRcd.h"
#include "appendManyPayloads.h"

appendManyPayloads::appendManyPayloads(const edm::ParameterSet& iConfig)
{
  std::cout<<"appendManyPayloads::appendManyPayloads"<<std::endl;
}

appendManyPayloads::~appendManyPayloads()
{
  std::cout<<"appendManyPayloads::~appendManyPayloads"<<std::endl;
}
void
appendManyPayloads::analyze(const edm::Event& evt, 
			     const edm::EventSetup& evtSetup){
  std::cout<<"appendManyPayloads::analyze getting handle"<<std::endl;
  edm::ESHandle<Pedestals> peds;
  evtSetup.get<PedestalsRcd>().get(peds);
  std::cout<<"get current data"<<std::endl;
  edm::Service<cond::service::PoolDBOutputService> mydbservice;
  try{
    if( mydbservice.isAvailable() ){
      unsigned long long currentTime=mydbservice->currentTime();
      std::cout<<"currentTime "<<currentTime<<std::endl;
      if( currentTime%2==0 ){
	std::cout<<"appending new calib data valid from "<<currentTime+1<<" to iov closing time"<<std::endl;
	Pedestals* myped=new Pedestals;
	for(int ichannel=1; ichannel<=5; ++ichannel){
	  Pedestals::Item item;
	  item.m_mean=1.11*ichannel+currentTime;
	  item.m_variance=1.12*ichannel+currentTime;
	  myped->m_pedestals.push_back(item);
	}
	size_t callbackToken=mydbservice->callbackToken("Pedestals");
	mydbservice->newValidityForNewPayload<Pedestals>(myped,currentTime,callbackToken);
      }
    }else{
      std::cout<<"Service is unavailable"<<std::endl;
    }
  }catch(const cond::Exception& er){
    std::cout<<er.what()<<std::endl;
  }catch(const std::exception& er){
    std::cout<<"caught std::exception "<<er.what()<<std::endl;
  }catch(...){
    std::cout<<"Funny error"<<std::endl;
  }
}

void
appendManyPayloads::endJob()
{
}

#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(appendManyPayloads)
