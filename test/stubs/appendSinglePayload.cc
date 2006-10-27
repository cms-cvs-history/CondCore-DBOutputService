#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "CondFormats/DataRecord/interface/PedestalsRcd.h"
#include "appendSinglePayload.h"

appendSinglePayload::appendSinglePayload(const edm::ParameterSet& iConfig)
{
  std::cout<<"appendSinglePayload::appendSinglePayload"<<std::endl;
}

appendSinglePayload::~appendSinglePayload()
{
  std::cout<<"appendSinglePayload::~appendSinglePayload"<<std::endl;
}
void
appendSinglePayload::analyze(const edm::Event& evt, 
			     const edm::EventSetup& evtSetup){
  std::cout<<"appendSinglePayload::analyze getting handle"<<std::endl;
  edm::ESHandle<Pedestals> peds;
  evtSetup.get<PedestalsRcd>().get(peds);
}

void
appendSinglePayload::endJob()
{
  std::cout<<"appendSinglePayload::endJob"<<std::endl;
  edm::Service<cond::service::PoolDBOutputService> mydbservice;
  if( !mydbservice.isAvailable() ){
    std::cout<<"Service is unavailable"<<std::endl;
    return;
  }
  size_t callbackToken=mydbservice->callbackToken("Pedestals");
  try{
    unsigned long long currentTime=mydbservice->currentTime();
    std::cout<<"currentTime last run "<<currentTime<<std::endl;
    std::cout<<"append new calib data valid from "<<currentTime+1<<" to iov closing time"<<std::endl;
    Pedestals* myped=new Pedestals;
    for(int ichannel=1; ichannel<=5; ++ichannel){
      Pedestals::Item item;
      item.m_mean=1.11*ichannel+currentTime;
      item.m_variance=1.12*ichannel+currentTime;
      myped->m_pedestals.push_back(item);
    }
    mydbservice->newValidityForNewPayload<Pedestals>(myped,currentTime,callbackToken);
  }catch(const cond::Exception& er){
    std::cout<<er.what()<<std::endl;
  }catch(const std::exception& er){
    std::cout<<"caught std::exception "<<er.what()<<std::endl;
  }catch(...){
    std::cout<<"Funny error"<<std::endl;
  }
}

#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(appendSinglePayload);
