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
  std::cout<<"appendSinglePayload::appendSinglePayload"<<std::endl;
}
void
appendSinglePayload::analyze(const edm::Event& evt, 
			     const edm::EventSetup& evtSetup){
  edm::ESHandle<Pedestals> peds;
  evtSetup.get<PedestalsRcd>().get(peds);
}
void
appendSinglePayload::endJob()
{
  std::cout<<"appendSinglePayload::endJob"<<std::endl;
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
      mydbservice->newValidityForNewPayload<Pedestals>(myped,mydbservice->endOfTime());
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
#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(appendSinglePayload)
