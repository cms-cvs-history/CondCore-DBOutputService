#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "createSinglePayload.h"
createSinglePayload::createSinglePayload(const edm::ParameterSet& iConfig)
{
  std::cout<<"createSinglePayload::createSinglePayload"<<std::endl;
}

createSinglePayload::~createSinglePayload()
{
  std::cout<<"createSinglePayload::createSinglePayload"<<std::endl;
}

void
createSinglePayload::endJob()
{
  std::cout<<"createSinglePayload::endJob"<<std::endl;
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
DEFINE_FWK_MODULE(createSinglePayload)
