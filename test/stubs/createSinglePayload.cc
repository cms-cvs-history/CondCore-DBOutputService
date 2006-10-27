#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "CondFormats/DataRecord/interface/PedestalsRcd.h"
#include "createSinglePayload.h"
createSinglePayload::createSinglePayload(const edm::ParameterSet& iConfig)
{
  std::cout<<"createSinglePayload::createSinglePayload"<<std::endl;
}

createSinglePayload::~createSinglePayload()
{
  std::cout<<"createSinglePayload::createSinglePayload"<<std::endl;
}
/*void
createSinglePayload::analyze(const edm::Event& evt, 
			     const edm::EventSetup& evtSetup){
  edm::ESHandle<Pedestals> peds;
  evtSetup.get<PedestalsRcd>().get(peds);
}
*/
void
createSinglePayload::endJob()
{
  std::cout<<"createSinglePayload::endJob"<<std::endl;
  Pedestals* myped=new Pedestals;
  edm::Service<cond::service::PoolDBOutputService> mydbservice;
  if( !mydbservice.isAvailable() ){
    std::cout<<"PoolDBOutputService unavailable"<<std::endl;
    return;
  }
  size_t callbackToken=mydbservice->callbackToken("Pedestals");
  for(int ichannel=1; ichannel<=5; ++ichannel){
    Pedestals::Item item;
    item.m_mean=1.11*ichannel;
    item.m_variance=1.12*ichannel;
    myped->m_pedestals.push_back(item);
  }
  try{
    mydbservice->newValidityForNewPayload<Pedestals>(myped,mydbservice->endOfTime(), callbackToken);
  }catch(const cond::Exception& er){
    std::cout<<er.what()<<std::endl;
  }catch(const std::exception& er){
    std::cout<<"caught std::exception "<<er.what()<<std::endl;
  }catch(...){
    std::cout<<"Funny error"<<std::endl;
  }
}
#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(createSinglePayload);
