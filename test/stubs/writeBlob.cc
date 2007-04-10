#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/Event.h"
#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "CondFormats/Calibration/interface/mySiStripNoises.h"
#include <boost/random/linear_congruential.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/variate_generator.hpp>
#include "writeBlob.h"
typedef boost::minstd_rand base_generator_type;
writeBlob::writeBlob(const edm::ParameterSet& iConfig):
  m_StripRecordName("mySiStripNoisesRcd")
{
}

writeBlob::~writeBlob()
{
  std::cout<<"writeBlob::writeBlob"<<std::endl;
}

void
writeBlob::analyze( const edm::Event& evt, const edm::EventSetup& evtSetup)
{
  std::cout<<"writeBlob::analyze "<<std::endl;
  base_generator_type rng(42u);
  boost::uniform_real<> uni_dist(0,1);
  boost::variate_generator<base_generator_type&, boost::uniform_real<> > uni(rng, uni_dist);
  edm::Service<cond::service::PoolDBOutputService> mydbservice;
  if( !mydbservice.isAvailable() ){
    std::cout<<"db service unavailable"<<std::endl;
    return;
  }
  try{
    mySiStripNoises* me = new mySiStripNoises;
    unsigned int detidseed=1234;
    unsigned int bsize=10;
    unsigned int nstrips=5;
    unsigned int nAPV=2;
    for (uint32_t detid=detidseed;detid<(detidseed+bsize);detid++){
      //Generate Noise for det detid
      mySiStripNoises::SiStripNoiseVector theSiStripVector;
      mySiStripNoises::SiStripData theSiStripData;
      for(unsigned short j=0; j<nAPV; ++j){
	for(unsigned int strip=0; strip<nstrips; ++strip){
	  float noiseValue=uni();
	  std::cout<<"\t encoding noise value "<<noiseValue<<std::endl;
	  theSiStripData.setData(noiseValue,false);
	  theSiStripVector.push_back(theSiStripData.Data);
	  std::cout<<"\t encoding noise as short "<<theSiStripData.Data<<std::endl;
	}
      }
      mySiStripNoises::Range range(theSiStripVector.begin(),theSiStripVector.end());
      me->put(detid,range);
    }
    if( mydbservice->isNewTagRequest(m_StripRecordName) ){
      mydbservice->createNewIOV<mySiStripNoises>(me,mydbservice->endOfTime(),m_StripRecordName);
    }else{
      mydbservice->appendSinceTime<mySiStripNoises>(me,mydbservice->currentTime(),m_StripRecordName);
    }
  }catch(const std::exception& er){
    std::cout<<"caught std::exception "<<er.what()<<std::endl;
  }catch(...){
    std::cout<<"Funny error"<<std::endl;
  }
}
#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(writeBlob);