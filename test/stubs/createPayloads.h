#ifndef createPayloads_h
#define createPayloads_h
//#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/EDAnalyzer.h"
namespace edm{
  class ParameterSet;
  class Event;
  class EventSetup;
}

//
// class decleration
//

class createPayloads : public edm::EDAnalyzer {
 public:
  explicit createPayloads(const edm::ParameterSet& iConfig );
  ~createPayloads();
  virtual void analyze( const edm::Event& evt, const edm::EventSetup& evtSetup);
  virtual void endJob(){}
 private:
  // ----------member data ---------------------------
};
#endif
