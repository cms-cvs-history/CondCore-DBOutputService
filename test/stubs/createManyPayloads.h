#ifndef createManyPayloads_h
#define createManyPayloads_h
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

class createManyPayloads : public edm::EDAnalyzer {
 public:
  explicit createManyPayloads(const edm::ParameterSet& iConfig );
  ~createManyPayloads();
  virtual void analyze( const edm::Event& evt, const edm::EventSetup& evtSetup);
  virtual void endJob(){}
 private:
  // ----------member data ---------------------------
};
#endif
