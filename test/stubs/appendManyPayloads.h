#ifndef appendManyPayloads_h
#define appendManyPayloads_h
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

class appendManyPayloads : public edm::EDAnalyzer {
 public:
  explicit appendManyPayloads(const edm::ParameterSet& iConfig );
  ~appendManyPayloads();
  virtual void analyze( const edm::Event& evt, const edm::EventSetup& evtSetup);
  virtual void endJob();
 private:
  // ----------member data ---------------------------
};
#endif
