#ifndef appendSinglePayload_h
#define appendSinglePayload_h
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

class appendSinglePayload : public edm::EDAnalyzer {
 public:
  explicit appendSinglePayload(const edm::ParameterSet& iConfig );
  ~appendSinglePayload();
  virtual void analyze( const edm::Event& evt, const edm::EventSetup& evtSetup);
  virtual void endJob();
 private:
  // ----------member data ---------------------------
};
#endif
