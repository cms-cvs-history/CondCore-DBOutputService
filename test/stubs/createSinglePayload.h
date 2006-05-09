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

class createSinglePayload : public edm::EDAnalyzer {
 public:
  explicit createSinglePayload(const edm::ParameterSet& iConfig );
  ~createSinglePayload();
  virtual void analyze( const edm::Event&, const edm::EventSetup& ){}
  virtual void endJob();
 private:
  // ----------member data ---------------------------
};

