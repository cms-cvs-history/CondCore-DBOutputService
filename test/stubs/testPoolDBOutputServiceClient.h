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

class testPoolDBOutputServiceClient : public edm::EDAnalyzer {
 public:
  explicit testPoolDBOutputServiceClient(const edm::ParameterSet& iConfig );
  ~testPoolDBOutputServiceClient();

  virtual void analyze(const edm::Event&, const edm::EventSetup&);
  virtual void endJob();
 private:
  // ----------member data ---------------------------
};

