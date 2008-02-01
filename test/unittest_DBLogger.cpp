#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/SessionConfiguration.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/DBCommon/interface/Connection.h"
#include "CondCore/DBCommon/interface/CoralTransaction.h"
#include "CondCore/DBCommon/interface/TokenBuilder.h"
#include "CondCore/DBOutputService/interface/LogDBEntry.h"
#include "CondCore/DBOutputService/interface/Logger.h"
#include "CondCore/DBOutputService/interface/UserLogInfo.h"

#include <string>
#include <iostream>
//#include <stdio.h>
//#include <time.h>

int main(){
  cond::TokenBuilder tk;
  tk.set("3E60FA40-D105-DA11-981C-000E0C4DE431",
	 "CondFormatsCalibration",
	 "Pedestals",
	 "PedestalsRcd",
	 0);
  std::string tok1=tk.tokenAsString();
  tk.set("3E60FA40-D105-DA11-981C-000E0C4DE431",
	 "CondFormatsCalibration",
	 "Pedestals",
	 "PedestalsRcd",
	 1);
  std::string tok2=tk.tokenAsString();
  std::string constr("sqlite_file:mylog.db");
  cond::DBSession* session=new cond::DBSession;
  session->configuration().setMessageLevel( cond::Debug );
  session->configuration().setAuthenticationMethod(cond::XML);
  cond::Connection con(constr,-1);
  session->open();
  con.connect(session);
  //cond::CoralTransaction& coralTransaction=con.coralTransaction();
  // coralTransaction.start(false);
  cond::Logger mylogger(&con);
  mylogger.getWriteLock();
  cond::service::UserLogInfo a;
  a.provenance="me";
  mylogger.createLogDBIfNonExist();
  mylogger.logOperationNow(a,constr,tok1,"mytag1","runnumber",0);
  mylogger.logFailedOperationNow(a,constr,tok1,"mytag1","runnumber",1,"EOOROR");
  mylogger.logOperationNow(a,constr,tok2,"mytag1","runnumber",1);
  mylogger.releaseWriteLock();
  std::cout<<"about to lookup last entry"<<std::endl;
  cond::LogDBEntry result;
  mylogger. LookupLastEntryByProvenance("me",result);
  std::cout<<"result \n";
  std::cout<<"logId "<<result.logId<<"\n";
  std::cout<<"destinationDB "<<result.destinationDB<<"\n";
  std::cout<<"provenance "<<result.provenance<<"\n";
  std::cout<<"comment "<<result.comment<<"\n";
  std::cout<<"iovtag "<<result.iovtag<<"\n";
  std::cout<<"iovtimetype "<<result.iovtimetype<<"\n";
  std::cout<<"payloadIdx "<<result.payloadIdx<<"\n";
  std::cout<<"payloadName "<<result.payloadName<<"\n";
  std::cout<<"payloadToken "<<result.payloadToken<<"\n";
  std::cout<<"payloadContainer "<<result.payloadContainer<<"\n";
  std::cout<<"exectime "<<result.exectime<<"\n";
  std::cout<<"execmessage "<<result.execmessage<<std::endl;
  //cond::LogDBEntry result;
  //mylogger. LookupLastEntryByProvenance("me",result);
  //coralTransaction.commit();
  con.disconnect();
  delete session;
}
