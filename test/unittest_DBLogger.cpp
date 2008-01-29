#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/SessionConfiguration.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/DBCommon/interface/Connection.h"
#include "CondCore/DBCommon/interface/CoralTransaction.h"
#include "CondCore/DBCommon/interface/TokenBuilder.h"
//#include "CondCore/DBCommon/interface/TokenInterpreter.h"
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

  std::string constr("sqlite_file:mylog.db");
  cond::DBSession* session=new cond::DBSession;
  session->configuration().setMessageLevel( cond::Error );
  session->configuration().setAuthenticationMethod(cond::XML);
  cond::Connection con(constr,-1);
  session->open();
  con.connect(session);
  //cond::CoralTransaction& coralTransaction=con.coralTransaction();
  // coralTransaction.start(false);
  cond::Logger mylogger(&con);
  mylogger.getWriteLock();
  cond::service::UserLogInfo a;
  mylogger.createLogDBIfNonExist();
  mylogger.logOperationNow(a,constr,tok1,"mytag1","runnumber");
  mylogger.logFailedOperationNow(a,constr,tok1,"mytag1","runnumber","EOOROR");
  mylogger.releaseWriteLock();
  //coralTransaction.commit();
  con.disconnect();
  delete session;
}
