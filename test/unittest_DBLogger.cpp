#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/SessionConfiguration.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/DBCommon/interface/Connection.h"
#include "CondCore/DBCommon/interface/CoralTransaction.h"
#include "CondCore/DBOutputService/interface/Logger.h"
#include "CondCore/DBOutputService/interface/UserLogInfo.h"
#include <string>
#include <iostream>
//#include <stdio.h>
//#include <time.h>

int main(){
  std::string constr("sqlite_file:mylog.db");
  cond::DBSession* session=new cond::DBSession;
  session->configuration().setMessageLevel( cond::Error );
  session->configuration().setAuthenticationMethod(cond::XML);
  cond::Connection con(constr,-1);
  session->open();
  con.connect(session);
  cond::CoralTransaction& coralTransaction=con.coralTransaction();
  coralTransaction.start(false);
  cond::Logger mylogger(coralTransaction);
  mylogger.getWriteLock();
  cond::service::UserLogInfo a;
  mylogger.createLogDBIfNonExist();
  mylogger.logOperationNow(std::string("mycontainer"),a,constr,std::string("MyPayload"),std::string("payloadToken"));
   mylogger.logFailedOperationNow(std::string("mycontainer"),a,constr,std::string("MyPayload"),std::string("payloadToken"),"EOOROR");
  mylogger.releaseWriteLock();
  coralTransaction.commit();
  con.disconnect();
  delete session;
}
