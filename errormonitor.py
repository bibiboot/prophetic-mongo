"""
The CriticalMonitor supplies summary of the critical logs once every TIMEPERIOD.
The code also emits a alert when a particular log exceeds the threshold frequency.

The critical logs are divided into 4 sections:
    1. Title       : The main heading of the critical log.
    2. Description : The subheading of the critical log.
    3. Parameters  : The request paramters or variable which aid in debugging the error.
    4. Exception   : The exception isssued from the execution.
"""
import pdb
import sys
import time
import pprint
import datetime 
from Mailer import Mailer
from pymongo import Connection

# Sender's mail id
SENDER     = "SENDER@SENDERMAIL.COM"
# Reciever's mail id
ADMIN      = [ "RECIEVER@RECIEVERMAIL.COM" ]
# Error mail summary every 30 minutes
TIMEPERIOD =  60*30
# Subject for critical log summary
SUB_SUMM = "MONGO SUMMARY"
# Subject for critical log alert
SUB_ALERT = "MONGO ALERT"

m = Mailer()

class Errormonitor():
    def __init__(self):
        # Alet tracking list
        self.error_mailed = []
        # Start time of the timeperiod.
        self.starttime = None 
        # Count the number of rows in the collection
        c = Connection().stats_db.critical.find({}).count()
        # Skip the rows in the collection to jump to realtime logs
        # critical : Collection where the critical logs are dumped
        # The collection must be capped for the tailable query to work
        self.cursor = Connection().stats_db.critical.find({},tailable=True).skip(c-1000)

    def run(self):
        """Monitor running"""
        stat_dict = {}
        while self.cursor.alive:
            try:
                doc = self.cursor.next()
                # Add data in the dictionary.
                # Check for violation.
                self.add(doc, stat_dict)
                # Check for timeperiod is over or not.
                if self.__timeover(doc['e0']):
                    # Send the summary mail as timeperiod is over.
                    self.mail_admin(stat_dict, doc['e0'])
                    # Refresh the stats dict at the 
                    # start of the timeperiod.
                    stat_dict = {}
                    #Sleep for two seconds.
                    time.sleep(2)
                    # Set the start time of the timeperiod.
                    self.starttime = doc['e0']
                    # Refresh the alert tracking list.
                    self.error_mailed = []
            except StopIteration:
                time.sleep(1)

    def add(self, doc, stat_dict):
        """
        Adds the currrent stream of log from the tail to the dict.
        Check if the dict created has any exception which has
        been violated i.e. needs an alert to be triggered.
        """
        alert_dict = {}
        if not doc.has_key('e4'):
            #Data not in current format
            return
        # Default exception where exception is not recorded
        exception = 'NA'
        # Remove the special characters like ',<,>
        title = doc['e3'].replace("'","")
        title = title.replace("<","")
        title = title.replace(">","")
        description = doc['e4'].replace("'","")
        description = description.replace("<","")
        description = description.replace(">","")
        if doc.has_key('e6'):
            exception = doc['e6'].replace("'","")
            exception = exception.replace(">","")
            exception = exception.replace("<","")
        #data = doc['e5']
        if stat_dict.has_key(title):
            if stat_dict[title][1].has_key(description):
                if stat_dict[title][1][description][1].has_key(exception):
                    #increment the exception count
                    stat_dict[title][1][description][1][exception][0] = stat_dict[title][1][description][1][exception][0] + 1 
                else:
                    #initialize the exception
                    stat_dict[title][1][description][1][exception] = [1]
                #increment the description
                stat_dict[title][1][description][0] = stat_dict[title][1][description][0] + 1
            else:
                #initialize description
                stat_dict[title][1][description] = [ 1,
                                                     { exception: [1] }
                                                   ]
            #increment title count
            stat_dict[title][0] = stat_dict[title][0] + 1
        else:
            #initialize title
            stat_dict[title] = [ 1, 
                                 { description: [ 1,
                                                  { exception: [1] } 
                                                ] 
                                 } 
                               ]
        # Check for violation
        self.violated(stat_dict)

    def __timeover(self, timestamp):
        """
        Check if Time period is over or not
        """
        if not self.starttime:
            # In the first loop, the self.starttime is not defined.
            self.starttime = timestamp
        if (timestamp - self.starttime).seconds > TIMEPERIOD:
            # Time period matches, send the data for analysis
            return True
        return False

    def mail_allowed(self):
        """
        Check the certain constraints to trigger mail
        """
        # Mail should be triggered for todays log
        # This wil prevent log of past which are not skipped in the query from 
        # triggering mails.
        if (datetime.datetime.now().day!=self.starttime.day):
            return False
        # Mails must be triggered fom the last 2hours mails.
        # Therefore when the monitor is started,
        # the mails will be shooted 2hours before the current time.
        if  (datetime.datetime.now() - self.starttime).seconds > 60*60*2:
            return False
        # Mail can be triggered.
        return True

    def violated(self, stat_dict):
        """
        Check if any exception is violated.
        Also appends the violated exception in a temporary list
        to prevent it from flooding the mail server during the 
        time period
        """
        for title, title_list in stat_dict.items():
            title_count = title_list[0]
            for desc, desc_list in title_list[1].items():
                desc_count = desc_list[0]
                for exception, exception_list in desc_list[1].items():
                    excp_count = exception_list[0]
                    if excp_count > 100:
                        alert_dict = {}
                        # If the alert has already been triggered for the 
                        # exception in the current timeperiod
                        # then pass
                        if exception in self.error_mailed:
                            pass
                        else:
                            alert_dict[title] = [ excp_count, 
                                                  { desc: [ excp_count,
                                                            { exception: [excp_count] } 
                                                          ] 
                                                  } 
                                                ]
                            # Create body of the alert and send mail.
                            self.mail_admin(alert_dict, self.starttime,True)
                            # Temporary list created to keep track of 
                            # alert exceptions triggered during the timeperiod.
                            self.error_mailed.append(exception)
                        

    def mail_admin(self, stat_dict, timestamp, violated=False):
        """
        Creates a body of mail for 
        critical log summary and critical log alert
        """

        day = "%(#)02d" % { "#":timestamp.hour }+':'+"%(#)02d" % { "#": timestamp.minute }+' '+str(timestamp.day)+' '+timestamp.strftime("%B")+' '+str(timestamp.year)
        if violated:
            # Alert mail violation
            subject = SUB_ALERT
            interval = "realtime stream"
        else:
            # Critical summary
            subject = SUB_SUMM
            interval = "every "+str(int(TIMEPERIOD)/60)+ " minutes"

        body = "<html>\
                <head>\
                  <style>\
                  .body {\
                        font-family:arial;\
                        font-size:6px;\
                        font-family:arial;\
                        color:black;\
                        }\
                  .basic {\
                          vertical-align:top;\
                         }\
                  .span data {\
                              float:left;\
                            }\
                  .span num {\
                            float:right;\
                            }\
                  #colr{\
                       background-color:#E8EDFF\
                       }\
                  table tr td{\
                             border: 1px solid #69C;\
                             }\
                  table th{\
                          border: 1px solid #69C;\
                          background-color:#69C;\
                          color: white;\
                          }\
                  </style>\
                <body>\
                  <h3>Critical log, %s at %s</h3>\
                  <table cellspacing=0 cellpadding=6>\
                    <th>Title</th><th>No.</th><th>Description</th><th>No.</th><th>Exception</th><th>No.</th>\
                " % (interval, day)

        for title, title_list in stat_dict.items():
            exp_result = []
            dsc_result = []
            td_exp_label, td_exp_num = "", ""
            td_dsc_label, td_dsc_num = "", ""

            body+="<tr>"
            body+="<td class='%s'>\
                     <div>\
                       <span class='data'>&#8227;&nbsp;%s</span>\
                     </div>\
                   </td>\
                   " % ( 'basic', title )

            body+="<td class='%s'>\
                     <div >\
                       <span class='num'>&#8227;&nbsp;%d</span>\
                     </div>\
                   </td>\
                   " % ( 'basic', int(title_list[0]) )

            for desc, desc_list in title_list[1].items():
                breaklines = len(desc_list[1])
                breakhtml= "<br/>" * breaklines
                dsc_label="<div>\
                             <span class='data'>&#8227;&nbsp;%s</span>\
                             %s\
                           </div>\
                           " % ( desc, breakhtml )

                dsc_num="<div>\
                           <span class='num'>&#8227;&nbsp;%d</span>\
                           %s\
                         </div>\
                         " % ( int(desc_list[0]), breakhtml )
                dsc_result.append((dsc_label, dsc_num))
                for exp, exp_list in desc_list[1].items():
                    exp_label="<div>\
                                 <span class='data'>&#8227;&nbsp;%s</span>\
                               </div>\
                               " % ( exp )

                    exp_num="<div>\
                               <span class='num'>&#8227;&nbsp;%d</span>\
                             </div>\
                             " % ( exp_list[0] )
                    exp_result.append((exp_label, exp_num))

            for html in exp_result:
                td_exp_label+=html[0]
                td_exp_num+=html[1]
            for html in dsc_result:
                td_dsc_label+=html[0]
                td_dsc_num+=html[1]
            
            body+="<td class='basic'>" + td_dsc_label + "</td>"
            body+="<td class='basic'>" + td_dsc_num + "</td>"
            body+="<td class='basic'>" + td_exp_label + "</td>"
            body+="<td class='basic'>" + td_exp_num + "</td>"
            body+="</tr>"
 
        body+="</table><br/>&#8227;&nbsp;NA stands for exception not recorded.<br/>\
                <i>&#8227;&nbsp;If debugging is the process of removing bugs, then programming must be the process of putting them in.</i>\
                 </body></html>"

        if self.mail_allowed():
            # Send mail
            m.sendmail(type='html', recipients = ADMIN,
                       subject = subject,
                       body = body )

if __name_ == '__main__':
    e = Errormonitor()
    e.run()
