using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
 
namespace TMSSMS
{
    class MainApp
    {
        // Logger
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
     
        /// <summary>
        /// Main
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            log.Info("################################# START #################################");
            if (args == null || args.Length == 0)
            {
                Console.WriteLine("SMS 발송 구분 값(Single, Campaign)을 지정해 주십시오." + Environment.NewLine + 
                                  "e.g. 개별 발송: TMSSMS.exe Single / 캠페인 발송: TMSSMS.exe Campaign");
            }
            else
            {
                string argument = args[0].ToString();//"campaign";//"Single";


                if (argument.ToLower() == "campaign") //캠페인 대량 발송
                {
                    CampaignSMS CmpgnSMS = new CampaignSMS();

                    CmpgnSMS.CampaignStart();
                }
                else if (argument.ToLower() == "single") //개별 발송
                {

                    SingleSMS SngSMS = new SingleSMS();

                    SngSMS.SingleStart();
                }

                log.Info("################################# END #################################");
            }
        }
    }
}
