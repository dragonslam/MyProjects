using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using System.Configuration;
using Biz.CM;
using TMSSMS;
using System.Collections;
using System.Data;
using System.Threading.Tasks;
using Oracle.DataAccess.Client;

namespace TMSSMS
{
    class CampaignSMS
    {
        //로그
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// 캠페인 발송 로그 조회
        /// </summary>
        public void CampaignStart()
        {
            Biz.CM.CMXmlAutoBizTx oBiz = null;
            Biz.CM.CMXmlAutoBiz oBiz2 = null;
            DataSet ds = null;
            int taskIdx = 0;

            try
            {

                #region 캠페인 SMS 발송 Function
                Func<object, int> action = (object oSms) =>
                {
                    int oraResult = 0;
                    string insertSQL = "";
                    // string insertValues = "";
                    CMXmlAutoBizTx bizTx = null;
                    StringBuilder sbQuery = new StringBuilder();
                    string oraConn = ConfigurationManager.AppSettings["OraConnStr"];
                    int totRows = 0;
                    int rowIdx = 0;
                    string minId = "";
                    string maxId = "";
                    string dataRange = "";

                    // SMS TRAN TABLE
                    string sc_tran_table = ConfigurationManager.AppSettings["SMS_TRAN_TABLE"];
                    // SMS SEQUENCE
                    string sc_sequence = ConfigurationManager.AppSettings["SMS_SEQUENCE"];

                    DataTable dt = (DataTable)oSms;

                    Hashtable ht = new Hashtable();
                    OracleConnection oCon = new OracleConnection(oraConn);
                    OracleTransaction oTx = null;
                    OracleCommand cmd = oCon.CreateCommand();

                    try
                    {
                        // 전체 대상 건수
                        totRows = dt.Rows.Count;

                        // 시작ID
                        maxId = dt.Rows[0]["CMPGNSMSLOGID"].ToString();
                        // 종료ID
                        minId = dt.Rows[totRows - 1]["CMPGNSMSLOGID"].ToString();

                        // 작업 대상 ID
                        dataRange = "CMPGNSMSLogId> " + minId + " ~ " + maxId;

                        try
                        {
                            // Connection Open.
                            oCon.Open();

                            // 데이터 저장 Array 생성 
                            /* TR_SENDDATE      DATE
                             * TR_PHONE         VARCHAR2(20)
                             * TR_CALLBACK      VARCHAR2(20)
                             * TR_MSG           VARCHAR2(160)
                             * TR_ETC3          VARCHAR2(160)
                             */
                            DateTime[] sendDateVals = new DateTime[totRows];
                            string[] sendStatVals = new string[totRows];
                            string[] msgTypeVals = new string[totRows];
                            string[] phoneVals = new string[totRows];
                            string[] callBackVals = new string[totRows];
                            string[] msgVals = new string[totRows];
                            string[] etc1Vals = new string[totRows];
                            string[] etc2Vals = new string[totRows];
                            string[] etc3Vals = new string[totRows];

                            // 데이터 저장 
                            foreach (DataRow dr in dt.Rows)
                            {
                                sendDateVals[rowIdx] = Convert.ToDateTime(dr["ReqDate"].ToString());
                                sendStatVals[rowIdx] = "0";
                                msgTypeVals[rowIdx] = "0";
                                phoneVals[rowIdx] = dr["Mobile"].ToString();
                                callBackVals[rowIdx] = dr["ReplyTelNo"].ToString();
                                msgVals[rowIdx] = dr["SendMessage"].ToString();
                                etc1Vals[rowIdx] = "TMS";
                                etc2Vals[rowIdx] = "Campaign";
                                etc3Vals[rowIdx] = dr["CMPGNSMSLogId"].ToString();

                                rowIdx++;
                            }

                            // Parameter 생성
                            OracleParameter pSendDate = new OracleParameter();
                            pSendDate.OracleDbType = OracleDbType.Date;
                            pSendDate.Value = sendDateVals;

                            OracleParameter pSendStat = new OracleParameter();
                            pSendStat.OracleDbType = OracleDbType.Varchar2;
                            pSendStat.Value = sendStatVals;

                            OracleParameter pMsgType = new OracleParameter();
                            pMsgType.OracleDbType = OracleDbType.Varchar2;
                            pMsgType.Value = msgTypeVals;

                            OracleParameter pPhone = new OracleParameter();
                            pPhone.OracleDbType = OracleDbType.Varchar2;
                            pPhone.Value = phoneVals;

                            OracleParameter pCallBack = new OracleParameter();
                            pCallBack.OracleDbType = OracleDbType.Varchar2;
                            pCallBack.Value = callBackVals;

                            OracleParameter pMsg = new OracleParameter();
                            pMsg.OracleDbType = OracleDbType.Varchar2;
                            pMsg.Value = msgVals;

                            OracleParameter pEtc1 = new OracleParameter();
                            pEtc1.OracleDbType = OracleDbType.Varchar2;
                            pEtc1.Value = etc1Vals;

                            OracleParameter pEtc2 = new OracleParameter();
                            pEtc2.OracleDbType = OracleDbType.Varchar2;
                            pEtc2.Value = etc2Vals;

                            OracleParameter pEtc3 = new OracleParameter();
                            pEtc3.OracleDbType = OracleDbType.Varchar2;
                            pEtc3.Value = etc3Vals;

                            oTx = oCon.BeginTransaction(IsolationLevel.ReadCommitted);

                            cmd.Transaction = oTx;
                            cmd.CommandType = CommandType.Text;

                            insertSQL = @"INSERT INTO " + sc_tran_table + " (TR_NUM"
                                          + ", TR_SENDDATE, TR_SENDSTAT, TR_MSGTYPE"
                                          + ", TR_PHONE,    TR_CALLBACK, TR_MSG"
                                          + ", TR_ETC1,     TR_ETC2,     TR_ETC3)";

                            insertSQL = insertSQL + @" VALUES(" + sc_sequence + ".NEXTVAL"
                                                  + ", :1, :2, :3"
                                                  + ", :4, :5, :6 "
                                                  + ", :7, :8, :9)";


                            cmd.CommandText = insertSQL;

                            cmd.ArrayBindCount = totRows;

                            cmd.Parameters.Add(pSendDate);
                            cmd.Parameters.Add(pSendStat);
                            cmd.Parameters.Add(pMsgType);
                            cmd.Parameters.Add(pPhone);
                            cmd.Parameters.Add(pCallBack);
                            cmd.Parameters.Add(pMsg);
                            cmd.Parameters.Add(pEtc1);
                            cmd.Parameters.Add(pEtc2);
                            cmd.Parameters.Add(pEtc3);

                            // Insert 실행
                            oraResult = cmd.ExecuteNonQuery();


                            if (oraResult > 0)
                            {
                                oTx.Commit();

                                log.Info(dataRange + " - 캠페인발송>SMS DB Insert> 성공");

                                ht.Clear();
                                ht.Add("SENDRESULT", "20");
                                ht.Add("MINID", minId);
                                ht.Add("MAXID", maxId);

                                bizTx = new CMXmlAutoBizTx();
                                if (bizTx.XmlNonExecute("uspCampaignSMSLogSendAfter", ht) > 0)
                                {
                                    log.Info(dataRange + " - 캠페인발송>발송결과(20) 갱신 성공");
                                }
                                else
                                {
                                    log.Error(dataRange + " - 캠페인발송>발송결과(20) 갱신 실패");
                                }
                            }
                            else
                            {
                                ht.Clear();
                                ht.Add("SENDRESULT", "40");
                                ht.Add("MINID", minId);
                                ht.Add("MAXID", maxId);

                                bizTx = new CMXmlAutoBizTx();
                                if (bizTx.XmlNonExecute("uspCampaignSMSLogSendAfter", ht) > 0)
                                {
                                    log.Error(dataRange + " - 캠페인발송>발송 실패, 발송결과(40) 갱신 성공");
                                }
                                else
                                {
                                    log.Error(dataRange + " - 캠페인발송>발송 실패, 발송결과(40) 갱신 실패");
                                }
                            }

                        }
                        catch (Exception ex)
                        {
                            log.Error(dataRange + " - 캠페인발송>SMS DB Insert 실패>Task Sub" + ex.ToString());

                            oTx.Rollback();

                            ht.Clear();
                            ht.Add("SENDRESULT", "40");
                            ht.Add("MINID", minId);
                            ht.Add("MAXID", maxId);

                            bizTx = new CMXmlAutoBizTx();
                            if (bizTx.XmlNonExecute("uspCampaignSMSLogSendAfter", ht) > 0)
                            {
                                log.Error(dataRange + " - 캠페인발송>발송 실패, 발송결과(40) 갱신 실패");
                            }
                            else
                            {
                                log.Error(dataRange + " - 캠페인발송>발송 실패, 발송결과(40) 갱신 실패");
                            }
                        }
                        finally
                        {
                            oCon.Close();
                            cmd.Dispose();
                        }
                    }
                    catch (Exception ex)
                    {
                        log.Error(dataRange + " - 캠페인발송>SMS DB Insert 실패>Task Main" + ex.ToString());

                        ht.Clear();
                        ht.Add("SENDRESULT", "40");
                        ht.Add("MINID", minId);
                        ht.Add("MAXID", maxId);

                        bizTx = new CMXmlAutoBizTx();
                        if (bizTx.XmlNonExecute("uspCampaignSMSLogSendAfter", ht) > 0)
                        {
                            log.Error(dataRange + " - 캠페인발송>발송 실패, 발송결과(40) 갱신 성공");
                        }
                        else
                        {
                            log.Error(dataRange + " - 캠페인발송>발송 실패, 발송결과(40) 갱신 실패");
                        }
                    }

                    return oraResult;
                };
                #endregion 캠페인 SMS 발송 Function

                #region 캠페인 SMS 로그 조회 Task

                bool sendCheck = false;
                int iResult = -1;

                Task<int>[] tasks = new Task<int>[1];

                while (sendCheck == false)
                {
                    oBiz = new CMXmlAutoBizTx();

                    iResult = oBiz.XmlNonExecute("uspCampaignSMSLogStand");

                    if (iResult < 1)
                    {
                        sendCheck = true;
                        break;
                    }
                    else
                    {
                        oBiz2 = new CMXmlAutoBiz();
                        ds = oBiz2.GetData("uspCampaignSMSLogSend");

                        log.Info(taskIdx.ToString() + " - 캠페인 발송대상 데이터 건수> " + ds.Tables[0].Rows.Count.ToString());

                        if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                        {
                            tasks[taskIdx] = Task<int>.Factory.StartNew(action, ds.Tables[0]);
                            taskIdx++;

                            Array.Resize(ref tasks, taskIdx + 1);

                            continue;
                        }
                        else
                        {
                            sendCheck = true;
                            break;
                        }

                    }

                }

                if (tasks.Length > 0)
                {
                    // 작업 종료할 때까지 대기 
                    if (tasks.GetValue(0) != null)
                    {

                        if (taskIdx > 0)
                        {
                            Array.Resize(ref tasks, taskIdx);
                        }

                        Task.WaitAll(tasks);
                    }
                    else
                    {
                        log.Info(taskIdx.ToString() + " - 캠페인 발송할 데이터 없음");
                    }
                }

                log.Info("캠페인 발송 작업 완료");

                #endregion 캠페인 SMS 로그 조회 Task
            }
            catch (Exception ex)
            {
                log.Error(taskIdx.ToString() + " - 캠페인발송>발송 작업 실패>" + ex.ToString());
            }
            finally
            {
                if (oBiz != null)
                    oBiz = null;
                if (oBiz2 != null)
                    oBiz2 = null;
            }
        }
    }
}
