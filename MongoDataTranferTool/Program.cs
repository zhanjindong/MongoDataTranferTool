using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using iFlytek.ComLibrary.Utility;
using MongoDB.Bson;
using MongoDB.Driver;
using OSSP.BLIService.DataTransferDll;
using System.IO;
using System.Net.Mail;
using DataTransferDll;

namespace OSSP.BLIService.DataTranferTool
{
    public class Program
    {
        /// <summary>
        /// 存储全局配置信息
        /// </summary>
        private static ConfigContainer configContainer = null;
        /// <summary>
        /// Xml配置文件存储路径
        /// </summary>
        private static string currentXmlPath = null;
        /// <summary>
        /// 只能启动一个程序实例
        /// </summary>
        public static Mutex singleOnly;
        /// <summary>
        /// 是否发生全局性错误
        /// </summary>
        public static bool isGlobalError = false;
        /// <summary>
        /// 任务列表
        /// </summary>
        private static List<TaskConfig> taskList = new List<TaskConfig>();
        /// <summary>
        /// 线程列表
        /// </summary>
        private static List<BaseThread> threadList = new List<BaseThread>();

        /// <summary>
        /// 主函数
        /// </summary>
        static void Main(string[] args)
        {
            try
            {
                ////只可启动一个实例
                //bool isRun;
                //singleOnly = new Mutex(true, "iFlyTek.OSSP.DataTransferTool", out isRun);
                //if (!singleOnly.WaitOne(1000, false) || !isRun)
                //{
                //    Console.WriteLine("无法启动多个实例,当前DataTransferTool已经启动,退出");
                //    return;
                //}

                //注册未处理事件函数
                AppDomain.CurrentDomain.UnhandledException += new System.UnhandledExceptionEventHandler(AppDomain_UnHandledException);

                //注册关闭事件处理函数
                CloseCtrlHandler.RegisterCtrlHandler((CallbackDelegate)EmergencyProcess);

                GetTickCount getTickCount = new GetTickCount();
                getTickCount.BeginRecordTime();

                LogHelper.MergeLog();

                LogHelper.WriteLogToFile(0, null, string.Format("--------------------开始{0}转移任务--------------------",
                    DateTime.Now.ToShortDateString()));
                LogHelper.WriteLogToFile(0, null, string.Format("步骤1：获取全局配置信息...\r\n"));

                //获取全局配置信息
                currentXmlPath = System.Environment.CurrentDirectory + @"\xml";
                configContainer = new ConfigContainer();
                configContainer.GetGlobalConfig(currentXmlPath + @"\global.xml");

                //同步map配置文件
                LogHelper.WriteLogToFile(0, null, string.Format("步骤2：同步map配置文件...\r\n"));
                SyncXmlStructure();


                //建立任务列表
                LogHelper.WriteLogToFile(0, null, string.Format("步骤3：生成任务列表...\r\n"));
                CreateTaskList();
                PrintTaskList();

                //转移数据
                LogHelper.WriteLogToFile(0, null, string.Format("步骤4：转移数据...\r\n"));
                TransferData();

                LogHelper.WriteLogToFile(0, null, string.Format("步骤5：合并日志文件...\r\n"));
                LogHelper.MergeLog();

                //删除数据，转移方式是剪切时，将自动删除过期数据     
                LogHelper.WriteLogToFile(0, null, string.Format("步骤6：删除过期数据...\r\n"));
                LogHelper.TryRemoveProgressLog(@".\log\progresslog\progresslog.log",
                    configContainer.GetTransferDate().ToShortDateString());
                if ("0" == configContainer.transferType)
                {
                    DeleteData();
                }

                //通知
                LogHelper.WriteLogToFile(0, null, string.Format("步骤7：发送邮件通知...\r\n"));
                LogHelper.WriteLogToFile(0, null, string.Format("--------------------{0}转移任务完成，耗时：{1} ms--------------------",
                    DateTime.Now.ToShortDateString(), getTickCount.EndRecordTime()));
                Notify();
            }
            catch (System.Exception ex)
            {
                string dataTransferError = string.Format("--------------------转移数据过程出错，异常信息：{0}--------------------",
                    ex.ToString());
                LogHelper.WriteLogToFile(0, null, dataTransferError);
                Console.WriteLine(dataTransferError);
            }
        }

        #region 突发事件处理

        /// <summary>
        /// 捕获未处理异常
        /// </summary>
        private static void AppDomain_UnHandledException(object sender, System.UnhandledExceptionEventArgs e)
        {
            string strUnHandledException = string.Format("捕获未处理异常，异常信息：{0}",
                (e.ExceptionObject as System.Exception).Message);
            LogHelper.WriteLogToFile(0, null, strUnHandledException);

            EmergencyProcess();
        }

        /// <summary>
        /// 突发事件，保存相关信息
        /// </summary>
        private static void EmergencyProcess()
        {
            foreach (BaseThread bt in threadList)
            {
                LogHelper.SetProgressLog(bt.GetConfigArgs().ProgressLogPath, bt.GetConfigArgs().TaskItem.TaskId, bt.GetConfigArgs().TaskItem.TableName,
                    Convert.ToDateTime(bt.GetConfigArgs().TransferDate).ToShortDateString(),
                    Convert.ToInt64(bt.GetConfigArgs().HaveTransferedRowCount));
            }

            LogHelper.MergeLog();
        }

        #endregion

        #region 生成本次转移任务列表

        /// <summary>
        /// 建立任务列表
        /// </summary>
        private static void CreateTaskList()
        {
            List<int> taskIds = new List<int>();

            //加载global.xml中配置的转移任务
            foreach (TaskItem taskItem in configContainer.taskItemList.Values)
            {
                TaskConfig configArgs = configContainer.CreateConfigArgs(
                    taskItem.TaskId, configContainer.GetTransferDate().ToShortDateString());
                if (configArgs.HaveTransferedRowCount != "-1")
                {
                    taskList.Add(configArgs);
                }
            }

            #region 加载日志中记录的没有转移完的但是又不在当前转移任务列表(global.xml)中的任务或者跟当前的转移日期不同。
            //注释掉原因是只需要执行用户配置的转移任务而不应该暗地里操作一些不为人知的事情 jdzhan 2013-01-17
            //string progressLogPath = @".\log\progresslog\progresslog.log";

            //if (string.IsNullOrEmpty(progressLogPath) ||
            //           (!File.Exists(progressLogPath)))
            //{
            //    return;
            //}
            //else
            //{
            //    FileStream fs = null;
            //    StreamReader sr = null;
            //    try
            //    {
            //        fs = new FileStream(progressLogPath, FileMode.Open);
            //        sr = new StreamReader(fs);

            //        string oneLine = null;
            //        while ((oneLine = sr.ReadLine()) != null)
            //        {
            //            try
            //            {
            //                if (!string.IsNullOrEmpty(oneLine))
            //                {
            //                    TaskConfig configArgs = configContainer.CreateConfigArgs(
            //               oneLine.Split(':')[0], oneLine.Split(':')[2]);

            //                    if ((oneLine.Split(':')[2] != Convert.ToDateTime(configContainer.GetTransferDate()).ToShortDateString())
            //                        && (oneLine.Split(':')[3] != "-1"))
            //                    {
            //                        taskList.Add(configArgs);
            //                    }
            //                }
            //            }
            //            catch (System.Exception ex)
            //            {
            //                isGlobalError = true;
            //                LogHelper.WriteLogToFile(0, null, string.Format("建立任务列表出错，错误信息：{0}", ex.Message));
            //            }
            //        }
            //    }
            //    catch (System.Exception ex)
            //    {
            //        isGlobalError = true;
            //        LogHelper.WriteLogToFile(0, null, string.Format("建立任务列表出错，错误信息：{0}", ex.Message));
            //    }
            //    finally
            //    {
            //        sr.Close();
            //        fs.Close();
            //    }
            //}
            #endregion
        }

        /// <summary>
        /// 打印出任务列表
        /// </summary>
        private static void PrintTaskList()
        {
            LogHelper.WriteLogToFile(0, null, "任务列表如下：");
            foreach (TaskConfig configargs in taskList)
            {
                string strTask = string.Format("任务ID：{0}，表名：{1}，日期：{2}，开始转移行数：{3}",
                 configargs.TaskItem.TaskId, configargs.TaskItem.TableName, configargs.TransferDate, configargs.HaveTransferedRowCount + 1);
                LogHelper.WriteLogToFile(0, null, strTask);
            }
            LogHelper.WriteLogToFile(0, null, "");
        }

        #endregion

        #region 转移数据

        /// <summary>
        /// 转移数据
        /// </summary>
        private static void TransferData()
        {
            foreach (TaskConfig configArgs in taskList)
            {
                IDataTransfer dataTransferInstance = null;
                try
                {
                    dataTransferInstance = DataTransferFactory.GetDataTransferInstance(configArgs, configArgs.TaskItem.TransferDirect);
                    dataTransferInstance.ReadyForDataTransfer();

                    BaseThread btRead = new ReadFromSourceHandler(dataTransferInstance);
                    btRead.Start();

                    BaseThread btWrite = new WriteToDestinyHandler(dataTransferInstance);
                    threadList.Add(btWrite);
                    btWrite.Start();
                }
                catch (System.Exception ex)
                {
                    isGlobalError = true;
                    string createThreadError = string.Format(@"开启读写线程过程出错，异常信息：{0}",
                        ex.ToString());
                    LogHelper.WriteLogToFile(1, configArgs.TaskItem.TableName, createThreadError);
                    Console.WriteLine(createThreadError);
                }
            }

            //等待转移数据完成
            while (true)
            {
                int finishedThreadCount = 0;
                foreach (BaseThread bt in threadList)
                {
                    if (bt.IsFinished() == true)
                    {
                        finishedThreadCount++;
                    }
                }

                //所有的执行线程都执行结束了，结束主线程
                if (threadList.Count == finishedThreadCount)
                {
                    break;
                }
                else
                {
                    Thread.Sleep(1000);
                }
            }
        }

        #endregion

        #region 同步xml配置文件
        private static void SyncXmlStructure()
        {

        }
        #endregion

        #region 删除过期数据

        /// <summary>
        /// 删除所有转移的mongodb服务器上的过期数据
        /// </summary> 
        private static void DeleteData()
        {
            foreach (var task in taskList)
            {
                //只当转移方向是MongoToSql的时候才删除mongodb上过期的数据
                if (task.TaskItem.TransferDirect == (int)TransferDirect.MongoToSql)
                {
                    DeleteExpiredData(task);
                }
            }
        }

        /// <summary>
        /// 删除mongoDB中过期数据
        /// </summary>
        private static void DeleteExpiredData(TaskConfig taskConfig)
        {
            try
            {
                GetTickCount getTickCountDel = new GetTickCount();
                int timesDel = 0;

                DateTime currentDate = DateTime.Now.Date;
                if (configContainer.isAutoTransfer == "1")
                {
                    currentDate = currentDate.AddDays(-1);
                }
                else
                {
                    currentDate = DateTime.Parse(configContainer.transferDate);
                }
                DateTime delDate = currentDate.AddDays(-Convert.ToInt32(configContainer.deleteDay));

                getTickCountDel.BeginRecordTime();
                string deleteBegin = string.Format("开始删除转移任务{0}的mongoDB:{1}中{2}的日志数据", taskConfig.TaskItem.TaskId, taskConfig.TaskItem.MongodbSrc, delDate.ToShortDateString());
                LogHelper.WriteLogToFile(0, null, deleteBegin);
                Console.WriteLine(deleteBegin);

                //初始化mongoHelper
                MongoDBHelper mongoHelper = new MongoDBHelper(taskConfig.TaskItem.MongodbSrc, taskConfig.TaskItem.MongodbDatabase);
                mongoHelper.Connect();

                //批量Drop，删除前要判断此表消息是否已经转完
                foreach (string dataTableName in mongoHelper.GetDataTableNames())
                {
                    int index = dataTableName.IndexOf('_');
                    if (index != -1)
                    {
                        string dataTableNameAfterTruncate = dataTableName.Split('_')[0];
                        string dataTableTransferDate = dataTableName.Substring(index + 1);
                        string dataTableTransferDateAfterReplace = dataTableTransferDate.Replace("_", "-");

                        if (CanDelete(@".\log\progresslog\progresslog.log", taskConfig.TaskItem.TaskId, dataTableNameAfterTruncate,
                            (Convert.ToDateTime(dataTableTransferDateAfterReplace)).ToShortDateString(),
                            delDate.ToShortDateString()))
                        {
                            mongoHelper.DropTable(dataTableName);
                            for (int i = 0; i < 24; i++)
                            {
                                string currentDropTableName = dataTableName + string.Format("_0:00", i);
                                mongoHelper.DropTable(currentDropTableName);
                            }
                        }
                    }
                }

                timesDel = getTickCountDel.EndRecordTime();
                string deleteEnd = string.Format("删除转移任务{0}的mongoDB:{1}中所有过期数据，耗时：{2}ms\r\n", taskConfig.TaskItem.TaskId, taskConfig.TaskItem.MongodbSrc, timesDel);
                LogHelper.WriteLogToFile(0, null, deleteEnd);
                Console.WriteLine(deleteEnd);
            }
            catch (System.Exception ex)
            {
                isGlobalError = true;
                string deleteError = string.Format("删除任务{0}的mongoDB:{1}中所有过期数据过程出错，异常信息：{2}",
                     taskConfig.TaskItem.TaskId, taskConfig.TaskItem.MongodbSrc, ex.Message);
                LogHelper.WriteLogToFile(0, null, deleteError);
                Console.WriteLine(deleteError);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="progressLogPath"></param>
        /// <param name="dataTableName"></param>
        /// <param name="dataTransferDate"></param>
        /// <param name="delDate"></param>
        /// <returns></returns>
        private static bool CanDelete(string progressLogPath, string taskId, string dataTableName, string dataTransferDate, string delDate)
        {
            if (string.IsNullOrEmpty(progressLogPath) ||
                     (!File.Exists(progressLogPath)))
            {
                return false;
            }
            else
            {
                FileStream fs = null;
                StreamReader sr = null;
                try
                {
                    fs = new FileStream(progressLogPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                    sr = new StreamReader(fs);

                    string oneLine = null;
                    while ((oneLine = sr.ReadLine()) != null)
                    {
                        if (!string.IsNullOrEmpty(oneLine)
                            && (oneLine.Split(':')[0] == taskId)
                            && (oneLine.Split(':')[1] == dataTableName)
                            && (oneLine.Split(':')[2] == dataTransferDate)
                            && (oneLine.Split(':')[3] != "-1"))
                        {
                            return false;
                        }
                    }

                    sr.Close();
                    sr = null;

                    if (Convert.ToDateTime(delDate) < Convert.ToDateTime(dataTransferDate))
                    {
                        return false;
                    }
                }
                catch (System.Exception ex)
                {
                    isGlobalError = true;
                    LogHelper.WriteLogToFile(0, null, string.Format("判断{0}:{1}表是否可删除出错，错误信息：{2}",
                        dataTableName, dataTransferDate, ex.Message));
                    return false;
                }
                finally
                {
                    if (sr != null)
                    {
                        sr.Close();
                        sr = null;
                    }
                }
            }
            return true;
        }

        #endregion

        #region  发送邮件

        private static void Notify()
        {
            currentXmlPath = System.Environment.CurrentDirectory + @"\xml";
            configContainer.GetMailSettings(currentXmlPath + @"\global.xml");

            bool isError = false;

            if (isGlobalError)
            {
                isError = true;
            }
            else
            {
                foreach (BaseThread bs in threadList)
                {
                    if (bs.IsError())
                    {
                        isError = true;
                        break;
                    }
                }
            }

            string directory = System.Environment.CurrentDirectory + @"\log";
            string filePath = string.Format(@"{0}\{1}.log", directory, DateTime.Now.ToString("yyyyMMdd"));

            //通知hadoop集群
            if (configContainer.isNotifyHadoop)
            {
                string statusDir = configContainer.exportFilePath + "status\\";
                if (!Directory.Exists(statusDir))
                {
                    Directory.CreateDirectory(statusDir);
                }

                string statusFile = "";
                if (!isError)
                {
                    statusFile = statusDir + DateTime.Now.ToString("yyyy-MM-dd") + ".success";
                }
                else
                {
                    statusFile = statusDir + DateTime.Now.ToString("yyyy-MM-dd") + ".error";
                }

                File.Create(statusFile);
            }

            //邮件通知
            if (!isError)
            {
                SendMail(configContainer.maiSettings.mailSuccess, configContainer.maiSettings.mailSuccessSubject,
                    configContainer.maiSettings.mailSuccessBody, filePath);
            }
            else
            {
                SendMail(configContainer.maiSettings.mailError, configContainer.maiSettings.mailErrorSubject,
                    configContainer.maiSettings.mailErrorBody, filePath);
            }
        }

        private static void SendMail(List<string> toAddress, string subject, string body, string sAttachFile)
        {
            try
            {
                MailMessage mm = new MailMessage();

                // From
                mm.From = new MailAddress(configContainer.maiSettings.mailFrom);

                // To
                foreach (string addr in toAddress)
                {
                    mm.To.Add(new MailAddress(addr));
                }

                // Subject
                mm.Subject = MacroReplace(subject);
                mm.SubjectEncoding = Encoding.UTF8;

                // Body
                mm.IsBodyHtml = true;
                mm.Body = MacroReplace(body);
                mm.BodyEncoding = Encoding.UTF8;

                Attachment att = null;
                if (null != sAttachFile)
                {
                    att = new Attachment(sAttachFile);

                    mm.Attachments.Add(att);
                }

                // Send
                SmtpClient sc = new SmtpClient(
                    configContainer.maiSettings.smtpServer,
                    Convert.ToInt32(configContainer.maiSettings.smtpPort));
                sc.Credentials = new System.Net.NetworkCredential(
                    configContainer.maiSettings.smtpUserName,
                    configContainer.maiSettings.smtpPassWord);

                sc.Send(mm);
                mm.Attachments.Clear();
                if (null != att)
                {
                    att.Dispose();
                }
            }
            catch (System.Exception ex)
            {
                string sendMailError = string.Format("发送邮件过程出错，错误信息",
                    ex.ToString());
                LogHelper.WriteLogToFile(0, null, sendMailError);
                Console.WriteLine(sendMailError);
            }
        }

        private static string MacroReplace(string s)
        {
            return s.Replace("%transferDate%", configContainer.GetTransferDate().ToShortDateString());
        }

        #endregion

    }
}
