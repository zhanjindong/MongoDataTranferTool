using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Globalization;
using System.Threading;

namespace OSSP.BLIService.DataTransferDll
{
    public class LogHelper
    {
        /// <summary>
        /// 指示是否正在设置进度日志
        /// </summary>
        protected static object isSetProgressLog = new object();                    

        /// <summary>
        /// 记录日志文件
        /// </summary>
        /// <param name="logType">0 最终生成日志，1 各表的临时日志，2 进度日志，3 临时错误日志</param>
        public static void WriteLogToFile(int logType, string dataTableName, string logContent)
        {
            string directory = null;
            if (0 == logType)
            {
                directory = System.Environment.CurrentDirectory + @"\log";
            }
            else if (1 == logType)
            {
                directory = System.Environment.CurrentDirectory + @"\log\templog";
            }
            else if (2 == logType)
            {
                directory = System.Environment.CurrentDirectory + @"\log\progresslog";
            }
            else if (3 == logType)
            {
                directory = System.Environment.CurrentDirectory + @"\log\templog";
            }

            FileStream fs = null;
            StreamWriter streamWriter = null;
            try
            {
                //路径不存在则建立
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                string filePath = null;

                if (0 == logType)
                {
                    filePath = string.Format(@"{0}\{1}.log", directory, DateTime.Now.ToString("yyyyMMdd"));
                }
                else if (1 == logType)
                {
                    filePath = string.Format(@"{0}\{1}_{2}.log", directory, dataTableName, DateTime.Now.ToString("yyyyMMdd"));
                }
                else if (2 == logType)
                {
                    filePath = string.Format(@"{0}\progresslog.log", directory);
                }
                else if (3 == logType)
                {
                    filePath = string.Format(@"{0}\errorlog_{1}_{2}.log", directory, dataTableName, DateTime.Now.ToString("yyyyMMdd"));
                }

                fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite,FileShare.ReadWrite);
                streamWriter = new StreamWriter(fs);
                streamWriter.BaseStream.Seek(0, SeekOrigin.End);

                if (2 == logType)
                {
                    streamWriter.WriteLine(logContent);
                }
                else
                {
                    streamWriter.WriteLine(string.Format("{0}:{1}", DateTime.Now.ToString(CultureInfo.CurrentCulture),
                                                    logContent));
                }
            }
            catch (Exception)
            {
                return;
            }
            finally
            {
                if (streamWriter!=null)
                {
                    streamWriter.Flush();
                    streamWriter.Close();
                }           
            }
        }

        /// <summary>
        /// 设置进度日志
        /// </summary>
        public static void SetProgressLog(
            string progressLogPath,string taskId, string dataTableName, string transferDate, long haveTransferedRowCount)
        {
            Monitor.Enter(isSetProgressLog);

            if (string.IsNullOrEmpty(progressLogPath) ||
                (!File.Exists(progressLogPath)))
            {
                LogHelper.WriteLogToFile(2, null, string.Format(@"{0}:{1}:{2}:{3}",
                    taskId,dataTableName,transferDate, haveTransferedRowCount));
            }
            else
            {
                FileStream fsRead = null;
                FileStream fsWrite = null;
                StreamReader sr = null;
                StreamWriter sw = null;

                try
                {
                    fsRead = new FileStream(progressLogPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                    sr = new StreamReader(fsRead);

                    string wholeContent = null;
                    string oneLine = null;
                    bool isFind = false;
                    while ((oneLine = sr.ReadLine()) != null)
                    {
                        try
                        {
                            if (!string.IsNullOrEmpty(oneLine)
                                && ((taskId == oneLine.Split(':')[0])
                                && (transferDate == oneLine.Split(':')[2])))
                            {
                                isFind = true;
                                if ((Convert.ToInt64(oneLine.Split(':')[3]) != -1) && (haveTransferedRowCount > Convert.ToInt64(oneLine.Split(':')[3]))
                                    || (-1 == haveTransferedRowCount))
                                {
                                    wholeContent += string.Format("{0}:{1}:{2}:{3}",
                                        taskId,dataTableName, transferDate, haveTransferedRowCount);
                                    wholeContent += "\r\n";
                                    continue;
                                }
                            }
                        }
                        catch (System.Exception ex)
                        {
                            string strSetProgressLogError = string.Format(@"设置进度日志出错，任务ID：{0}，表名：{1}，已转移{2}行，异常信息：{3}",
                            taskId,dataTableName, haveTransferedRowCount, ex.ToString());
                            LogHelper.WriteLogToFile(1, dataTableName, strSetProgressLogError);
                            Console.WriteLine(strSetProgressLogError);
                        }

                        wholeContent += oneLine;
                        wholeContent += "\r\n";
                    }

                    if (false == isFind)
                    {
                        wholeContent += string.Format("{0}:{1}:{2}:{3}",
                            taskId,dataTableName, transferDate, haveTransferedRowCount);
                        wholeContent += "\r\n";
                    }

                    fsWrite = new FileStream(progressLogPath, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
                    sw = new StreamWriter(fsWrite);
                    sw.Write(wholeContent);
                }
                catch (System.Exception ex)
                {
                    string strSetProgressLogError = string.Format(@"设置进度日志出错，任务ID:{0}，表名：{1}，已转移{2}行，异常信息：{3}",
                            taskId,dataTableName, haveTransferedRowCount, ex.ToString());
                    LogHelper.WriteLogToFile(1, dataTableName, strSetProgressLogError);
                    Console.WriteLine(strSetProgressLogError);
                }
                finally
                {
                    sw.Flush();
                    sw.Close();
                }
            }

            Monitor.Exit(isSetProgressLog);
        }

        /// <summary>
        /// 某部分数据完全转移后，删除相应记录
        /// </summary>
        public static void TryRemoveProgressLog(
            string progressLogPath, string transferDate)
        {
            Monitor.Enter(isSetProgressLog);

            if (File.Exists(progressLogPath))
            {
                FileStream fsRead = null;
                FileStream fsWrite = null;
                StreamReader sr = null;
                StreamWriter sw = null;

                try
                {
                    fsRead = new FileStream(progressLogPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                    sr = new StreamReader(fsRead);

                    string wholeContent = null;
                    string oneLine = null;
                    while ((oneLine = sr.ReadLine()) != null)
                    {
                        try
                        {
                            if (!string.IsNullOrEmpty(oneLine)
                                && (Convert.ToDateTime(transferDate).AddDays(-5) > Convert.ToDateTime(oneLine.Split(':')[2]).Date))
                            {
                                continue;
                            }
                        }
                        catch (System.Exception ex)
                        {
                            string strTryRemoveProgressLogError = string.Format(@"删除进度日志相应记录出错，异常信息：{0}",
                            ex.ToString());
                            LogHelper.WriteLogToFile(0, null, strTryRemoveProgressLogError);
                            Console.WriteLine(strTryRemoveProgressLogError);
                        }
                        wholeContent += oneLine;
                        wholeContent += "\r\n";
                    }

                    fsWrite = new FileStream(progressLogPath, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
                    sw = new StreamWriter(fsWrite);
                    sw.Write(wholeContent);
                }
                catch (System.Exception ex)
                {
                    string strTryRemoveProgressLogError = string.Format(@"删除进度日志相应记录出错，异常信息：{0}",
                            ex.ToString());
                    LogHelper.WriteLogToFile(0, null, strTryRemoveProgressLogError);
                    Console.WriteLine(strTryRemoveProgressLogError);
                }
                finally
                {
                    sw.Flush();
                    sw.Close();
                }
            }

            Monitor.Exit(isSetProgressLog);
        }

        /// <summary>
        /// 合并临时日志文件
        /// </summary>
        public static void MergeLog()
        {
            string logTempDirectory = System.Environment.CurrentDirectory + @"\log\templog";
            LogHelper.MergeTempLog(logTempDirectory);
        }

        private static void MergeTempLog(string tempDirectory)
        {
            if (Directory.Exists(tempDirectory))
            {
                string[] fileNames = Directory.GetFiles(tempDirectory);
                foreach (string fileName in fileNames)
                {
                    FileStream tempFileStream = null;
                    StreamReader sr = null;
                    try
                    {
                        tempFileStream = new FileStream(fileName, FileMode.Open);
                        sr = new StreamReader(tempFileStream);
                        string tempContent = sr.ReadToEnd();
                        tempContent += "\r\n";
                        LogHelper.WriteLogToFile(0, null, tempContent);

                        tempFileStream.Flush();

                        sr.Close();
                        tempFileStream.Close();

                        sr = null;
                        tempFileStream = null;

                        File.Delete(fileName);
                    }
                    catch (System.Exception ex)
                    {
                        string SetProgressLogError = string.Format(@"合并临时日志文件出错，异常信息：{0}",
                                ex.ToString());
                        LogHelper.WriteLogToFile(0, null, SetProgressLogError);
                        Console.WriteLine(SetProgressLogError);
                    }
                    finally
                    {
                        if (sr != null)
                        {
                            sr.Close();
                            sr = null;
                        }

                        if (tempFileStream != null)
                        {
                            tempFileStream.Flush();
                            tempFileStream.Close();
                            tempFileStream = null;
                        }
                    }
                }
            }
        }
    }
}
