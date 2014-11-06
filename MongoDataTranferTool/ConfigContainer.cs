using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using OSSP.BLIService.DataTransferDll;
using System.IO;
using System.Linq;

namespace OSSP.BLIService.DataTranferTool
{
    /// <summary>
    /// 存储全局配置信息的类
    /// </summary>
    class ConfigContainer
    {
        /// <summary>
        /// 任务列表
        /// </summary>
        public Dictionary<string, TaskItem> taskItemList = new Dictionary<string, TaskItem>();
        /// <summary>
        /// 发送邮件设置
        /// </summary>
        public MailSettings maiSettings = new MailSettings();

        #region 对应属性的私有字段
        //是否自动转移,0 不自动转移,1 自动转移
        private string _isAutoTransfer = null;
        //转移哪天的数据,只有当不自动转移时候才有效,格式为：YYYY-MM-DD，如2011-01-01                         
        private string _transferDate = null;
        //转移方式，0 剪切，1 复制                            
        private string _transferType = null;
        //删除日志库(Mongodb)服务器中距离转移日期N天的数据                            
        private string _deleteDay = null;
        //转移队列中最多数据块数量                          
        private string _dataBlockCount = null;
        //数据块中行数                          
        private string _blockRowCount = null;

        //导出文件时候的路径
        private string _exportFilePath = null;

        private bool _isSyncTableStructure = true;

        private bool _isSyncXmlStructure = true;

        private string _syncXmlDataSource = string.Empty;

        private bool _isNotifyHadoop = false;
        #endregion
        /// <summary>
        /// 是否自动转移,0 不自动转移，1 自动转移 
        /// </summary>
        public string isAutoTransfer { get { return _isAutoTransfer; } }
        /// <summary>
        /// 转移哪天的数据，只有当不自动转移时候才有效，格式为：YYYY-MM-DD，如2011-01-01
        /// </summary>
        public string transferDate { get { return _transferDate; } }
        /// <summary>
        /// 转移方式，0 剪切，1 复制
        /// </summary>
        public string transferType { get { return _transferType; } }
        /// <summary>
        /// 删除日志库(Mongodb)服务器中距离转移日期N天的数据
        /// </summary>
        public string deleteDay { get { return _deleteDay; } }
        /// <summary>
        /// 转移队列中最多数据块数量
        /// </summary>
        public string dataBlockCount { get { return _dataBlockCount; } }
        /// <summary>
        /// 数据块中行数
        /// </summary>
        public string blockRowCount { get { return _blockRowCount; } }

        public string exportFilePath { get { return _exportFilePath; } }

        public bool isSyncTableStructure { get { return _isSyncTableStructure; } }

        public bool isSyncXmlStructure { get { return _isSyncXmlStructure; } }

        public string syncXmlDataSource { get { return _syncXmlDataSource; } }

        public bool isNotifyHadoop { get { return _isNotifyHadoop; } }

        /// <summary>
        /// 获得全局配置信息
        /// </summary>
        /// <param name="xmlPath">全局配置文件路径</param>
        public void GetGlobalConfig(string xmlPath)
        {
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.Load(xmlPath);

            _isAutoTransfer = NodeText(xmlDoc.SelectSingleNode("/globalConfig/isAutoTransfer"));
            _transferDate = NodeText(xmlDoc.SelectSingleNode("/globalConfig/transferDate"));
            _transferType = NodeText(xmlDoc.SelectSingleNode("/globalConfig/transferType"));
            _deleteDay = NodeText(xmlDoc.SelectSingleNode("/globalConfig/deleteDay"));
            _dataBlockCount = NodeText(xmlDoc.SelectSingleNode("/globalConfig/dataBlockCount"));
            _blockRowCount = NodeText(xmlDoc.SelectSingleNode("/globalConfig/blockRowCount"));
            _exportFilePath = NodeText(xmlDoc.SelectSingleNode("/globalConfig/exportFilePath"));

            Boolean.TryParse(NodeText(xmlDoc.SelectSingleNode("/globalConfig/isNotifyHadoop")), out _isNotifyHadoop);

            string isSyncTableStructureStr = NodeText(xmlDoc.SelectSingleNode("/globalConfig/isSyncTableStructure"));
            if (string.IsNullOrEmpty(isSyncTableStructureStr))
            {
                _isSyncTableStructure = true;
            }
            else
            {
                if (!bool.TryParse(isSyncTableStructureStr, out _isSyncTableStructure))
                {
                    _isSyncTableStructure = false;
                }
            }
            string isSyncXmlStructureStr = NodeText(xmlDoc.SelectSingleNode("/globalConfig/isSyncXmlStructure"));
            if (string.IsNullOrEmpty(isSyncXmlStructureStr))
            {
                _isSyncXmlStructure = true;
            }
            else
            {
                if (!bool.TryParse(isSyncXmlStructureStr, out _isSyncXmlStructure))
                {
                    _isSyncXmlStructure = false;
                }
            }
            _syncXmlDataSource = NodeText(xmlDoc.SelectSingleNode("/globalConfig/syncXmlDataSource"));

            XmlNodeList xmlNodeList = xmlDoc.SelectNodes("/globalConfig/taskList/taskItem");
            foreach (XmlNode xmlNode in xmlNodeList)
            {
                List<string> tmpAttributes = new List<string>();
                for (int i = 0; i < xmlNode.Attributes.Count; i++)
                {
                    tmpAttributes.Add(xmlNode.Attributes[i].Name);
                }

                TaskItem taskItem = new TaskItem();
                taskItem.TaskId = NodeText(xmlNode.Attributes["taskId"]);
                taskItem.TableName = NodeText(xmlNode.Attributes["tableName"]);

                if (tmpAttributes.Contains("mongoShardLevel"))
                {
                    taskItem.MongoShardLevel = NodeText(xmlNode.Attributes["mongoShardLevel"]);
                }
                if (tmpAttributes.Contains("sqlShardLevel"))
                {
                    taskItem.SqlServerShardLevel = NodeText(xmlNode.Attributes["sqlShardLevel"]);
                }
                if (tmpAttributes.Contains("transferDirect"))
                {
                    taskItem.TransferDirect = int.Parse(NodeText(xmlNode.Attributes["transferDirect"]));
                }
                if (tmpAttributes.Contains("sqlAfterWhere"))
                {
                    taskItem.SqlAfterWhere = NodeText(xmlNode.Attributes["sqlAfterWhere"]);
                }
                if (tmpAttributes.Contains("orderFields"))
                {
                    taskItem.OrderFields = NodeText(xmlNode.Attributes["orderFields"]);
                }
                if (tmpAttributes.Contains("queryDocument"))
                {
                    taskItem.QueryDocument = NodeText(xmlNode.Attributes["queryDocument"]);
                }
                if (tmpAttributes.Contains("transferDate"))
                {
                    taskItem.TransferDate = NodeText(xmlNode.Attributes["transferDate"]);
                }
                if (tmpAttributes.Contains("aliasTableName"))
                {
                    taskItem.AliasTableName = NodeText(xmlNode.Attributes["aliasTableName"]);
                }


                if (tmpAttributes.Contains("mongoSrc"))
                {
                    string mongoSrc = NodeText(xmlNode.Attributes["mongoSrc"]);
                    int index = mongoSrc.LastIndexOf(';');
                    int length = mongoSrc.Length;
                    taskItem.MongodbSrc = mongoSrc.Substring(0, index).Trim();
                    taskItem.MongodbDatabase = mongoSrc.Substring(index + 1, length - index - 1).Trim();
                }
                if (tmpAttributes.Contains("sqlSrc"))
                {
                    taskItem.SqlServerSrc = NodeText(xmlNode.Attributes["sqlSrc"]);
                }

                if (taskItemList.ContainsKey(taskItem.TaskId))
                {
                    throw new Exception("添加重复TaskId为" + taskItem.TaskId + "的任务。");
                }
                else
                {
                    taskItemList.Add(taskItem.TaskId, taskItem);
                }
            }
        }

        /// <summary>
        /// 获得邮件配置信息
        /// </summary>
        /// <param name="xmlPath"></param>
        public void GetMailSettings(string xmlPath)
        {
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.Load(xmlPath);

            maiSettings.smtpServer = NodeText(xmlDoc.SelectSingleNode("/globalConfig/mailSettings/smtp/server"));
            maiSettings.smtpPort = NodeText(xmlDoc.SelectSingleNode("/globalConfig/mailSettings/smtp/port"));
            maiSettings.smtpUserName = NodeText(xmlDoc.SelectSingleNode("/globalConfig/mailSettings/smtp/userName"));
            maiSettings.smtpPassWord = NodeText(xmlDoc.SelectSingleNode("/globalConfig/mailSettings/smtp/passWord"));

            maiSettings.mailFrom = NodeText(xmlDoc.SelectSingleNode("/globalConfig/mailSettings/mailNotifier/mailFrom"));

            XmlNodeList xmlNodeSuccessList = xmlDoc.SelectNodes("/globalConfig/mailSettings/mailNotifier/mailSuccess/address");
            foreach (XmlNode xmlNode in xmlNodeSuccessList)
            {
                maiSettings.mailSuccess.Add(NodeText(xmlNode));
            }

            maiSettings.mailSuccessSubject = NodeText(xmlDoc.SelectSingleNode("/globalConfig/mailSettings/mailNotifier/mailSuccess/subject"));
            maiSettings.mailSuccessBody = NodeText(xmlDoc.SelectSingleNode("/globalConfig/mailSettings/mailNotifier/mailSuccess/body"));

            XmlNodeList xmlNodeErrorList = xmlDoc.SelectNodes("/globalConfig/mailSettings/mailNotifier/mailError/address");
            foreach (XmlNode xmlNode in xmlNodeErrorList)
            {
                maiSettings.mailError.Add(NodeText(xmlNode));
            }

            maiSettings.mailErrorSubject = NodeText(xmlDoc.SelectSingleNode("/globalConfig/mailSettings/mailNotifier/mailError/subject"));
            maiSettings.mailErrorBody = NodeText(xmlDoc.SelectSingleNode("/globalConfig/mailSettings/mailNotifier/mailError/body"));
        }

        /// <summary>
        /// 获得数据转移配置信息
        /// </summary>
        public TaskConfig CreateConfigArgs(string taskId, string transferDate)
        {
            //这里存在一个潜在问题就是从日志加载上一次没有转移完的任务时候
            //如过现在对应的TaskId的TaskItem参数已经跟当时转移时候不一致则会出问题。
            //解决方法就是在日志中记载转移任务的完整信息对于没有转移成功的任务信息完全
            //完全从日志中加载不依赖现有配置
            TaskConfig configArgs = new TaskConfig();
            configArgs.TaskItem = taskItemList[taskId];
            string preFormatDateStr = string.Empty;

            if (!string.IsNullOrEmpty(taskItemList[taskId].TransferDate))
            {
                preFormatDateStr = taskItemList[taskId].TransferDate;
            }
            else
            {
                preFormatDateStr = transferDate;
            }
            string[] dateArr = preFormatDateStr.Replace('/', '-').Split(new char[] { '-' });
            for (int i = 1; i <= 2; i++)
            {
                if (dateArr[i].Length == 1)
                {
                    dateArr[i] = "0" + dateArr[i];
                }
            }

            configArgs.TransferDate = dateArr[0] + "-" + dateArr[1] + "-" + dateArr[2];

            configArgs.DataBlockCount = _dataBlockCount;
            configArgs.BlockRowCount = _blockRowCount;
            configArgs.IsSyncTableStructure = _isSyncTableStructure;
            //2013-01-09
            configArgs.ExportFilePath = _exportFilePath;

            //如果有别名就以别名来查找配置文件
            if (!string.IsNullOrEmpty(taskItemList[taskId].AliasTableName))
            {
                configArgs.MapXmlPath = string.Format(@".\xml\{0}_map.xml", taskItemList[taskId].AliasTableName);
            }
            else
            {
                configArgs.MapXmlPath = string.Format(@".\xml\{0}_map.xml", taskItemList[taskId].TableName);
            }
            configArgs.ProgressLogPath = @".\log\progresslog\progresslog.log";

            configArgs.HaveTransferedRowCount =
            GetHaveTransferedRowCount(configArgs.ProgressLogPath, taskId, configArgs.TransferDate).ToString();

            return configArgs;
        }

        /// <summary>
        /// 获得当前转移日期
        /// </summary>
        public DateTime GetTransferDate()
        {
            if (_isAutoTransfer == "1")
            {
                return DateTime.Now.AddDays(-1);
            }

            return Convert.ToDateTime(_transferDate);
        }

        /// <summary>
        /// 获得已转移行数
        /// </summary>
        /// <param name="progressLogPath">转移进程日志路径</param>
        /// <param name="taskId">转移任务id</param>
        /// <param name="transferDate">转移日期</param>
        /// <returns></returns>
        private long GetHaveTransferedRowCount(string progressLogPath, string taskId, string transferDate)
        {
            long haveTransferedRowCount = 0;
            if (string.IsNullOrEmpty(progressLogPath) ||
                       (!File.Exists(progressLogPath)))
            {
                haveTransferedRowCount = 0;
                return haveTransferedRowCount;
            }
            else
            {
                FileStream fs = null;
                StreamReader sr = null;
                try
                {
                    fs = new FileStream(progressLogPath, FileMode.Open);
                    sr = new StreamReader(fs);

                    string oneLine = null;
                    while ((oneLine = sr.ReadLine()) != null)
                    {
                        if ((taskId == oneLine.Split(':')[0])
                            && (Convert.ToDateTime(transferDate) == Convert.ToDateTime(oneLine.Split(':')[2])))
                        {
                            haveTransferedRowCount = Convert.ToInt64(oneLine.Split(':')[3]);
                            return haveTransferedRowCount;
                        }
                    }

                    sr.Close();
                    sr = null;

                    fs.Close();
                    fs = null;
                }
                catch (System.Exception ex)
                {
                }
                finally
                {
                    if (sr != null)
                    {
                        sr.Close();
                        sr = null;
                    }
                    if (fs != null)
                    {
                        fs.Close();
                        fs = null;
                    }
                }

                haveTransferedRowCount = 0;
                return haveTransferedRowCount;
            }
        }

        private string NodeText(XmlNode xmlNode)
        {
            if (xmlNode == null)
            {
                return "";
            }
            else
            {
                return xmlNode.InnerText;
            }
        }
    }


    /// <summary>
    /// 邮件设置
    /// </summary>
    class MailSettings
    {
        public string smtpServer;
        public string smtpPort;
        public string smtpUserName;
        public string smtpPassWord;
        public string mailFrom;
        public List<string> mailSuccess = new List<string>();
        public string mailSuccessSubject;
        public string mailSuccessBody;
        public List<string> mailError = new List<string>();
        public string mailErrorSubject;
        public string mailErrorBody;
    }
}
