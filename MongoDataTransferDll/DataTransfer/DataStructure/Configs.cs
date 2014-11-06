using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OSSP.BLIService.DataTransferDll
{
    /// <summary>
    /// 数据转移配置信息
    /// </summary>
    public class TaskConfig
    {
        /// <summary>
        /// 转移哪天的数据
        /// </summary>
        public string TransferDate = null;
        /// <summary>
        /// 从哪行开始转移
        /// </summary>
        public string HaveTransferedRowCount = null;
        /// <summary>
        /// 转移队列中最多数据块数量
        /// </summary>
        public string DataBlockCount = null;
        /// <summary>
        /// 数据块行数
        /// </summary>
        public string BlockRowCount = null;
        /// <summary>
        /// 存储当前表Xml配置文件路径     
        /// </summary>
        public string MapXmlPath = null;
        /// <summary>
        /// 存储当前表进度日志路径     
        /// </summary>
        public string ProgressLogPath = null;
        /// <summary>
        /// 导出文件路径
        /// </summary>
        public string ExportFilePath = null;

        public TaskItem TaskItem = null;

        public bool IsSyncTableStructure = true;
    }

    /// <summary>
    /// 转移任务
    /// </summary>
    public class TaskItem
    {
        /// <summary>
        /// 任务ID
        /// </summary>
        public string TaskId = null;
        /// <summary>
        /// 当前正在转移表名前缀
        /// </summary>
        public string TableName = null;
        /// <summary>
        /// mongoDB数据库名          
        /// </summary>
        public string MongodbDatabase = null;
        /// <summary>
        /// mongoDB数据库地址
        /// </summary>
        public string MongodbSrc = null;
        /// <summary>
        /// sqlserver数据库地址
        /// </summary>
        public string SqlServerSrc = null;
        /// <summary>
        /// 当前mongoDB表分表等级 
        /// </summary>              
        public string MongoShardLevel = "2";
        /// <summary>
        /// 当前SqlServer表分表等级 
        /// </summary>
        public string SqlServerShardLevel = "2";
        /// <summary>
        /// 转移方向
        /// </summary>
        public int TransferDirect = 0;

        public string SqlAfterWhere = string.Empty;
        public string OrderFields = string.Empty;
        public string QueryDocument = string.Empty;
        public string TransferDate = string.Empty;
        public string AliasTableName = string.Empty;
    }
}
