using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataTransferDll;

namespace OSSP.BLIService.DataTransferDll
{
    public static class DataTransferFactory
    {
        //////////////////////////////////////////////////////////////////////////
        //
        //  关于分表等级的说明：
        //  0 按小时，1 按天，2 按月，3 按年，4 不分表
        //
        //////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// 获得数据转移类实例
        /// </summary>
        /// <param name="dataTableName">表名前缀，不包括分表等级</param>
        /// <param name="mongoShardLevel">mongoDB分表等级</param>
        /// <param name="sqlShardLevel">SQLServer分表等级</param>
        /// <param name="mapXmlPath">映射文件路径</param>
        /// <param name="errorLogPath">错误日志路径</param>
        /// <param name="transferDirect">转移方向，0,mongodb到sqlserver,1sqlserver到mongodb，2,mongodb到文件，3,sqlserver到文件</param>
        /// <returns>一个数据转移类实例</returns>
        public static IDataTransfer GetDataTransferInstance(TaskConfig configArgs, int transferDirect)
        {
            IDataTransfer dataTransferInstance = null;
            TransferDirect direct = (TransferDirect)transferDirect;

            switch (direct)
            {
                case TransferDirect.MongoToSql:
                    dataTransferInstance = new MongoToSQL(configArgs);break;
                case TransferDirect.SqlToMongo:
                    dataTransferInstance = new SQLToMongo(configArgs);break;
                case TransferDirect.MongoToFile:
                    dataTransferInstance = new MongoToFile(configArgs);break;
                case TransferDirect.SqlToFile:
                    dataTransferInstance = new SqlToFile(configArgs);break; 
                default: break;
            }

            return dataTransferInstance; ;
        }
    }
}
