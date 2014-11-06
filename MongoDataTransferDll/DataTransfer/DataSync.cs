using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using iFlytek.ComLibrary.Utility;
using MongoDB.Bson;
using MongoDB.Driver;
using OSSP.BLIService.Common;

namespace OSSP.BLIService.DataTransferDll
{
    class DataSync : BaseDataTransfer, IDataTransfer
    {
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="configArgs">当前转移表的参数信息</param>
        public DataSync(TaskConfig configArgs)
            : base(configArgs)
        {
        }

        public void ReadyForDataTransfer() { }
        public void ReadFromSource() { }
        public void WriteToDestiny() { }
    }
}
