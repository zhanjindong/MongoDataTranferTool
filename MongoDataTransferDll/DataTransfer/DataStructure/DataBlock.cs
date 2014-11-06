using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace OSSP.BLIService.DataTransferDll
{
    public class DataBlock
    {
        public DataRow[] dataRows = null;
        public long availableRowCount = -1;
        /// <summary>
        /// 指示是否有新数据
        /// </summary>
        public bool isNew = false;             
    }
}
