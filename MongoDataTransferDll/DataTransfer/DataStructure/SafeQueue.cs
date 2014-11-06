using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Data;
using System.Data.SqlClient;

namespace OSSP.BLIService.DataTransferDll
{
    class SafeQueue
    {
        public Queue<DataBlock> instance = new Queue<DataBlock>();
        DataRow[] dataRows = null;

        public void SafeEnqueue(DataBlock dataBlock)
        {
            Monitor.Enter(instance);
            instance.Enqueue(dataBlock);
            Monitor.Exit(instance);
        }

        public DataBlock SafeDequeue()
        {
            Monitor.Enter(instance);
            DataBlock dataBlock = instance.Dequeue();
            Monitor.Exit(instance);
            return dataBlock;
        }

        public int CurrentCount()
        {
            return instance.Count;
        }
    }
}
