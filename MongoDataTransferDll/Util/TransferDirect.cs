using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataTransferDll
{
    public enum TransferDirect
    {
        MongoToSql=0,
        SqlToMongo=1,
        MongoToFile=2,
        SqlToFile=3
    }
}
