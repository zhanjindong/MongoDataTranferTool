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
    class BsonBlock
    {
        public BsonDocument[] bsonDocuments = null;
        public long availableRowCount;

        public BsonBlock(long availableRowCount,BsonDocument[] bsonDocuments)
        {
            this.bsonDocuments = new BsonDocument[availableRowCount];
            Array.Copy(bsonDocuments, this.bsonDocuments, availableRowCount);

            this.availableRowCount = availableRowCount;
        }
    }
}
