using System;
using System.Collections.Generic;
using System.Text;
using System.Web;
using MongoDB.Driver;
using MongoDB.Bson;
using OSSP.BLIService.Common;
using iFlytek.ComLibrary.Utility;

namespace OSSP.BLIService.DataTransferDll
{
    public class MongoDBHelper
    {
        private MongoDatabase mongoDatabase;

        private string _mongoDBSrc;                     //mongoDB地址
        private string _mongoDBDatabase;                //mongoDB数据库

        //mongDB地址属性
        public string mongoDBSrc
        {
            get { return _mongoDBSrc; }
            set { _mongoDBSrc = value; }
        }
        //mongoDB数据库属性
        public string mongoDBDatabase
        {
            get { return _mongoDBDatabase; }
            set { _mongoDBDatabase = value; }
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        public MongoDBHelper(string mongoDBSrc, string mongoDBDatabase)
        {
            this.mongoDBSrc = mongoDBSrc;
            this.mongoDBDatabase = mongoDBDatabase;
        }

        //192.168.71.35:OSSP10Log2
        public MongoDBHelper(string connectionString)
        {
            int index = connectionString.LastIndexOf(':');
            this.mongoDBSrc = connectionString.Substring(0, index);
            this.mongoDBDatabase = connectionString.Substring(index + 1, connectionString.Length - index - 1);
        }

        /// <summary>
        /// 连接
        /// </summary>
        public void Connect()
        {
            string[] connectionStr = _mongoDBSrc.Split(new char[] { ':' });
            string dataBase = _mongoDBDatabase;

            MongoServer mongoServer;

            MongoServerSettings setting = new MongoServerSettings();

            if (connectionStr.Length >= 2)
            {
                setting.Server = new MongoServerAddress(connectionStr[0], int.Parse(connectionStr[1]));
            }
            else
            {
                setting.Server = new MongoServerAddress(connectionStr[0]);
            }

            setting.MaxConnectionIdleTime = new TimeSpan(0, 23, 59, 59);
            setting.ConnectTimeout = new TimeSpan(0, 23, 59, 59);
            setting.MaxConnectionLifeTime = new TimeSpan(0, 23, 59, 59);
            setting.SocketTimeout = new TimeSpan(0, 23, 59, 59);

            mongoServer = MongoServer.Create(setting);

            mongoDatabase = mongoServer.GetDatabase(dataBase);
        }

        #region 增删改操作

        public void InsertSafe(string tableName, BsonDocument document)
        {
            MongoCollection collection = mongoDatabase.GetCollection(tableName);
            collection.Insert(document, SafeMode.True);
        }

        public void InsertNoSafe(string tableName, BsonDocument document)
        {
            MongoCollection collection = mongoDatabase.GetCollection(tableName);
            collection.Insert(document, SafeMode.False);
        }

        public void InsertBatchNoSafe(string tableName, IList<BsonDocument> documentList)
        {
            MongoCollection collection = mongoDatabase.GetCollection(tableName);
            collection.InsertBatch(documentList, SafeMode.False);
        }

        public void InsertBatchSafe(string tableName, IList<BsonDocument> documentList)
        {
            MongoCollection collection = mongoDatabase.GetCollection(tableName);
            collection.InsertBatch(documentList, SafeMode.True);
        }

        public void ReMoveSafe(string tableName, IMongoQuery query)
        {
            MongoCollection<BsonDocument> collection =
               mongoDatabase.GetCollection<BsonDocument>(tableName);

            collection.Remove(query, SafeMode.True);
        }

        public void ReMoveNoSafe(string tableName, IMongoQuery query)
        {
            MongoCollection<BsonDocument> collection =
               mongoDatabase.GetCollection<BsonDocument>(tableName);

            collection.Remove(query, SafeMode.False);
        }

        public void UpdateSafe(string tableName, IMongoUpdate update, IMongoQuery query)
        {
            MongoCollection collection = mongoDatabase.GetCollection(tableName);
            collection.Update(query, update, SafeMode.True);
        }

        public void UpdateNoSafe(string tableName, IMongoUpdate update, IMongoQuery query)
        {
            MongoCollection collection = mongoDatabase.GetCollection(tableName);
            collection.Update(query, update, SafeMode.False);
        }

        #endregion

        public MongoCursor<BsonDocument> GetCursor(string tableName, IMongoQuery query, long haveTransferedRowCount)
        {
            MongoCollection<BsonDocument> collection =
                mongoDatabase.GetCollection<BsonDocument>(tableName);

            MongoCursor<BsonDocument> cursor = null;

            if (query == null)
                cursor = collection.FindAll().SetSortOrder(new string[] { "_id" });
            else
                cursor = collection.Find(query).SetSortOrder(new string[] { "_id" });

            cursor.Skip = (int)haveTransferedRowCount;

            return cursor;
        }

        public bool IsExists(string tableName, IMongoQuery query, ref BsonDocument oldDocument)
        {
            MongoCollection<BsonDocument> collection =
                mongoDatabase.GetCollection<BsonDocument>(tableName);

            BsonDocument bsonDocument = collection.FindOne(query);

            if (bsonDocument == null)
            {
                return false;
            }
            else
            {
                oldDocument = bsonDocument;
                return true;
            }
        }

        public void DropTable(string tableName)
        {
            MongoCollection collection = mongoDatabase.GetCollection(tableName);
            if (collection.Exists())
            {
                collection.Drop();
            }
        }

        public IEnumerable<string> GetDataTableNames()
        {
            return mongoDatabase.GetCollectionNames();
        }

        public long GetDataTableRowCount(string tableName)
        {
            MongoCollection<BsonDocument> collection =
                mongoDatabase.GetCollection<BsonDocument>(tableName);

            MongoCursor<BsonDocument> cursor = null;

            cursor = collection.FindAll();

            return cursor.Count();
        }
    }
}
