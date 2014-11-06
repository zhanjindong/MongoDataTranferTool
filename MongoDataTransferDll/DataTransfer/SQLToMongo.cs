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
using DataTransferDll;

namespace OSSP.BLIService.DataTransferDll
{
    class SQLToMongo : SqlToFile
    {
        private MongoDBHelper mongoHelper;

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="configArgs">当前转移表的参数信息</param>
        public SQLToMongo(TaskConfig configArgs)
            : base(configArgs)
        {
            SqlServerStr =
                  configArgs.TaskItem.SqlServerSrc.Substring(configArgs.TaskItem.SqlServerSrc.IndexOf('=') + 1, configArgs.TaskItem.SqlServerSrc.IndexOf(';') - configArgs.TaskItem.SqlServerSrc.IndexOf('=') - 1);
            mongoHelper = new MongoDBHelper(configArgs.TaskItem.MongodbSrc, configArgs.TaskItem.MongodbDatabase);
        }

        public override void ReadyForDataTransfer()
        {
            base.ReadyForDataTransfer();

            DestShardingTableName = CurrentMongoShardingTableName;
            WriteDescInfo = string.Format(@"向MongoDB:{0}写入{1}表", TaskConfig.TaskItem.MongodbSrc, DestShardingTableName);
            ReadDescInfo = string.Format(@"从SqlServer:{0}读取{1}表", SqlServerStr, CurrentSqlShardingTableName);
        }

        protected override void Export(TaskConfig taskConfig, DataRow[] dataRows, string tableName, MapContainer mapContainer)
        {
            mongoHelper.Connect();

            #region 插入
            List<BsonDocument> documents = new List<BsonDocument>();
            foreach (var row in dataRows)
            {
                BsonDocument bd = new BsonDocument();
                foreach (string str in mapContainer.MapHashtable.Keys)
                {
                    if (mapContainer.TypeHashtable[str].ToString() == "int")
                    {
                        bd[Convert.ToString(MapContainer.MapHashtable[str])] = Convert.ToInt32(row[str].ToString());
                    }
                    else if (mapContainer.TypeHashtable[str].ToString() == "bigint")
                    {
                        bd[Convert.ToString(MapContainer.MapHashtable[str])] = Convert.ToInt64(row[str].ToString());
                    }
                    else if (mapContainer.TypeHashtable[str].ToString() == "datetime")
                    {
                        bd[Convert.ToString(MapContainer.MapHashtable[str])] = Convert.ToDateTime(row[str].ToString());
                    }
                    else
                    {
                        bd[Convert.ToString(MapContainer.MapHashtable[str])] = row[str].ToString();
                    }
                }

                documents.Add(bd);
            }

            mongoHelper.InsertBatch(tableName, documents);
            #endregion 

            #region 更新
            //foreach (var row in dataRows)
            //{
            //    BsonDocument bd = new BsonDocument();
            //    QueryDocument q = new QueryDocument();
            //    q.Add("BizId", row["BizId"].ToString());
            //    q.Add("Uid", Convert.ToInt64(row["Uid"]));
            //    //mongoHelper.QueryOne(tableName, q, ref u);

            //    string cv=row["CurrentVersion"].ToString();
            //    string cd = row["CurrentDownFrom"].ToString();
            //    DateTime lvmt = Convert.ToDateTime(row["LastVersionModifyTime"]);
            //    DateTime ldmt=Convert.ToDateTime(row["LastDownFromModifyTime"]);

            //    bd["CurrentVersion"] = cv;
            //    bd["CurrentDownFrom"] = cd;
            //    bd["LastVersionModifyTime"] = lvmt;
            //    bd["LastDownFromModifyTime"] = ldmt;


            //    UpdateDocument u = new UpdateDocument("$set",bd);
            //    mongoHelper.Update(tableName, q, u);
            //}
            #endregion
        }
    }
}
