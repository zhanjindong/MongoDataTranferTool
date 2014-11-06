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
    class MongoToSQL : BaseDataTransfer
    {
        #region Field
        /// <summary>
        /// mongoDB查询语句
        /// </summary>        
        protected QueryDocument qd = null;
        #endregion

        /// <summary>
        /// 构造函数
        /// </summary>
        public MongoToSQL(TaskConfig configArgs)
            : base(configArgs)
        {
            if (TaskConfig.TaskItem.TransferDirect == (int)TransferDirect.MongoToSql)
            {
                SqlServerStr =
                    configArgs.TaskItem.SqlServerSrc.Substring(configArgs.TaskItem.SqlServerSrc.IndexOf('=') + 1, configArgs.TaskItem.SqlServerSrc.IndexOf(';') - configArgs.TaskItem.SqlServerSrc.IndexOf('=') - 1);
            }
        }

        public override void ReadyForDataTransfer()
        {
            base.ReadyForDataTransfer();
            DestShardingTableName = CurrentSqlShardingTableName;
            WriteDescInfo = string.Format(@"向SQLServer:{0}写入{1}表", SqlServerStr, CurrentSqlShardingTableName);
            ReadDescInfo = string.Format(@"从mongoDB:{0}读取{1}表", TaskConfig.TaskItem.MongodbSrc, TaskConfig.TaskItem.TableName + MongoShardNo);
            qd = CreateMongoQureyDocument();

            //同步sqlserver表结构
            if (TaskConfig.TaskItem.TransferDirect == (int)TransferDirect.MongoToSql)
            {
                if (TaskConfig.IsSyncTableStructure)
                {
                    SyncTableStructure();
                }
            }
        }

        private void SyncTableStructure()
        {
            SqlHelper sqlHelper = new SqlHelper(TaskConfig.TaskItem.SqlServerSrc);
            string sqlText = "select name from syscolumns where id=object_id('" + DestShardingTableName + "')";
            sqlHelper.GetDataReader(sqlText);
            List<string> cloumns = new List<string>();
            foreach (var item in sqlHelper)
            {
                IDataReader reader = item as IDataReader;
                cloumns.Add(reader[0].ToString().ToLower());
            }

            StringBuilder addSql = new StringBuilder();

            foreach (string item in MapContainer.TypeHashtable.Keys)
            {
                if (!cloumns.Contains(item.ToLower()))
                {
                    string type = MapContainer.TypeHashtable[item].ToString();
                    if (type == "char" || type == "varchar")
                    {
                        int length = 1024;
                        string lengthStr = MapContainer.StringLengthHashtable[item].ToString();
                        if (!string.IsNullOrEmpty(lengthStr))
                        {
                            Int32.TryParse(lengthStr, out length);
                        }

                        addSql.Append("ALTER TABLE " + DestShardingTableName + " ADD [" + item + "] " + MapContainer.TypeHashtable[item] + "(" + length + ") ");
                    }
                    else
                    {
                        addSql.Append("ALTER TABLE " + DestShardingTableName + " ADD [" + item + "] " + MapContainer.TypeHashtable[item] + " ");
                    }
                }
            }

            if (!string.IsNullOrEmpty(addSql.ToString()))
            {
                sqlHelper.ExecSql(addSql.ToString());
            }

        }

        protected override void LoadOneRecordToDataRows(object obj, ref DataRow[] dataRows, long no)
        {
            #region new
            BsonDocument bsonDocument = obj as BsonDocument;
            string tmpValue = string.Empty;
            string tmpKey = string.Empty;

            //注意的是MapHashtable是sqlserver字段到mongodb的映射
            //所以针对mongotosql和sqltomongo的策略是不一样的。
            foreach (DictionaryEntry item in MapContainer.MapHashtable)
            {
                //之前有一个很隐藏的bug，把这行放在了foreach之外，想想看有什么影藏的bug?
                //只要出现一次……就会影响剩下的……
                bool bRst = true;

                string findKey = item.Value.ToString();//查找的是映射的mongodb字段
                string sqlField = item.Key.ToString();//映射的sqlserver字段

                try
                {
                    //大部分情况是能直接索引到的。
                    tmpValue = bsonDocument[findKey].ToString();
                    tmpKey = findKey;
                }
                //如果出错再忽略大小写查找。
                catch (Exception)
                {
                    bRst = bsonDocument.ContainsKey(findKey, true, ref tmpKey, ref tmpValue);
                }

                if (bRst)
                {
                    if (@"int" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                    {
                        int iValue = 0;
                        if (Int32.TryParse(tmpValue, out iValue))
                            dataRows[no][sqlField] = iValue;
                        else
                            dataRows[no][sqlField] = 0;

                    }
                    else if (@"bigint" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                    {
                        Int64 lValue = 0;
                        if (Int64.TryParse(tmpValue, out lValue))
                            dataRows[no][sqlField] = lValue;
                        else
                            dataRows[no][sqlField] = 0;
                    }
                    else if ((@"varchar" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                        || (@"char" == Convert.ToString(MapContainer.TypeHashtable[sqlField])))
                    {
                        dataRows[no][sqlField] = tmpValue;
                    }
                    else if (@"datetime" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                    {
                        //mongoDB以GMT存储时间，因此取出时间后应加上8小时,这里DateTime.Parse自动转换了
                        //但这样做可能存在的问题是，DateTime.Parse()依赖系统的区域配置甚至是.net Framwork的版本
                        //dataRows[no][str] = DateTime.Parse(tmpValue);

                        //这么做的好处依赖AsDateTime方法
                        BsonType btype = bsonDocument[tmpKey].BsonType;
                        //如果是日期格式需要做特殊的处理
                        if (btype == BsonType.DateTime)
                        {
                            dataRows[no][sqlField] = bsonDocument[tmpKey].AsDateTime.AddHours(8);
                        }
                        else
                        {
                            dataRows[no][sqlField] = bsonDocument[tmpKey];
                        }
                    }
                }
                else//默认值
                {
                    if (@"int" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                    {
                        dataRows[no][sqlField] = 0;
                    }
                    else if (@"bigint" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                    {
                        dataRows[no][sqlField] = 0;
                    }
                    else if ((@"varchar" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                        || (@"char" == Convert.ToString(MapContainer.TypeHashtable[sqlField])))
                    {
                        dataRows[no][sqlField] = string.Empty;
                    }
                    else if (@"datetime" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                    {
                        dataRows[no][sqlField] = DateTime.Now;
                    }
                }
            }
            #endregion

            #region old
            //BsonDocument bsonDocument = obj as BsonDocument;
            //foreach (string str in MapContainer.MapHashtable.Keys)
            //{
            //    if (@"int" == Convert.ToString(MapContainer.TypeHashtable[str]))
            //    {
            //        dataRows[no][str] = bsonDocument[Convert.ToString(MapContainer.MapHashtable[str])].ToInt32();
            //    }
            //    else if (@"bigint" == Convert.ToString(MapContainer.TypeHashtable[str]))
            //    {
            //        dataRows[no][str] = bsonDocument[Convert.ToString(MapContainer.MapHashtable[str])].ToInt64();
            //    }
            //    else if ((@"varchar" == Convert.ToString(MapContainer.TypeHashtable[str]))
            //        || (@"char" == Convert.ToString(MapContainer.TypeHashtable[str])))
            //    {
            //        if (str == "RemoteIP")
            //        {
            //            try
            //            {
            //                dataRows[no][str] = bsonDocument[Convert.ToString(MapContainer.MapHashtable[str])].ToString();
            //            }
            //            catch (Exception)
            //            {
            //                dataRows[no][str] = bsonDocument["RemoteIp"].ToString();
            //            }
            //        }
            //        else
            //        {
            //            dataRows[no][str] = bsonDocument[Convert.ToString(MapContainer.MapHashtable[str])].ToString();
            //        }
            //    }
            //    else if (@"datetime" == Convert.ToString(MapContainer.TypeHashtable[str]))
            //    {
            //        //mongoDB以GMT存储时间，因此取出时间后应加上8小时
            //        dataRows[no][str] = bsonDocument[Convert.ToString(MapContainer.MapHashtable[str])].AsDateTime.AddHours(8);
            //    }
            //}
            #endregion
        }

        protected override IEnumerable GetCursor(string tableName)
        {
            MongoDBHelper mongoHelper = new MongoDBHelper(TaskConfig.TaskItem.MongodbSrc, TaskConfig.TaskItem.MongodbDatabase);
            mongoHelper.Connect();

            //获取mongoDB游标
            MongoCursor<BsonDocument> mongoCursor = null;
            if ("0" == TaskConfig.TaskItem.MongoShardLevel)
            {
                if (true == IsStartTable)
                {
                    mongoCursor = mongoHelper.GetCursor(tableName, qd, CurrentTableTransferedRowCount);
                    IsStartTable = false;
                }
                else
                {
                    mongoCursor = mongoHelper.GetCursor(tableName, qd, 0);
                }
            }
            else
            {
                mongoCursor = mongoHelper.GetCursor(tableName, qd, HaveTransferedRowCount);
            }

            return mongoCursor;
        }

        protected override void Export(TaskConfig taskConfig, DataRow[] dataRows, string tableName, MapContainer mapContainer)
        {
            SqlHelper sqlHelper = new SqlHelper(TaskConfig.TaskItem.SqlServerSrc);
            sqlHelper.BacthInsert(TaskConfig, dataRows, tableName, MapContainer.MapHashtable);
        }

        /// <summary>
        /// 生成MongoDB的查询语句
        /// 如果分区的跨度超过一天那么必须构造查询语句从mongodb数据库里面查询转移日期那一天的数据
        /// </summary>
        private QueryDocument CreateMongoQureyDocument()
        {
            //如果mongodb是按小时或天分表
            if (("1" == TaskConfig.TaskItem.MongoShardLevel) || ("0" == TaskConfig.TaskItem.MongoShardLevel))
            {
                return null;
            }
            if (TaskConfig.TaskItem.QueryDocument == "*")
            {
                return null;
            }

            BsonDocument bd = new BsonDocument();
            QueryDocument q = new QueryDocument();

            bd.Add("$gte", BsonDateTime.Create(Convert.ToDateTime(CurrentTransferDate.ToShortDateString() + "T00:00:00")));
            bd.Add("$lt", BsonDateTime.Create(Convert.ToDateTime(CurrentTransferDate.ToShortDateString() + "T23:59:59")));
            q.Add("CreatedTime", bd);

            return q;
        }
    }
}
