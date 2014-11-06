using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using OSSP.BLIService.DataTransferDll;
using System.IO;
using System.Data;
using System.Collections;
using MongoDB.Bson;

namespace DataTransferDll
{
    class MongoToFile : MongoToSQL
    {
        #region Construct
        /// <summary>
        /// 构造函数
        /// </summary>
        public MongoToFile(TaskConfig configArgs)
            : base(configArgs)
        {

        }
        #endregion

        public override void ReadyForDataTransfer()
        {
            base.ReadyForDataTransfer();
            DestShardingTableName = CurrentMongoShardingTableName;

            string directory = TaskConfig.ExportFilePath + "\\" + TaskConfig.TaskItem.TableName + "\\";
            string fileName = directory + CurrentMongoShardingTableName + ".log";

            WriteDescInfo = string.Format(@"向文件:{0}写入{1}表", fileName, CurrentMongoShardingTableName);
        }

        protected override void LoadOneRecordToDataRows(object obj, ref DataRow[] dataRows, long no)
        {
            #region new
            BsonDocument bsonDocument = obj as BsonDocument;
            string tmpValue = string.Empty;
            string tmpKey = string.Empty;


            foreach (DictionaryEntry item in MapContainer.MapHashtable)
            {
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
                    //日期要做特殊处理
                    if (@"datetime" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                    {
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
                    else if (@"bigint" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                    {
                        Int64 iValue = 0;
                        if (Int64.TryParse(bsonDocument[tmpKey].ToString(), out iValue))
                            dataRows[no][sqlField] = iValue;
                        else
                            continue;

                    }
                    else if (@"int" == Convert.ToString(MapContainer.TypeHashtable[sqlField]))
                    {
                        Int32 iValue = 0;
                        if (Int32.TryParse(bsonDocument[tmpKey].ToString(), out iValue))
                            dataRows[no][sqlField] = iValue;
                        else
                            continue;
                    }
                    else
                    {
                        dataRows[no][sqlField] = tmpValue;
                    }
                }
                else
                {
                    //导出文本的时候不存在的字段一律为空字符串
                    dataRows[no][sqlField] = DBNull.Value;
                }
            }
            #endregion
        }

        protected override void Export(TaskConfig taskConfig, System.Data.DataRow[] dataRows, string tableName, MapContainer mapContainer)
        {

            DateTime tDate = Convert.ToDateTime(taskConfig.TransferDate);
            string directory = taskConfig.ExportFilePath + tDate.AddDays(1).ToString("yyyy-MM-dd") + "\\";
            //string directory = taskConfig.ExportFilePath + DateTime.Now.ToString("yyyy-MM-dd") + "\\";
            string fileName = tableName + ".log";

            //批量写入文件
            DataClean.ExportToFile(dataRows, directory, fileName, mapContainer);
        }

        public override void ReadFromSource()
        {
            base.ReadFromSource();
        }
    }
}
