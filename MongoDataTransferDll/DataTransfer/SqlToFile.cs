using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using OSSP.BLIService.DataTransferDll;
using System.Data;
using System.Collections;
using System.IO;

namespace DataTransferDll
{
    class SqlToFile : BaseDataTransfer
    {
        int count = 0;
        #region Construct
        /// <summary>
        /// 构造函数
        /// </summary>
        public SqlToFile(TaskConfig configArgs)
            : base(configArgs)
        {
            SqlServerStr =
                   configArgs.TaskItem.SqlServerSrc.Substring(configArgs.TaskItem.SqlServerSrc.IndexOf('=') + 1, configArgs.TaskItem.SqlServerSrc.IndexOf(';') - configArgs.TaskItem.SqlServerSrc.IndexOf('=') - 1);
        }
        #endregion

        public override void ReadyForDataTransfer()
        {
            base.ReadyForDataTransfer();

            string fileSuffix = "_" + TaskConfig.TransferDate.Replace('-', '_');
            if (!string.IsNullOrEmpty(TaskConfig.TaskItem.AliasTableName))
            {
                //DestShardingTableName = TaskConfig.TaskItem.AliasTableName + SqlShardNo;
                DestShardingTableName = TaskConfig.TaskItem.AliasTableName + fileSuffix;
            }
            else
            {
                //DestShardingTableName = TaskConfig.TaskItem.TableName + SqlShardNo;
                DestShardingTableName = TaskConfig.TaskItem.TableName + fileSuffix;
            }

            string directory = TaskConfig.ExportFilePath + "\\" + TaskConfig.TaskItem.TableName + "\\";
            string fileName = directory + DestShardingTableName + ".log";

            WriteDescInfo = string.Format(@"向文件:{0}写入{1}表", fileName, TaskConfig.TaskItem.TableName);
            ReadDescInfo = string.Format(@"从sqlserver:{0}读取{1}表", SqlServerStr, TaskConfig.TaskItem.TableName);
        }

        public override void CreateWaitForReadingTableList()
        {
            if (SqlShardNo != string.Empty)
            {
                ReadingTables.Add(TaskConfig.TaskItem.TableName + @"_BackUp" + SqlShardNo);
            }
            else
            {
                ReadingTables.Add(TaskConfig.TaskItem.TableName);
            }
        }

        protected override IEnumerable GetCursor(string tableName)
        {
            StringBuilder commandText = new StringBuilder();
            commandText.Append("WITH temp AS (");
            commandText.Append("SELECT ");

            if (!string.IsNullOrEmpty(TaskConfig.TaskItem.OrderFields))
            {
                commandText.Append(" ROW_NUMBER() OVER (ORDER BY " + TaskConfig.TaskItem.OrderFields + ") AS 'RowNumber',");
            }
            else
            {
                commandText.Append(" ROW_NUMBER() OVER (ORDER BY ID) AS 'RowNumber',");
            }

            if (MapContainer.MapHashtable.Count == 0)
                throw new Exception("MapContainer.MapHashtable.Count==0");

            int i = 1;
            foreach (var item in MapContainer.MapHashtable.Keys)
            {
                if (i < MapContainer.MapHashtable.Count)
                {
                    commandText.Append("[" + item + "],");
                }
                else
                {
                    commandText.Append("[" + item + "]");
                }

                i++;
            }

            commandText.Append(" FROM " + tableName);
            if (!string.IsNullOrEmpty(TaskConfig.TaskItem.SqlAfterWhere))
            {
                commandText.Append(" WHERE " + TaskConfig.TaskItem.SqlAfterWhere);
            }
            commandText.Append(")");
            commandText.Append("SELECT * FROM temp WHERE RowNumber>" + TaskConfig.HaveTransferedRowCount);

            SqlHelper sqlHelper = new SqlHelper(TaskConfig.TaskItem.SqlServerSrc);
            sqlHelper.GetDataReader(commandText.ToString());

            return sqlHelper;
        }

        protected override void LoadOneRecordToDataRows(object obj, ref DataRow[] dataRows, long no)
        {
            IDataReader reader = obj as IDataReader;
            foreach (string str in MapContainer.MapHashtable.Keys)
            {
                //dataRows[no][str] = reader[Convert.ToString(MapContainer.MapHashtable[str])];
                dataRows[no][str] = reader[str];
            }
        }

        protected override void Export(TaskConfig taskConfig, System.Data.DataRow[] dataRows, string tableName, MapContainer mapContainer)
        {
            DateTime tDate = Convert.ToDateTime(taskConfig.TransferDate);
            string directory = taskConfig.ExportFilePath + tDate.AddDays(1).ToString("yyyy-MM-dd") + "\\";
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
