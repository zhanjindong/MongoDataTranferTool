using System;
using System.Data;
using System.Data.SqlClient;
using iFlytek.ComLibrary.Utility;
using OSSP.BLIService.Common;
using System.Collections;
using System.Collections.Generic;
//using OSSP.BLIService.DataTranferTool.Model;
//using SystemLog = OSSP.BLIService.DataTranferTool.Model.SystemLog;
using System.IO;
using System.Globalization;
using System.Configuration;
using System.Text;

namespace OSSP.BLIService.DataTransferDll
{
    public class SQLServerHelper
    {
        private string _sqlServerDataTable;         //SQLServer表
        private string _connectionString;           //SQLServer连接字符串

        //SQLServer表属性
        public string sqlServerDataTable
        {
            set { _sqlServerDataTable = value; }
            get { return _sqlServerDataTable; }
        }
        //SQLServer连接字符串属性
        public string connectionString
        {
            set { _connectionString = value; }
            get { return _connectionString; }
        }

        public SQLServerHelper(string sqlServerDataTable, string connectionString)
        {
            this.sqlServerDataTable = sqlServerDataTable;
            this.connectionString = connectionString;
        }

        /// <summary>
        /// BCP方式批量插入
        /// </summary>
        public void BacthInsert(TaskConfig config, DataRow[] dataRows, string tableName, Hashtable mapHashtable)
        {
            SqlBulkCopy sqlBulkCopy = null;
            SqlConnection sqlConnection = null;

            try
            {
                sqlBulkCopy = new SqlBulkCopy(_connectionString);
                sqlBulkCopy.DestinationTableName = tableName;
                sqlBulkCopy.BatchSize = 100000;
                sqlBulkCopy.BulkCopyTimeout = 7200;

                sqlConnection = new SqlConnection(_connectionString);
                sqlConnection.Open();

                foreach (string str in mapHashtable.Keys)
                {
                    sqlBulkCopy.ColumnMappings.Add(Convert.ToString(mapHashtable[str]), str);
                }

                if ((dataRows != null) && (dataRows.Length != 0))
                {
                    sqlBulkCopy.WriteToServer(dataRows);
                }
            }
            finally
            {
                if (sqlBulkCopy != null)
                {
                    sqlBulkCopy.Close();
                    sqlBulkCopy = null;
                }
                if (sqlConnection != null)
                {
                    sqlConnection.Close();
                    sqlConnection = null;
                }
            }
        }
    }
}