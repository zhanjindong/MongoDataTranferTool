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
    public class SqlHelper : IEnumerable
    {
        /// <summary>
        /// SQLServer表
        /// </summary>
        private string _sqlServerDataTable;
        /// <summary>
        /// SQLServer连接字符串
        /// </summary>
        private string _connectionString;

        private SqlCommand _cmd = null;
        private SqlConnection conn = null;

        private IDataReader _dataReader;

        //SQLServer表属性
        public string SqlServerDataTable
        {
            set { _sqlServerDataTable = value; }
            get { return _sqlServerDataTable; }
        }
        //SQLServer连接字符串属性
        public string ConnectionString
        {
            set { _connectionString = value; }
            get { return _connectionString; }
        }

        public SqlHelper(string connectionString, string sqlServerDataTable)
            : this(connectionString)
        {
            this.SqlServerDataTable = sqlServerDataTable;
        }

        public SqlHelper(string connectionString)
        {
            this.ConnectionString = connectionString;
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

        /// <summary>
        /// 获取DataReader
        /// </summary>
        /// <returns></returns>
        public void GetDataReader(string commandText)
        {
            try
            {
                conn = new SqlConnection(_connectionString);
                conn.Open();
                _cmd = new SqlCommand(commandText, conn);
                _cmd.CommandTimeout = 600000;
                _dataReader = _cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            finally
            {
                if (_cmd != null)
                {
                    _cmd.Dispose();
                }
            }
        }

        public IEnumerator GetEnumerator()
        {
            try
            {
                while (_dataReader.Read())
                {
                    yield return _dataReader;
                }
            }
            finally
            {
                //枚举完成或失败都要关闭SqlCommand同时关闭SqlConnection
                _cmd.Dispose();
            }
        }
    }
}