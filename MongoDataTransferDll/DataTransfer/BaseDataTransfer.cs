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
using System.IO;

namespace OSSP.BLIService.DataTransferDll
{
    public class BaseDataTransfer : IDataTransfer
    {
        /// <summary>
        /// 数据块缓冲池
        /// </summary>        
        protected DataBlock[] DataBlocks = new DataBlock[2];
        /// <summary>
        /// 当前转移表的参数信息
        /// </summary>
        protected TaskConfig TaskConfig = new TaskConfig();
        /// <summary>
        /// 当前转移表的结构信息
        /// </summary>
        protected MapContainer MapContainer = new MapContainer();
        /// <summary>
        /// 要转移数据的日期
        /// </summary>   
        protected DateTime CurrentTransferDate = DateTime.Now;
        /// <summary>
        /// mongoDB分表后缀
        /// </summary>
        protected string MongoShardNo = null;
        /// <summary>
        /// SQLServer分表后缀
        /// </summary>                   
        protected string SqlShardNo = null;
        /// <summary>
        /// 已经转移的行数
        /// </summary>
        protected long HaveTransferedRowCount = -1;
        /// <summary>
        /// 本次转移已经读取的行数
        /// </summary>
        protected long CurrentTableReadCount = -1;
        /// <summary>
        /// 本次转移最终读取的行数
        /// </summary>
        protected long FinalReadCount = -1;
        /// <summary>
        /// 记录读取一个集合或表的时间
        /// </summary>
        protected GetTickCount GetTickCountRead = new GetTickCount();
        /// <summary>
        /// 记录写入一个集合或表的时间
        /// </summary>
        protected GetTickCount GetTickCountWrite = new GetTickCount();
        /// <summary>
        /// 指示转移是否完成
        /// </summary>
        protected bool _isFinished = false;
        /// <summary>
        /// 指示是否发生错误
        /// </summary>
        protected bool _isError = false;

        /// <summary>
        /// 等待读取的表队列
        /// 只有当分区的跨度小于一天
        /// </summary>
        protected List<string> ReadingTables = new List<string>();
        /// <summary>
        /// 指示是否已经开始插入
        /// </summary>                          
        protected bool IsWriting = false;
        /// <summary>
        /// 指示是否已经读到尾
        /// </summary>                                  
        protected bool IsReadToEnd = false;
        /// <summary>
        /// 指示按小时分表时，是否是开始读取的表
        /// </summary>                         
        protected bool IsStartTable = true;
        /// <summary>
        /// 按小时分表时，当前表已读行数
        /// </summary>                        
        protected long CurrentTableTransferedRowCount = 0;
        /// <summary>
        /// 
        /// </summary>
        protected string SqlServerStr;
        /// <summary>
        /// 
        /// </summary>
        protected string WriteDescInfo;
        protected string ReadDescInfo;
        /// <summary>
        /// sql分区表名称
        /// </summary>
        protected string CurrentSqlShardingTableName;
        /// <summary>
        /// mongodb分区表名称
        /// </summary>
        protected string CurrentMongoShardingTableName;
        /// <summary>
        /// 目标分区表名称
        /// </summary>
        protected string DestShardingTableName;


        /// <summary>
        /// 构造函数
        /// </summary>
        public BaseDataTransfer(TaskConfig configArgs)
        {
            this.TaskConfig = configArgs;
            this.HaveTransferedRowCount = Convert.ToInt64(configArgs.HaveTransferedRowCount);
            this.CurrentTransferDate = Convert.ToDateTime(configArgs.TransferDate);
        }

        /// <summary>
        /// 获取转移配置信息
        /// </summary>
        public virtual TaskConfig GetConfigArgs()
        {
            return TaskConfig;
        }

        /// <summary>
        /// mongoDB转移数据到SQLServer环境准备
        /// </summary>
        public virtual void ReadyForDataTransfer()
        {
            MapContainer.GetMappedTypeInfo(TaskConfig.MapXmlPath);

            GetMongoShardNo();
            GetSQLShardNo();
            CreateWaitForReadingTableList();

            //根据类型hash表新建匹配的DataTable
            DataTable dt = new DataTable();
            CreateMatchedDataTable(ref dt);
            //根据正在转移的表结构初始化两块缓冲区
            for (int i = 0; i < 2; i++)
            {
                DataBlocks[i] = new DataBlock();
                DataBlocks[i].dataRows = new DataRow[Convert.ToInt32(TaskConfig.BlockRowCount)];
                for (int j = 0; j < Convert.ToInt32(TaskConfig.BlockRowCount); j++)
                {
                    DataBlocks[i].dataRows[j] = dt.NewRow();
                }
            }
        }

        /// <summary>
        /// 建立待读取的table列表
        /// </summary>
        public virtual void CreateWaitForReadingTableList()
        {
            //按小时转移
            if ("0" == TaskConfig.TaskItem.MongoShardLevel)
            {
                //定位要开始转移的表和行数
                MongoDBHelper mongoHelper = new MongoDBHelper(TaskConfig.TaskItem.MongodbSrc, TaskConfig.TaskItem.MongodbDatabase);
                mongoHelper.Connect();

                //打印按小时分表相关信息
                string currentMongoShardNo = CurrentTransferDate.ToString("_yyyy_MM_dd");
                string tableInfo = string.Format(@"{0}按小时分表，将转移以下表：", TaskConfig.TaskItem.TableName + currentMongoShardNo);
                LogHelper.WriteLogToFile(1, TaskConfig.TaskItem.TableName, tableInfo);

                //建立待转移按小时分表队列
                long currentTableRowCount = 0;
                long currentTotalTransferedRowCount = 0;
                //根据上次转移的行数计算出剩余转移的表
                for (int i = 0; i < 24; i++)
                {
                    currentMongoShardNo = CurrentTransferDate.ToString("_yyyy_MM_dd");
                    currentMongoShardNo += string.Format("_{0:00}", i);
                    string currentTableName = TaskConfig.TaskItem.TableName + currentMongoShardNo;

                    currentTableRowCount = mongoHelper.GetDataTableRowCount(currentTableName);
                    currentTotalTransferedRowCount += currentTableRowCount;
                    if (currentTotalTransferedRowCount < HaveTransferedRowCount)
                    {
                        CurrentTableTransferedRowCount = HaveTransferedRowCount - currentTableRowCount;
                        continue;
                    }
                    else
                    {
                        ReadingTables.Add(currentTableName);
                        string table = string.Format(@"{0}", currentTableName);
                        LogHelper.WriteLogToFile(1, TaskConfig.TaskItem.TableName, table);
                    }
                }
            }
            else
            {
                string currentCollectionName = TaskConfig.TaskItem.TableName + MongoShardNo;
                ReadingTables.Add(currentCollectionName);
            }
        }

        /// <summary>
        /// 从MongoDB中依次读取集合，放入缓冲区
        /// </summary>
        public virtual void ReadFromSource()
        {
            CurrentTableReadCount = this.HaveTransferedRowCount;

            string tableReadBeginDesc = @"开始" + ReadDescInfo;
            LogHelper.WriteLogToFile(1, TaskConfig.TaskItem.TableName, tableReadBeginDesc);
            Console.WriteLine(tableReadBeginDesc);

            GetTickCountRead.BeginRecordTime();
            try
            {
                //向0块写入
                int writingBlock = 0;
                #region
                foreach (string currentCollectionName in ReadingTables)
                {
                    //向DataRows中按行插入数据          
                    int no = 0;
                    foreach (var obj in GetCursor(currentCollectionName))
                    {
                        while (DataBlocks[writingBlock].isNew == true)
                        {
                            Thread.Sleep(100);
                        }

                        LoadOneRecordToDataRows(obj, ref DataBlocks[writingBlock].dataRows, no);

                        CurrentTableReadCount++;
                        no++;

                        //如果数组已被填满
                        if (no >= Convert.ToInt32(TaskConfig.BlockRowCount))
                        {
                            Monitor.Enter(DataBlocks[writingBlock]);
                            DataBlocks[writingBlock].availableRowCount = no;
                            DataBlocks[writingBlock].isNew = true;
                            Monitor.Exit(DataBlocks[writingBlock]);

                            writingBlock = 1 - writingBlock;
                            no = 0;
                        }
                    }

                    //将表末尾不满blockRowCount行的最后数据放入队列
                    if (no > 0)
                    {
                        Monitor.Enter(DataBlocks[writingBlock]);
                        DataBlocks[writingBlock].availableRowCount = no;
                        DataBlocks[writingBlock].isNew = true;
                        Monitor.Exit(DataBlocks[writingBlock]);

                        writingBlock = 1 - writingBlock;
                    }
                }
                #endregion
                IsReadToEnd = true;
                FinalReadCount = CurrentTableReadCount;

                string tableReadEnd = string.Format(@"成功" + ReadDescInfo + "，耗时:{0} ms，数目:{1}", GetTickCountRead.EndRecordTime(), CurrentTableReadCount);
                LogHelper.WriteLogToFile(1, TaskConfig.TaskItem.TableName, tableReadEnd);
                Console.WriteLine(tableReadEnd);
            }
            catch (Exception ex)
            {
                IsReadToEnd = true;
                _isError = true;
                string tableReadError = string.Format(ReadDescInfo + @"过程出错，已读取{0}行，异常信息:{1}", CurrentTableReadCount, ex.ToString());
                LogHelper.WriteLogToFile(1, TaskConfig.TaskItem.TableName, tableReadError);
                Console.WriteLine(tableReadError);
            }
        }

        /// <summary>
        /// 从缓冲区中读取数据，插入SQL Server
        /// </summary>
        public void WriteToDestiny()
        {

            //循环从DataBlock队列中取出DataBlock写入SQLServer数据库
            int readingBlock = 0;
            try
            {
                while (true)
                {
                    //若读取0行，则直接退出
                    if ((true == IsReadToEnd) && (0 == FinalReadCount))
                    {
                        _isFinished = true;
                        break;
                    }

                    //已转移行数同最终读取行数，转移完成
                    if ((true == IsReadToEnd) && (FinalReadCount != -1) && (HaveTransferedRowCount == FinalReadCount))
                    {
                        _isFinished = true;
                        string tableWriteEnd = string.Format(@"成功" + WriteDescInfo + "，耗时:{0} ms，数目:{1}",
                              GetTickCountWrite.EndRecordTime(), HaveTransferedRowCount);
                        LogHelper.WriteLogToFile(1, TaskConfig.TaskItem.TableName, tableWriteEnd);
                        Console.WriteLine(tableWriteEnd);

                        break;
                    }

                    if (DataBlocks[readingBlock].isNew == false)
                    {
                        Thread.Sleep(100);
                        continue;
                    }

                    if (false == IsWriting)
                    {
                        GetTickCountWrite.BeginRecordTime();

                        string tableWriteBegin = "开始" + WriteDescInfo;
                        LogHelper.WriteLogToFile(1, TaskConfig.TaskItem.TableName, tableWriteBegin);
                        Console.WriteLine(tableWriteBegin);

                        IsWriting = true;
                    }

                    Monitor.Enter(DataBlocks[readingBlock]);

                    if (DataBlocks[readingBlock].availableRowCount == Convert.ToInt32(TaskConfig.BlockRowCount))
                    {
                        Export(TaskConfig, DataBlocks[readingBlock].dataRows, DestShardingTableName, MapContainer);
                        HaveTransferedRowCount += DataBlocks[readingBlock].availableRowCount;
                    }
                    else
                    {
                        DataRow[] tempDataRows = new DataRow[DataBlocks[readingBlock].availableRowCount];
                        Array.Copy(DataBlocks[readingBlock].dataRows, tempDataRows, DataBlocks[readingBlock].availableRowCount);
                        Export(TaskConfig, tempDataRows, DestShardingTableName, MapContainer);
                        HaveTransferedRowCount += DataBlocks[readingBlock].availableRowCount;
                    }
                    DataBlocks[readingBlock].isNew = false;

                    Monitor.Exit(DataBlocks[readingBlock]);

                    readingBlock = 1 - readingBlock;

                    if (false == _isFinished)
                    {
                        LogHelper.SetProgressLog(TaskConfig.ProgressLogPath, TaskConfig.TaskItem.TaskId,
                            TaskConfig.TaskItem.TableName, CurrentTransferDate.ToShortDateString(), HaveTransferedRowCount);
                    }
                    else
                    {
                        LogHelper.SetProgressLog(TaskConfig.ProgressLogPath, TaskConfig.TaskItem.TaskId,
                           TaskConfig.TaskItem.TableName, CurrentTransferDate.ToShortDateString(), -1);
                    }
                }
            }
            catch (Exception ex)
            {
                _isError = true;
                _isFinished = true;

                string tableWriteError = string.Format(@"向" + WriteDescInfo + "过程出错，已插入{0}行，异常信息:{1}",
                     HaveTransferedRowCount, ex.ToString());
                LogHelper.WriteLogToFile(1, TaskConfig.TaskItem.TableName, tableWriteError);
                Console.WriteLine(tableWriteError);
            }
            finally
            {
                if ((false == _isError) && (true == _isFinished))
                {
                    LogHelper.SetProgressLog(TaskConfig.ProgressLogPath, TaskConfig.TaskItem.TaskId,
                      TaskConfig.TaskItem.TableName, CurrentTransferDate.ToShortDateString(), -1);
                }
                else
                {
                    LogHelper.SetProgressLog(TaskConfig.ProgressLogPath, TaskConfig.TaskItem.TaskId,
                      TaskConfig.TaskItem.TableName, CurrentTransferDate.ToShortDateString(), HaveTransferedRowCount);
                }
            }
        }

        /// <summary>
        /// 按行加载数据
        /// </summary>
        protected virtual void LoadOneRecordToDataRows(object obj, ref DataRow[] dataRows, long no)
        {
        }

        protected virtual void Export(TaskConfig taskConfig, DataRow[] dataRows, string tableName, MapContainer mapContainer)
        {
        }

        protected virtual IEnumerable GetCursor(string tableName)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// 设置mongoDB分表后缀
        /// </summary>
        protected virtual void GetMongoShardNo()
        {
            //不支持按小时分片？
            if (("0" == TaskConfig.TaskItem.MongoShardLevel) || ("1" == TaskConfig.TaskItem.MongoShardLevel))
            {
                MongoShardNo = CurrentTransferDate.ToString("_yyyy_MM_dd");
            }
            else if ("2" == TaskConfig.TaskItem.MongoShardLevel)
            {
                MongoShardNo = CurrentTransferDate.ToString("_yyyy_MM");
            }
            else if ("3" == TaskConfig.TaskItem.MongoShardLevel)
            {
                MongoShardNo = CurrentTransferDate.ToString("_yyyy");
            }
            else if ("4" == TaskConfig.TaskItem.MongoShardLevel)
            {
                MongoShardNo = "";
            }

            if (!string.IsNullOrEmpty(TaskConfig.TaskItem.AliasTableName))
            {
                CurrentMongoShardingTableName = TaskConfig.TaskItem.AliasTableName + MongoShardNo;
            }
            else
            {
                CurrentMongoShardingTableName = TaskConfig.TaskItem.TableName + MongoShardNo;
            }
        }

        /// <summary>
        /// 设置SQLServer分表后缀
        /// </summary>
        protected virtual void GetSQLShardNo()
        {
            if (("0" == TaskConfig.TaskItem.SqlServerShardLevel) || ("1" == TaskConfig.TaskItem.SqlServerShardLevel))
            {
                SqlShardNo = CurrentTransferDate.ToString("_yyyy_MM_dd");
            }
            else if ("2" == TaskConfig.TaskItem.SqlServerShardLevel)
            {
                SqlShardNo = CurrentTransferDate.ToString("_yyyy_MM");
            }
            else if ("3" == TaskConfig.TaskItem.SqlServerShardLevel)
            {
                SqlShardNo = CurrentTransferDate.ToString("_yyyy");
            }
            else if ("4" == TaskConfig.TaskItem.SqlServerShardLevel)
            {
                SqlShardNo = "";
            }

            if (!string.IsNullOrEmpty(TaskConfig.TaskItem.AliasTableName))
            {
                CurrentSqlShardingTableName = TaskConfig.TaskItem.AliasTableName + @"_BackUp" + SqlShardNo;
            }
            else
            {
                CurrentSqlShardingTableName = TaskConfig.TaskItem.TableName + @"_BackUp" + SqlShardNo;
            }
        }

        /// <summary>
        /// 建立和正在转移表匹配的DataTable数据结构
        /// </summary>
        private void CreateMatchedDataTable(ref DataTable dt)
        {
            foreach (string str in MapContainer.TypeHashtable.Keys)
            {
                if (@"int" == Convert.ToString(MapContainer.TypeHashtable[str]))
                {
                    dt.Columns.Add(str, typeof(Int32));
                }
                else if (@"bigint" == Convert.ToString(MapContainer.TypeHashtable[str]))
                {
                    dt.Columns.Add(str, typeof(Int64));
                }
                else if ((@"varchar" == Convert.ToString(MapContainer.TypeHashtable[str]))
                    || (@"char" == Convert.ToString(MapContainer.TypeHashtable[str])))
                {
                    dt.Columns.Add(str, typeof(string));
                }
                else if (@"datetime" == Convert.ToString(MapContainer.TypeHashtable[str]))
                {
                    dt.Columns.Add(str, typeof(DateTime));
                }
            }
        }

        /// <summary>
        /// 指示转移是否已完成
        /// </summary>
        public virtual bool IsFinished()
        {
            return _isFinished;
        }

        /// <summary>
        /// 指示转移是否发生错误
        /// </summary>
        public virtual bool IsError()
        {
            return _isError;
        }
    }
}
