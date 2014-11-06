using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using OSSP.BLIService.DataTransferDll;

namespace OSSP.BLIService.DataTranferTool
{
    class BaseThread
    {    
        protected Thread thread = null;
        protected bool isRunning = false;
        protected IDataTransfer dataTransferinstance = null;
        protected TaskConfig configArgs = new TaskConfig();

        public BaseThread(IDataTransfer dataTransferinstance)
        {
            this.dataTransferinstance = dataTransferinstance;
        }

        /// <summary>
        /// 线程主函数
        /// </summary>
        public virtual void ThreadMain()
        {
            //子类实现
        }

        /// <summary>
        /// 启动线程，子类可以重载
        /// </summary>
        public virtual int Start()
        {
            if (isRunning)
            {
                return -1;
            }

            isRunning = true;
            thread = new Thread(new ThreadStart(this.ThreadMain));
            thread.IsBackground = true;
            thread.Start();
            return 0;
        }

        /// <summary>
        /// 停止线程，并等待waitTime，子类可以重载
        /// </summary>
        /// <param name="waitTime">时间为毫秒</param>
        public virtual void Stop(int waitTime)
        {
            if (!isRunning)
            {
                return;
            }

            isRunning = false;
            thread.Join(waitTime);
        }

        /// <summary>
        /// 线程是否已停止
        /// </summary>
        public virtual bool IsFinished()
        {
            return dataTransferinstance.IsFinished();
        }

        /// <summary>
        /// 线程是否出现错误
        /// </summary>
        public virtual bool IsError()
        {
            return dataTransferinstance.IsError();
        }

        public virtual TaskConfig GetConfigArgs()
        {
            return dataTransferinstance.GetConfigArgs();
        }
    }
}
