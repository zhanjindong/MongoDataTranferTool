using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Runtime.InteropServices;
using OSSP.BLIService.DataTransferDll;

namespace OSSP.BLIService.DataTranferTool
{
    public delegate bool ConsoleCtrlDelegate(int dwCtrlType);
    public delegate void CallbackDelegate();

    class CloseCtrlHandler
    {     
        //控制台收到消息  
        //0- CTL + C  
        //1- CTL + Break  
        //2- CLOSE,可能是通过关闭按钮也可能是直接关闭进程
        //3- 操作系统要注销  
        //4- 系统要关机
        private const int CTRL_C_EVENT = 0;
        private const int CTRL_BREAK_EVENT = 1;
        private const int CTRL_CLOSE_EVENT = 2;
        private const int CTRL_LOGOFF_EVENT = 3;
        private const int CTRL_POWEROFF_EVENT = 4;

        [DllImport("kernel32.dll")]
        private static extern bool SetConsoleCtrlHandler(ConsoleCtrlDelegate HandlerRoutine, bool Add);

        private static ConsoleCtrlDelegate consoleCtrlDelegate = new ConsoleCtrlDelegate(ConsoleCtrlHandler);
        private static CallbackDelegate callBackDelegate;

        public static Boolean RegisterCtrlHandler(CallbackDelegate callBack)
        {
            Boolean bRet = SetConsoleCtrlHandler(consoleCtrlDelegate, true);
            if (!bRet)
            {
                LogHelper.WriteLogToFile(0, null, "注册事件处理函数失败");
            }
            callBackDelegate = new CallbackDelegate(callBack);

            return bRet;
        }

        /// <summary>
        /// 此函数为系统调用
        /// </summary>
        /// <param name="CtrlType">事件</param>
        /// <returns>返回false，表示忽略此事件，返回true，表示处理此事件</returns>
        private static bool ConsoleCtrlHandler(int CtrlType)
        {
            switch (CtrlType)
            {
                case CTRL_C_EVENT:
                case CTRL_BREAK_EVENT:
                    {
                        Console.WriteLine("收到命令CTRL+C或CTRL+BREAK，不处理此消息");
                        return false;
                    }
                case CTRL_CLOSE_EVENT:
                case CTRL_LOGOFF_EVENT:
                case CTRL_POWEROFF_EVENT:
                    {
                        Console.WriteLine("异常关闭，正在保存工作状态");
                        LogHelper.WriteLogToFile(0, null, "异常关闭，正在保存工作状态");

                        if (callBackDelegate != null)
                        {
                            callBackDelegate();
                        }

                        break;
                    }
            }
            return true;
        }
    }
}
