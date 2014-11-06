using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OSSP.BLIService.DataTransferDll
{
    public interface IDataTransfer
    {
        void ReadyForDataTransfer();
        void ReadFromSource();
        void WriteToDestiny();
        bool IsFinished();
        bool IsError();
        TaskConfig GetConfigArgs();
    }
}
