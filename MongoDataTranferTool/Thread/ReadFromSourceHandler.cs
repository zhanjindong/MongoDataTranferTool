using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using OSSP.BLIService.DataTransferDll;

namespace OSSP.BLIService.DataTranferTool
{
    class ReadFromSourceHandler : BaseThread
    {
        public ReadFromSourceHandler(IDataTransfer dataTransferinstance)
            : base(dataTransferinstance)
        {
        }

        public override void ThreadMain()
        {
            dataTransferinstance.ReadFromSource();
        }
    }
}
