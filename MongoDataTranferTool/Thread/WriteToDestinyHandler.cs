using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using OSSP.BLIService.DataTransferDll;

namespace OSSP.BLIService.DataTranferTool
{
    class WriteToDestinyHandler:BaseThread
    {
        public WriteToDestinyHandler(IDataTransfer dataTransferinstance)
            : base(dataTransferinstance)
        {
        }

        public override void ThreadMain()
        {
            dataTransferinstance.WriteToDestiny();
        }
    }
}
