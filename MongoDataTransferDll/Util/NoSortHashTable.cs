using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace OSSP.BLIService.DataTransferDll
{
    public class NoSortHashTable : Hashtable
    {
        private ArrayList list = new ArrayList();

        public override void Add(object key, object value)
        {
            base.Add(key, value);
            list.Add(key);
        }

        public override void Clear()
        {
            base.Clear();
            list.Clear();
        }

        public override void Remove(object key)
        {
            base.Remove(key);
            list.Remove(key);
        }

        public override ICollection Keys
        {
            get
            {
                return list;
            }
        }
    }
}
