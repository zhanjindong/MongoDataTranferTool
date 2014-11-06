using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;

namespace DataTransferDll
{
    /// <summary>
    /// 对BsonDocument的扩展
    /// </summary>
    public static class ExtBsonDocument
    {
        /// <summary>
        /// 判断BsonDocument是否存在指定的key，如果存在并返回value。
        /// </summary>
        /// <param name="bsonDoc">BsonDocument</param>
        /// <param name="key">查找的key</param>
        /// <param name="ignoreCase">是否忽略大小写</param>
        /// <param name="returnValue">返回的value</param>
        /// <returns></returns>
        public static bool ContainsKey(this BsonDocument bsonDoc, string key, bool ignoreCase, ref string returnKey,ref string returnValue)
        {
            List<BsonElement> list = bsonDoc.AsEnumerable().ToList<BsonElement>();
            foreach (var item in list)
            {
                if (string.Compare(item.Name.Trim(), key, ignoreCase) == 0)
                {
                    returnKey = item.Name;
                    returnValue = bsonDoc[item.Name].ToString();
                    return true;
                }
            }

            return false;
        }
    }
}
