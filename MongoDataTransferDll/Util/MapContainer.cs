using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Collections;

namespace OSSP.BLIService.DataTransferDll
{
    public class MapContainer
    {
        /// <summary>
        /// SQLServer到mongoDB的字段映射hash表
        /// </summary>
        public Hashtable MapHashtable = new NoSortHashTable();
        /// <summary>
        /// SQLServer的字段和字段类型hash表
        /// </summary>
        public Hashtable TypeHashtable = new NoSortHashTable();
        /// <summary>
        /// SQLServervarchar类型和char类型字段的长度
        /// </summary>    
        public Hashtable StringLengthHashtable = new NoSortHashTable();    

        /// <summary>
        /// 获取mongodb和sqlserver列映射关系
        /// </summary>
        /// <param name="xmlPath"></param>
        public void GetMappedTypeInfo(string xmlPath)
        {
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.Load(xmlPath);
            XmlNodeList xmlNodeList = xmlDoc.SelectNodes("/columnsMap/column");
            foreach (XmlNode xmlNode in xmlNodeList)
            {

                MapHashtable.Add(NodeText(xmlNode.Attributes["sqlServer"]),
                    NodeText(xmlNode.Attributes["mongoDB"]));

                TypeHashtable.Add(NodeText(xmlNode.Attributes["sqlServer"]),
                    NodeText(xmlNode.Attributes["type"]));

                if (("varchar" == NodeText(xmlNode.Attributes["type"]))
                    || ("char" == NodeText(xmlNode.Attributes["type"])))
                {
                    StringLengthHashtable.Add(NodeText(xmlNode.Attributes["sqlServer"]),
                    NodeText(xmlNode.Attributes["length"]));//映射的xml文件中没有长度信息？
                }
            }
        }

        private string NodeText(XmlNode xmlNode)
        {
            if (xmlNode == null)
            {
                return "";
            }
            else
            {
                return xmlNode.InnerText;
            }
        }
    }
}
