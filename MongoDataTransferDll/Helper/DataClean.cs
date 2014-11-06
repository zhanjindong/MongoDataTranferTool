using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections;
using System.IO;
using System.Xml;
using OSSP.BLIService.DataTransferDll;

namespace DataTransferDll
{
    public static class DataClean
    {
        /// <summary>
        /// 字段编号映射表
        /// </summary>
        private static Dictionary<string, string> filedNumberMap = new Dictionary<string, string>();
        private static bool IsMapKey = true;

        static DataClean()
        {
            string xmlPath = System.Environment.CurrentDirectory + @"\xml\FieldNumber_map.xml";
            string xmlGlobalPath = System.Environment.CurrentDirectory + @"\xml\global.xml";
            XmlDocument xmlDoc = new XmlDocument();
            XmlDocument xmlGlobalDoc = new XmlDocument();
            xmlDoc.Load(xmlPath);
            xmlGlobalDoc.Load(xmlGlobalPath);
            string bStr = NodeText(xmlGlobalDoc.SelectSingleNode("/globalConfig/isMapKey"));
            bool.TryParse(bStr, out IsMapKey);

            XmlNodeList xmlNodeList = xmlDoc.SelectNodes("/fields/field");
            foreach (XmlNode xmlNode in xmlNodeList)
            {
                string[] fields = NodeText(xmlNode.Attributes["name"]).Split(new char[] { ',' });
                string number = NodeText(xmlNode.Attributes["number"]);

                foreach (var field in fields)
                {
                    if (!filedNumberMap.ContainsKey(field.ToLower()))
                    {
                        filedNumberMap.Add(field.ToLower(), number);
                    }
                }
            }
        }

        private static string MapKey(string key)
        {
            string tmpKey = key.ToLower();
            if (filedNumberMap.ContainsKey(tmpKey))
            {
                return filedNumberMap[tmpKey];
            }
            else
                return key;
        }

        private static string CleanValue(string value)
        {
            value = value.Replace("\r\n", "").Replace("\n", "").Replace("\r", "").Replace("~", "");
            return value;
        }

        public static void ExportToFile(DataRow[] dataRows, string directory, string fileName, MapContainer mapContainer)
        {
            FileStream fs = null;
            StreamWriter streamWriter = null;

            try
            {
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                string filePath = directory + fileName;

                StringBuilder logContent = new StringBuilder();
                for (int i = 0; i < dataRows.Length; i++)
                {
                    int k = 1;
                    //这里映射是以global.xml中的sqlserver字段为准
                    foreach (string str in mapContainer.MapHashtable.Keys)
                    {
                        string tmpKey = string.Empty;
                        if (IsMapKey)
                            tmpKey = MapKey(str);
                        else
                            tmpKey = str;

                        string value = string.Empty;
                        if (@"datetime" == Convert.ToString(mapContainer.TypeHashtable[str]) && dataRows[i][str] != DBNull.Value)
                        {
                            value = Convert.ToDateTime(dataRows[i][str]).ToString("yyyy-MM-dd HH:mm:ss");
                        }
                        else if ((@"char" == Convert.ToString(mapContainer.TypeHashtable[str]) || @"varchar" == Convert.ToString(mapContainer.TypeHashtable[str]))
                            && dataRows[i][str] != DBNull.Value)
                        {
                            value = CleanValue(dataRows[i][str].ToString());
                        }
                        else
                        {
                            value = dataRows[i][str].ToString();
                        }

                        if (k < mapContainer.MapHashtable.Keys.Count)
                        {
                            logContent.Append(tmpKey + '~' + value + (char)31);
                        }
                        else
                        {
                            logContent.Append(tmpKey + '~' + value);
                        }

                        k++;
                    }
                    logContent.Append("\n");
                }

                fs = new FileStream(filePath, FileMode.Append);
                streamWriter = new StreamWriter(fs);
                streamWriter.BaseStream.Seek(0, SeekOrigin.End);
                streamWriter.WriteLine(logContent.Replace("<![CDATA[", "").Replace("]]>", ""));
            }
            finally
            {
                if (streamWriter != null)
                {
                    streamWriter.Flush();
                    streamWriter.Close();
                }
                if (fs != null)
                {
                    fs.Dispose();
                }
            }
        }

        private static string NodeText(XmlNode xmlNode)
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
