using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleSandbox
{
    public class DtsColumn
    {
        #region Properties
        public string Name { get; set; }
        public Type BaseType { get; set; }
        public DataType DataType { get; set; }
        public int Length { get; set; }
        public int Precision { get; set; }
        public int Scale { get; set; }
        public int CodePage { get; set; }
        #endregion

        public DtsColumn(string prefix, PropertyInfo pInfo)
        {
            if (string.IsNullOrEmpty(prefix))
                this.Name = pInfo.Name;
            else
                this.Name = string.Format("{0}_{1}", prefix, pInfo.Name);

            this.BaseType = pInfo.PropertyType;
            this.DataType = DataType.DT_NULL;
            this.Length = 0;
            this.Scale = 0;
            this.Precision = 0;
            this.CodePage = 0;    

            switch (pInfo.PropertyType.ToString())
            {
                case "System.Boolean":
                    this.DataType = DataType.DT_BOOL;
                    break;
                case "System.Byte":
                    this.DataType = DataType.DT_UI1;
                    break;
                case "System.Char":
                    this.DataType = DataType.DT_WSTR;
                    this.Length = 1;
                    break;
                case "System.DateTime":
                    this.DataType = DataType.DT_DBTIMESTAMP;
                    break;
                case "System.DateTimeOffset":
                    this.DataType = DataType.DT_DBTIMESTAMPOFFSET;
                    this.Scale = 7;
                    break;
                case "System.Decimal":
                    this.DataType = DataType.DT_DECIMAL;
                    this.Scale = 10;
                    break;
                case "System.Double":
                    this.DataType = DataType.DT_R8;
                    break;
                case "System.Int16":
                    this.DataType = DataType.DT_I2;
                    break;
                case "System.Int32":
                    this.DataType = DataType.DT_I4;
                    break;
                case "System.Int64":
                    this.DataType = DataType.DT_I8;
                    break;
                case "System.SByte":
                    this.DataType = DataType.DT_I1;
                    break;
                case "System.Single":
                    this.DataType = DataType.DT_R4;
                    break;
                case "System.String":
                    this.DataType = DataType.DT_WSTR;
                    this.Length = byte.MaxValue;
                    break;
                case "System.TimeSpan":
                    this.DataType = DataType.DT_I8;
                    break;
                case "System.UInt16":
                    this.DataType = DataType.DT_UI2;
                    break;
                case "System.UInt32":
                    this.DataType = DataType.DT_UI4;
                    break;
                case "System.UInt64":
                    this.DataType = DataType.DT_UI8;
                    break;
                case "System.Guid":
                    this.DataType = DataType.DT_GUID;
                    break;
            }

            if (this.DataType == DataType.DT_NULL)
            {
                this.DataType = DataType.DT_WSTR;
                this.Length = byte.MaxValue;
            }
        }
    }
}
