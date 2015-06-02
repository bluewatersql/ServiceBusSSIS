using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleSandbox
{
    [AttributeUsage(AttributeTargets.All)]
    public class SSISDataObjectMetaData: Attribute
    {
        public DataType Type { get; set; }
        public int Length { get; set; }
        public int Precision { get; set;  }
        public int Scale { get; set; }
        public int CodePage { get; set; }
    }
}
