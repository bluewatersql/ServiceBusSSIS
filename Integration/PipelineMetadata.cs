using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus
{
    [AttributeUsage(AttributeTargets.Property)]
    public class PipelineMetaData : Attribute
    {
        public DataType Type { get; set; }
        public int Length { get; set; }
        public int Precision { get; set; }
        public int Scale { get; set; }
        public int CodePage { get; set; }
    }
}
