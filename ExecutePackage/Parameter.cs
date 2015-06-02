using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace BluewaterSQL.DTS.Tasks
{
    [DataContract]
    public class Parameter
    {
        [DataMember]
        public string Name { get; set; }
        [DataMember]
        public ParameterType Type { get; set; }
        [DataMember]
        public object Value { get; set; }
    }
}
