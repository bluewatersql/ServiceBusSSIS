using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.DataFlow
{
    public class DTSOutputMetadata
    {
        #region Properties
        public string Name { get; set; }
        public int ID { get; set; }
        public List<DTSColumnMetadata> Columns { get; set; }
        public Type ObjectType { get; set; }
        public Dictionary<string, int> OutputColumnToBufferMap { get; set; }
        #endregion

        public DTSOutputMetadata()
        {
            this.Columns = new List<DTSColumnMetadata>();
            this.OutputColumnToBufferMap = new Dictionary<string, int>();
        }
    }
}
