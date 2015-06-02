using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.DataFlow
{
    public static class PipelineHelper
    {
        public static void CreateProperty(IDTSComponentMetaData100 metadata, string name, dynamic value)
        {
            CreateProperty(metadata, name, null, value);
        }

        public static void CreateProperty(IDTSComponentMetaData100 metadata, string name, string description, dynamic value)
        {
            IDTSCustomProperty100 p = metadata.CustomPropertyCollection.New();
            p.Name = name;

            if (!string.IsNullOrEmpty(description))
                p.Description = description;

            p.Value = value;


        }
    }
}
