using BluewaterSQL.DTS.ServiceBus.DataFlow;
using Microsoft.SqlServer.Dts.Pipeline;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus
{
    public interface IMessageHandler<T> where T : class
    {
        void Process(PipelineBuffer buffer, T message);
        T Consume(PipelineBuffer buffer);
        DTSOutputMetadata[] GetMetadata();
    }
}
