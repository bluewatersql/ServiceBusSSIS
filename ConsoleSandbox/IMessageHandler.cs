using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleSandbox
{
    public interface IMessageHandler<T> where T : class
    {
        DataSet Process(T message);

        T Consume(DataRow d);

        DataSet CreateDataSetSchema();
    }
}
