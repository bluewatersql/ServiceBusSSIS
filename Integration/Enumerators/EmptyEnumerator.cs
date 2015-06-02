using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.Enumerators
{
    public sealed class EmptyEnumerator : IEnumerator
    {
        public object Current
        {
            get
            {
                throw new InvalidOperationException();
            }
        }

        public bool MoveNext()
        {
            return false;
        }

        public void Reset()
        {
        }
    }
}
