using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.Integration.Samples
{
    public class Account
    {
        public int ID { get; set; }
        public decimal Balance { get; set; }
        public Customer Customer { get; set; }
        public Address[] Addresses { get; set; }
    }
}
