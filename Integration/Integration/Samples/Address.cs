﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.ServiceBus.Integration.Samples
{
    public class Address
    {
        public int ID { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string PostalCode { get; set; }
        public AddressType Type { get; set; }
    }

    public enum AddressType
    {
        BILLING,
        SHIPPING,
        ALTERNATE
    }
}
