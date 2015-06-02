using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleSandbox
{
    public class Person
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int ID { get; set; }
        public DateTime DateOfBirth { get; set; }
        public Address[] Addresses { get; set; }
        public Account Account { get; set; }
    }
}
