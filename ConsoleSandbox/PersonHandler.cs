using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleSandbox
{
    public class PersonHandler : IMessageHandler<Person>
    {
        public DataSet Process(Person message)
        {
            DataSet ds = CreateDataSetSchema();

            DataRow p = ds.Tables["Person"].NewRow();

            p["ID"] = message.ID;
            p["FirstName"] = message.FirstName;
            p["LastName"] = message.LastName;
            p["DateOfBirth"] = message.DateOfBirth;

            ds.Tables["Person"].Rows.Add(p);

            foreach (var address in message.Addresses)
            {
                DataRow a = ds.Tables["Address"].NewRow();

                a["ID"] = address.ID;
                a["PersonID"] = message.ID;
                a["Address1"] = address.Address1;
                a["Address2"] = address.Address2;
                a["City"] = address.City;
                a["State"] = address.State;
                a["PostalCode"] = address.PostalCode;
                a["AddressType"] = address.Type.ToString();

                ds.Tables["Address"].Rows.Add(a);
            }

            return ds;
        }

        public Person Consume(DataRow dr)
        {
            Person p = new Person();

            p.ID = Convert.ToInt32(dr["ID"]);

            return p;
        }

        public DataSet CreateDataSetSchema()
        {
            DataSet ds = new DataSet();

            ds.Tables.Add(new DataTable("Person"));

            ds.Tables[0].Columns.Add(new DataColumn("ID", typeof(int)));
            ds.Tables[0].Columns.Add(new DataColumn("FirstName", typeof(string))
                {
                });

            ds.Tables[0].Columns.Add(new DataColumn("LastName", typeof(string)));
            ds.Tables[0].Columns.Add(new DataColumn("DateOfBirth", typeof(DateTime)));

            ds.Tables.Add(new DataTable("Addresses"));
            ds.Tables[1].Columns.Add(new DataColumn("PersonID", typeof(int)));
            ds.Tables[1].Columns.Add(new DataColumn("ID", typeof(int)));
            ds.Tables[1].Columns.Add(new DataColumn("Address1", typeof(string)));
            ds.Tables[1].Columns.Add(new DataColumn("Address2", typeof(string)));
            ds.Tables[1].Columns.Add(new DataColumn("City", typeof(string)));
            ds.Tables[1].Columns.Add(new DataColumn("State", typeof(string)));
            ds.Tables[1].Columns.Add(new DataColumn("PostalCode", typeof(string)));
            ds.Tables[1].Columns.Add(new DataColumn("AddressType", typeof(string)));

            return ds;
        }
    }
}
