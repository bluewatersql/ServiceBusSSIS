using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace ConsoleSandbox
{
    class Program
    {
        static void Main(string[] args)
        {
            Person p = new Person()
            {
                ID = 12345,
                FirstName = "Tim",
                LastName = "Tester",
                DateOfBirth = new DateTime(1980, 1, 1),
                Account = new Account(){
                    ID = 999,
                    Balance = 5000
                },
                Addresses = new Address[1] { 
                    new Address(){
                        ID = 123,
                        Address1 = "1234 Main St",
                        Address2 = "Suite 100",
                        City = "Tampa",
                        State = "Florida",
                        PostalCode = "34638",
                        Type = AddressType.BILLING
                    }
                }
            };

            var t = p.GetType().Name;

            var cols = Flatten(p);

            Console.WriteLine("Press enter to exit..");
            Console.ReadLine();
        }

        private static List<DtsColumn> Flatten<T>(T obj)
        {
            return Flatten(null, obj);
        }

        private static List<DtsColumn> Flatten<T>(string prefix, T obj)
        {
            List<DtsColumn> columns = new List<DtsColumn>();

            Type type = typeof(T);
            Assembly ass = Assembly.GetExecutingAssembly();

            Console.WriteLine(ass.FullName);
            Console.WriteLine(type.FullName);

            foreach (PropertyInfo p in type.GetProperties())
            {
                if (!p.PropertyType.IsArray)
                {
                    if ((IsValidType(p.PropertyType) || p.PropertyType.IsEnum))
                    {
                        var d = new DtsColumn(prefix, p);
                        columns.Add(d);

                        var v = p.GetValue(obj, new object[0]);

                        Console.WriteLine("Property: {0} - {1} - {2}", d.Name, d.DataType, v);
                    }
                    else if (p.PropertyType.IsClass)
                    {
                        if (string.IsNullOrEmpty(prefix))
                            prefix = p.Name;
                        else
                            prefix = string.Format("{0}_{1}", prefix, p.Name);

                        dynamic v = p.GetValue(obj, new object[0]);

                        var cols = Flatten(prefix, v);
                        columns.AddRange(cols);
                    }
                }
            }

            return columns;
        }

        private static T GetValue<T>(PropertyInfo pInfo, Object obj1)
        {
            return (T)pInfo.GetValue(obj1, new object[0]);
        }

        private static bool IsValidType(Type type)
        {
            bool bValid = false;

            switch (type.ToString())
            {
                case "System.Boolean":
                case "System.Byte":
                case "System.Char":
                case "System.DateTime":
                case "System.DateTimeOffset":
                case "System.Decimal":
                case "System.Double":
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                case "System.SByte":
                case "System.Single":
                case "System.String":
                case "System.TimeSpan":
                case "System.UInt16":
                case "System.UInt32":
                case "System.UInt64":
                case "System.Guid":
                    bValid = true;
                    break;
                default:
                    bValid = false;
                    break;
            }

            return bValid;
        }

        #region One Way
        public static DataSet ProcessMessage<T>(T message) where T : class
        {
            Type handlerType = typeof(IMessageHandler<>);
            Type[] typeArgs = { message.GetType() };
            Type constructed = handlerType.MakeGenericType(typeArgs);

            Assembly assembly = Assembly.GetExecutingAssembly();
            Type type = null;

            foreach (var t in assembly.GetTypes())
            {
                if (t.GetInterfaces().Where(i => i.FullName.Equals(constructed.FullName)).SingleOrDefault() != null && 
                    t.IsClass)
                {
                    type = t;
                    break;
                }
            }

            DataSet ds = null;

            if (type != null)
            {
                ConstructorInfo ci = type.GetConstructor(new Type[] { });
                var handler = ci.Invoke(new Object[] { });

                var methodInfo = constructed.GetMethod("Process");
                ds = (DataSet)methodInfo.Invoke(handler, new[] { message });
            }

            return ds;
        }

        public static T ConsumeMessage<T>(DataRow dr) where T : class
        {
            Type handlerType = typeof(IMessageHandler<>);
            Type[] typeArgs = {typeof(T)};
            Type constructed = handlerType.MakeGenericType(typeArgs);

            Assembly assembly = Assembly.GetExecutingAssembly();
            Type type = null;

            foreach (var t in assembly.GetTypes())
            {
                if (t.GetInterfaces().Where(i => i.FullName.Equals(constructed.FullName)).SingleOrDefault() != null &&
                    t.IsClass)
                {
                    type = t;
                    break;
                }
            }

            T msgBody = null;

            if (type != null)
            {
                ConstructorInfo ci = type.GetConstructor(new Type[] { });
                var handler = ci.Invoke(new Object[] { });

                var methodInfo = constructed.GetMethod("Consume");
                msgBody = (T)methodInfo.Invoke(handler, new[] { dr });
            }

            return msgBody;
        }
        #endregion
    }
}
