using HA.Core;
using HA.Model.Foundation;
using HA.Persistence;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HA.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var db = new BasePersistence();
            var lst = db.Fetch<BaseData>(t => t.CreateBy == "edward" || t.Name == "Bill");
        }
    }
}
