using HA.Core;
using HA.Model.Foundation;
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
            var db = new Database("HA");
            var lst = db.Fetch<BaseData>("");

            lst.Where(t => t == null);
        }
    }
}
