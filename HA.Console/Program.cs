using HA.Core;
using HA.Model.Foundation;
using HA.Persistence;
using HA.Service.Foundation;
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
            var service = new BaseDataService();
            var list = new List<BaseDataDataModel>();
            list.Add(new BaseDataDataModel { GroupCode = "X", Code = "1", Name = "edward4", CreateBy = "edward" });
            list.Add(new BaseDataDataModel { GroupCode = "X", Code = "1", Name = "edward5", CreateBy = "edward" });
            service.Insert(list);
        }
    }
}
