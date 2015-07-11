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
            list.Add(new BaseDataDataModel { GroupCode = "X", Code = "1", Name = "edward4", CreatedBy = "edward" });
            list.Add(new BaseDataDataModel { GroupCode = "X", Code = "1", Name = "edward5", CreatedBy = "edward" });
            var list2 = new List<BaseDataDataModel>();
            //list2.Add(new BaseDataDataModel { GroupCode = "Y", Code = "1", Name = "edward4", CreatedBy = "edward" });
            //list2.Add(new BaseDataDataModel { GroupCode = "Y", Code = "1", Name = "edward5", CreatedBy = "edward" });
            var listGroup = new List<BaseDataGroupDataModel>();
            listGroup.Add(new BaseDataGroupDataModel { Code = "A", BaseDataList = list });
            listGroup.Add(new BaseDataGroupDataModel { Code = "B", BaseDataList = list2 });
            service.Insert(listGroup,"WHERE 1=1");

        }
    }
}
