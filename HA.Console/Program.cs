using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using HA.Model.Foundation;
using HA.Service.Foundation;
using System.Linq;

namespace HA.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            //Insert();

            //Update();

            Page();
        }

        private static void Page()
        {
            var service = new BaseDataService();
            var list = service.Fetch<BaseDataDataModel>(t => t.RowStatus == 0 && t.Id > 10);
            var page = service.Page<BaseDataDataModel>(2, 15, t => t.RowStatus == 0 && t.Id > 10);
        }

        private static void Update()
        {
            var service = new BaseDataService();
            var list = service.Fetch<BaseDataDataModel>(t=>t.RowStatus==0);
            service.Update(list[0]);
            service.Update(list[0], t => t.RowStatus);
            service.Update(new Expression<Func<BaseDataDataModel, object>>[]{t => t.Name, t => t.GroupCode}, list, t => t.RowStatus, t => t.Code);
        }

        private static void Insert()
        {
            var service = new BaseDataService();
            var list = new List<BaseDataDataModel>
            {
                new BaseDataDataModel {GroupCode = "X", Code = "1", Name = "edward4", CreatedBy = "edward"},
                new BaseDataDataModel {GroupCode = "X", Code = "1", Name = "edward5", CreatedBy = "edward"}
            };
            var list2 = new List<BaseDataDataModel>();
            //list2.Add(new BaseDataDataModel { GroupCode = "Y", Code = "1", Name = "edward4", CreatedBy = "edward" });
            //list2.Add(new BaseDataDataModel { GroupCode = "Y", Code = "1", Name = "edward5", CreatedBy = "edward" });
            var listGroup = new List<BaseDataGroupDataModel>
            {
                new BaseDataGroupDataModel {Code = "A",CreatedOn=DateTime.Now, BaseDataList = list},
                new BaseDataGroupDataModel {Code = "B", BaseDataList = list2}
            };
            service.Insert(listGroup);
        }
    }
}
