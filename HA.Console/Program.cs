using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using HA.Core;
using HA.Model.Foundation;
using HA.Model.Statistics;
using HA.Service.Foundation;

namespace HA.Console
{


    class Program
    {
        static void Main(string[] args)
        {

            var db = new Database("HA");

            var sql = new Sql<HouseData>("T");
            var list = db.Fetch(sql);

            //var sql = new Sql<BaseDataDataModel>("T1");
            //sql.Select(t=>t.RowStatus).From();
            //sql.InnerJoin<BaseDataGroupDataModel>("T2").On((t1, t2) => t1.RowStatus == t2.RowStatus || t1.Name == t2.Name || 0 == t2.RowStatus);
            //sql.Where(t => t.RowStatus != 0 && !(t.Name == "eeee" || t.GroupCode == "tt"));
            //sql.Where(t => t.Id == 1000);
            //sql.Where("T2.RowStatus=@0", 100);
            //sql.OrderByDescending(t => t.Id, t => t.CreatedBy).OrderBy(t => t.GroupCode);

            //var list=db.Fetch(sql);

            //var group = new BaseDataGroupDataModel
            //{
            //    Code = "BD_Region",
            //    Name = "地区",
            //    CreatedBy = "system",
            //    CreatedOn = DateTime.Now
            //};
            //var bdList = new List<BaseDataDataModel>
            //{
            //    new BaseDataDataModel
            //    {
            //        Code = "051201",
            //        Name = "吴中区",
            //        CreatedBy = "system",
            //        CreatedOn = DateTime.Now,
            //        GroupCode = "BD_Region",
            //        Path = "/1/"
            //    },
            //    new BaseDataDataModel
            //    {
            //        Code = "051200",
            //        Name = "苏州市",
            //        CreatedBy = "system",
            //        CreatedOn = DateTime.Now,
            //        GroupCode = "BD_Region",
            //        Path = "/"
            //    }
            //};
            //db.Insert(group);
            //db.BulkInsert(bdList);

        }

        private static void Page()
        {
            var service = new BaseDataService();
            var list = service.Fetch(new Sql<BaseDataDataModel>("T").Select(t=>t.Name).From().Where(t => t.RowStatus == 0 && t.Id > 10).OrderBy(t=>t.CreatedOn,t=>t.RowStatus).OrderByDescending(t=>t.Name));
            var page = service.Page(2, 15, new Sql<BaseDataDataModel>().Where(t => t.RowStatus == 0 && t.Id > 10));
        }

        private static void Update()
        {
            var service = new BaseDataService();
            var list = service.Fetch(new Sql<BaseDataDataModel>().Where(t => t.RowStatus == 0));
            service.Update(list[0]);
            service.Update(list[0], t => t.RowStatus);
            //service.Update(new Expression<Func<BaseDataDataModel, object>>[]{t => t.Name, t => t.GroupCode}, list, t => t.RowStatus, t => t.Code);
        }

        private static void Insert()
        {
            var service = new BaseDataService();
            var list = new List<BaseDataDataModel>
            {
                new BaseDataDataModel {Path="/", GroupCode = "X", Code = "1", Name = "edward4", CreatedBy = "edward"},
                new BaseDataDataModel {Path="/",GroupCode = "X", Code = "1", Name = "edward5", CreatedBy = "edward"}
            };
            var list2 = new List<BaseDataDataModel>();
            //list2.Add(new BaseDataDataModel { GroupCode = "Y", Code = "1", Name = "edward4", CreatedBy = "edward" });
            //list2.Add(new BaseDataDataModel { GroupCode = "Y", Code = "1", Name = "edward5", CreatedBy = "edward" });
            var listGroup = new List<BaseDataGroupDataModel>
            {
                new BaseDataGroupDataModel {Code = "A",CreatedOn=DateTime.Now},
                new BaseDataGroupDataModel {Code = "B"}
            };
            service.BulkInsert(listGroup);
        }
    }
}
