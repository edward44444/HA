﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using HA.Core;
using HA.Model.Foundation;
using HA.Service.Foundation;

namespace HA.Console
{


    class Program
    {
        static void Main(string[] args)
        {
            //Insert();


            //var db = new Database("HA");
            //var model = new BaseDataDataModel { Id = 1000, Path = "/1/", GroupCode = "SB", Code = "A", Name = "测试", CreatedBy = "edward", CreatedOn = DateTime.Now, RowStatus = 10 };
            ////var list = new List<BaseDataDataModel>();
            ////list.Add(model);
            ////list.Add(new BaseDataDataModel { Id = 2000, Path = "/1/", GroupCode = "SB2", Code = "A", Name = "测试", CreatedBy = "edward", CreatedOn = DateTime.Now, RowStatus = 10 });
            //db.BulkUpdate(new Expression<Func<BaseDataDataModel, object>>[] { t => t.Name }, new[] { model }, t => t.CreatedBy);


            //Insert();

            //Update();

            //var db=new Database("HA");
            //var list = db.Page<BaseDataDataModel>(1, 1, new Sql<BaseDataDataModel>().Where(t => t.RowStatus == 0));

            //var model = new BaseDataDataModel { Id = 1000,Path="/1/" };
            //var num =Convert.ToInt32(db.Insert(model));
            //db.Insert(new List<BaseDataDataModel> { model });
            //db.Update(model);
            //db.Update(model, t => t.Name, t => t.GroupCode);
            //db.BulkUpdate(new List<BaseDataDataModel> { model }, t => t.Name);
            //Page();

            var db = new Database("HA");
            //var sql = new Sql<BaseDataDataModel>("T1");
            //sql.Select(t => t.GroupCode).From();
            //sql.InnerJoin<BaseDataGroupDataModel>("T2").On((t1, t2) => t1.RowStatus == t2.RowStatus || t1.Name == t2.Name || 0 == t2.RowStatus);
            //sql.Where(t => t.RowStatus != 0 && !(t.Name == "eeee" || t.GroupCode == "tt"));
            //sql.Where(t => t.Id == 1000);
            //sql.Where("T2.RowStatus=@0", 100);
            //sql.OrderByDescending(t => t.Id, t => t.CreatedBy).OrderBy(t => t.GroupCode);

            var sql = new Sql<BaseDataDataModel>();

            var lst = db.Fetch(sql);

//            var s = @"
//SELECT COUNT(*) FROM [FD_BaseData]  WITH(NOLOCK)
//INNER JOIN FD_BaseDataGroup  WITH(NOLOCK)
//ON ([FD_BaseData].[RowStatus] = [FD_BaseDataGroup].[RowStatus] OR [FD_BaseData].[BDName] = [FD_BaseDataGroup].[BDGName])
//WHERE (([FD_BaseData].[RowStatus] != @0 AND NOT ([FD_BaseData].[BDName] = @1 OR [FD_BaseData].[BDGroupCode] = @2)))
//AND ([FD_BaseData].[BDID] = @3)
//ORDER BY FD_BaseData.[BDID] DESC 
//";
//            var reg = new Regex(@"\bORDER\s+BY\s+(?:\((?>\((?<depth>)|\)(?<-depth>)|.?)*(?(depth)(?!))\)|[\w\(\)\[\]\.])+(?:\s+(?:ASC|DESC))?(?:\s*,\s*(?:\((?>\((?<depth>)|\)(?<-depth>)|.?)*(?(depth)(?!))\)|[\w\(\)\[\]\.])+(?:\s+(?:ASC|DESC))?)*\s*\z", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.Compiled);

//            var match = reg.Match(s);


            //var list = new List<BaseDataDataModel>
            //{
            //    new BaseDataDataModel {Path="/", GroupCode = "X", Code = "1", Name = "edward4", CreatedBy = "edward",Id=1000},
            //    new BaseDataDataModel {Path="/",GroupCode = "X", Code = "1", Name = "edward5", CreatedBy = "edward"}
            //};
            //db.BulkInsert(list);



            //var sql = new Sql<BaseDataDataModel>("T1")
            //    .Select(t => t.Name)
            //    .From()
            //    .InnerJoin<BaseDataGroupDataModel>("T2").On((t1,t2)=>t1.RowStatus==t2.RowStatus)
            //    .Where(t => t.RowStatus == 10 && t.Name == "edward")
            //    .Where(t => t.Id < 1000);
            //var list = db.Fetch(sql);
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
                new BaseDataGroupDataModel {Code = "A",CreatedOn=DateTime.Now, BaseDataList = list},
                new BaseDataGroupDataModel {Code = "B", BaseDataList = list2}
            };
            service.BulkInsert(listGroup);
        }
    }
}
