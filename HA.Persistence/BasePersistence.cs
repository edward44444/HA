using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using HA.Core;

namespace HA.Persistence
{
    public abstract class BasePersistence
    {
        public Database Database { get; private set; }

        protected BasePersistence()
        {
            Database = new Database("HA");
        }

        public List<T> Fetch<T>(Sql<T> sql)
        {
            return Database.Query<T>(sql).ToList();
        }

        public Page<T> Page<T>(long page, long itemsPerPage, Sql<T> sql)
        {
            return Database.Page<T>(page, itemsPerPage, sql);
        }

        public T Insert<T>(T model)
        {
            return (T)Database.Insert(model);
        }

        public void BulkCopy<T>(List<T> collection)
        {
            Database.BulkCopy(collection);
        }

        public int Insert<T>(List<T> collection)
        {
            return Database.Insert(collection);
        }

        public int Insert<T>(List<T> collection,Func<string, T, string> sqlRebuild)
        {
           return Database.Insert(collection,sqlRebuild);
        }

        public int Insert<T>(List<T> collection,string whereSql)
        {
            return Database.Insert(collection, whereSql);
        }

        public int Update<T>(T poco, params Expression<Func<T, object>>[] expressions)
        {
            return Database.Update(poco, expressions);
        }

        public int Update<T>(IList<T> collection,params Expression<Func<T, object>>[] expressions)
        {
            return Database.Update(collection, expressions);
        }

        public int Update<T>(Expression<Func<T, object>>[] primaryKeyExpressions, IList<T> collection, params Expression<Func<T, object>>[] expressions)
        {
            return Database.Update(primaryKeyExpressions, collection, expressions);
        }

        public int Update<T>(Expression<Func<T, object>>[] primaryKeyExpressions, IList<T> collection,Func<string, T, string> sqlRebuild, params Expression<Func<T, object>>[] expressions)
        {
            return Database.Update(primaryKeyExpressions, collection, sqlRebuild,expressions);
        }
    }
}
