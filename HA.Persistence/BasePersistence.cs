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

        public T Insert<T>(object model)
        {
            return (T)Database.Insert(model);
        }

        public void BulkCopy<T>(List<T> collection)
        {
            Database.BulkCopy(collection);
        }

        public int BulkInsert<T>(List<T> collection)
        {
            return Database.BulkInsert(collection);
        }

        public int BulkInsert<T>(List<T> collection, Func<string, T, string> sqlRebuild)
        {
            return Database.BulkInsert(collection, sqlRebuild);
        }

        public int BulkInsert<T>(List<T> collection, string whereSql)
        {
            return Database.BulkInsert(collection, whereSql);
        }

        public int Update<T>(T poco, params Expression<Func<T, object>>[] expressions)
        {
            return Database.Update(poco, expressions);
        }

        public int BulkUpdate<T>(IList<T> collection, params Expression<Func<T, object>>[] expressions)
        {
            return Database.BulkUpdate(collection, expressions);
        }

        public int BulkUpdate<T>(IList<T> collection, Expression<Func<T, object>>[] primaryKeyExpressions, params Expression<Func<T, object>>[] expressions)
        {
            return Database.BulkUpdate(collection, primaryKeyExpressions, expressions);
        }

        public int BulkUpdate<T>(IList<T> collection, Expression<Func<T, object>>[] primaryKeyExpressions, Func<string, T, string> sqlRebuild, params Expression<Func<T, object>>[] expressions)
        {
            return Database.BulkUpdate(collection, primaryKeyExpressions, sqlRebuild, expressions);
        }
    }
}
