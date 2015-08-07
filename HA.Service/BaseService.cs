using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using HA.Core;
using HA.Persistence;

namespace HA.Service
{
    public abstract class BaseService<TPersistence> where TPersistence : BasePersistence
    {
        public TPersistence Persistence { get; private set; }

        protected BaseService()
        {
            Persistence = Activator.CreateInstance<TPersistence>();
        }

        public List<T> Fetch<T>(Sql<T> sql)
        {
            return Persistence.Fetch(sql);
        }

        public Page<T> Page<T>(long page, long itemsPerPage, Sql<T> sql)
        {
            return Persistence.Page(page, itemsPerPage, sql);
        }

        public T Insert<T>(object model)
        {
            return Persistence.Insert<T>(model);
        }

        public void BulkCopy<T>(List<T> collection)
        {
            Persistence.BulkCopy(collection);
        }

        public int BulkInsert<T>(List<T> collection)
        {
            return Persistence.BulkInsert(collection);
        }

        public int Insert<T>(List<T> collection, Func<string, T, string> sqlRebuild)
        {
            return Persistence.BulkInsert(collection, sqlRebuild);
        }

        public int Insert<T>(List<T> collection, string whereSql)
        {
            return Persistence.BulkInsert(collection, whereSql);
        }

        public int Update<T>(T poco, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.Update(poco, expressions);
        }

        public int Update<T>(IList<T> collection, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.BulkUpdate(collection, expressions);
        }

        public int Update<T>(IList<T> collection,Expression<Func<T, object>>[] primaryKeyExpressions, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.BulkUpdate(collection, primaryKeyExpressions, expressions);
        }

        public int Update<T>(IList<T> collection,Expression<Func<T, object>>[] primaryKeyExpressions, Func<string, T, string> sqlRebuild, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.BulkUpdate(collection, primaryKeyExpressions, sqlRebuild, expressions);
        }
    }
}
