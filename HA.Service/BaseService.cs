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

        public T Insert<T>(T model)
        {
            return Persistence.Insert(model);
        }

        public void BulkCopy<T>(List<T> collection)
        {
            Persistence.BulkCopy(collection);
        }

        public int Insert<T>(List<T> collection)
        {
            return Persistence.Insert(collection);
        }

        public int Insert<T>(List<T> collection, Func<string, T, string> sqlRebuild)
        {
           return Persistence.Insert(collection, sqlRebuild);
        }

        public int Insert<T>(List<T> collection, string whereSql)
        {
            return Persistence.Insert(collection, whereSql);
        }

        public int Update<T>(T poco, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.Update(poco, expressions);
        }

        public int Update<T>(IList<T> collection, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.Update(collection, expressions);
        }

        public int Update<T>(Expression<Func<T, object>>[] primaryKeyExpressions, IList<T> collection, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.Update(primaryKeyExpressions, collection, expressions);
        }

        public int Update<T>(Expression<Func<T, object>>[] primaryKeyExpressions, IList<T> collection, Func<string, T, string> sqlRebuild, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.Update(primaryKeyExpressions, collection, sqlRebuild, expressions);
        }
    }
}
