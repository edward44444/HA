using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using HA.Core;

namespace HA.Persistence
{
    public class BasePersistence
    {
        private readonly ExpressionVisitor _expressionVisitor;

        public Database Database { get; private set; }

        public BasePersistence()
        {
            Database = new Database("HA");
            _expressionVisitor = new HaExpressionVisitor();
        }

        public List<T> Fetch<T>(Expression<Func<T, bool>> predicate)
        {
            var expression = _expressionVisitor.Visit(predicate);
            return null;
        }
    }
}
