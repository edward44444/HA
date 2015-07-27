using System.Linq.Expressions;

namespace HA.Core
{
    public class OnClauseBuilder : WhereClauseBuilder
    {
        public OnClauseBuilder()
        {
        }

        public OnClauseBuilder(string alias)
            : base(alias)
        {
        }

        protected override void VisitRight(Expression node)
        {
            Visit(node);
        }
    }
}
