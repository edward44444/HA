using System.Linq.Expressions;

namespace HA.Core
{
    public class OnClauseBuilder : WhereClauseBuilder
    {
        protected override void VisitRight(Expression node)
        {
            Visit(node);
        }
    }
}
