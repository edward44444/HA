namespace HA.Core
{
    public class OnClauseBuilder : WhereClauseBuilder
    {
        private readonly string _aliasLeft;
        private readonly string _aliasRight; 

        public OnClauseBuilder(string aliasLeft, string aliasRight)
        {
            _aliasLeft = aliasLeft;
            _aliasRight = aliasRight;
        }

        protected override void VisitRight(System.Linq.Expressions.Expression node)
        {
            _alias = _aliasRight;
            Visit(node);
        }

        protected override void VisitLeft(System.Linq.Expressions.Expression node)
        {
            _alias = _aliasLeft;
            Visit(node);
        }
    }
}
