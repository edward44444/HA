namespace HA.Model.ViewModel
{
    public class BaseViewModel
    {
        /// <summary>
        /// 页数
        /// </summary>
        public long CurrentPage { get; set; }

        /// <summary>
        /// 每页行数
        /// </summary>
        public long ItemsPerPage { get; set; }
    }
}
