const DB = require('../config/sequelize')
const BackCode = require('../utils/BackCode')

const SearchService = {
  list: async (req) => {
    let { title, page, size } = req.query
    // 筛选符合条件的课程
    let data = (await DB.Product.findAll()).filter((item) => item.title.search(new RegExp(title.replace(/([,.+?:()*\[\]^$|{}\\-])/g, '\\$1'), "i")) >= 0)
    if (page) {
      // 总个数
      let total_record = data.length

      // 计算总页数
      let total_page = null
      total_record / size == 0 ? (total_page = total_record / size) : (total_page = Math.ceil(total_record / size))

      data = { current_data: data.slice(0, size), total_page, total_record }
    }

    return BackCode.buildSuccessAndData({ data })
  }
}
module.exports = SearchService