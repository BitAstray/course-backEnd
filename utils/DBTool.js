const { Op } = require('sequelize')
class DBTool {
  /**
   * 根据查询参数对模型进行查询
   * @param {*} model Sequelize模型
   * @param {*} options 查询的参数
   * @returns 查询的结果
   */
  static async paginate(model, options) {
    const { page, size } = options
    const offset = Number((page - 1) * size)
    const limit = Number(size)
    const { count, rows } = await model.findAndCountAll({ ...options, offset, limit })
    // 计算总页数
    let total_pages = null
    count / size == 0 ? (total_pages = count / size) : (total_pages = Math.ceil(count / size))
    return { current_data: rows, total_record: count, total_pages }
  }


  /**
   * 生成查询的参数
   * @param {*} condition 查询的条件 -> id绝对查询，其他的根据searchFields模糊匹配
   * @param {*} searchFields 模糊匹配的查询字段
   * @param {*} gmt_start 查询的开始时间段
   * @param {*} gmt_end 查询的结束时间段
   * @returns 查询的参数
   */
  static generateWhereOptions({ condition, gmt_start, gmt_end, searchFields }) {
    let whereOptions = { del: 0 }

    // 判断是否有查询条件
    if (condition) {
      // 如果有查询条件就将其合并到whereOptions中
      whereOptions = {
        [Op.and]: [
          { del: 0 },
          {
            // id为绝对匹配，其他的根据searchFields模糊匹配
            [Op.or]: [
              { id: condition },
              ...searchFields.map((item) => {
                return {
                  [item]: {
                    [Op.like]: `%${condition}%`
                  }
                }
              })
            ]
          }
        ]
      }
    }

    // 如果有开始时间和结束时间
    if (gmt_start && gmt_end) {
      // 将之前的条件和时间合并
      // 之前的条件为必要条件，时间条件则是在gmt_create和gmt_modified的任意包括值
      whereOptions = {
        [Op.and]: [
          whereOptions,
          {
            [Op.or]: [
              {
                gmt_create: {
                  [Op.between]: [gmt_start, gmt_end]
                }
              },
              {
                gmt_modified: {
                  [Op.between]: [gmt_start, gmt_end]
                }
              }
            ]
          }
        ]
      }
      // 如果只有开始时间
    } else if (gmt_start) {
      // 将之前的条件和时间合并
      // 之前的条件为必要条件，时间条件则是在gmt_create和gmt_modified的任意开始值
      whereOptions = {
        [Op.and]: [
          whereOptions,
          {
            [Op.or]: [
              {
                gmt_create: {
                  [Op.gte]: gmt_start
                }
              },
              {
                gmt_modified: {
                  [Op.gte]: gmt_start
                }
              }
            ]
          }
        ]
      }
      // 如果只有结束条件
    } else if (gmt_end) {
      // 将之前的条件和时间合并
      // 之前的条件为必要条件，时间条件则是在gmt_create和gmt_modified的任意结束值
      whereOptions = {
        [Op.and]: [
          whereOptions,
          {
            [Op.or]: [
              {
                gmt_create: {
                  [Op.lte]: gmt_end
                }
              },
              {
                gmt_modified: {
                  [Op.lte]: gmt_end
                }
              }
            ]
          }
        ]
      }
    }

    return whereOptions
  }
}

module.exports = DBTool
