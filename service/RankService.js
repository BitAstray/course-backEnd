const dayjs = require('dayjs')
const { zrevrange, exists, set, get } = require('../config/redisConfig')
const rankProduct = require('../mock/rankProduct.json')
const BackCode = require('../utils/BackCode')
const DB = require('../config/sequelize')
const rankDuration = require('../mock/rankDuration.json')
const { Op, QueryTypes } = require('sequelize')

const RankService = {
  hot_product: async () => {
    let time = dayjs().format('YYYY-MM-DD')
    // 获取redis当中的课程销量列表
    let result = await zrevrange({ key: `${time}:rank:hot_product`, start: 0, stop: 14 })
    // 兜底数据
    let list = result.map((item) => JSON.parse(item)).concat(rankProduct.list).slice(0, 15)
    return BackCode.buildSuccessAndData({ data: list })
  },
  duration: async () => {
    // 查询redis如果存在则使用缓存
    if (await exists('rank:duration')) {
      let rankListRedis = await get('rank:duration')
      return BackCode.buildSuccessAndData({ data: JSON.parse(rankListRedis) })
    }

    let rankSql = `select account_id,sum(duration)/60 as minute from duration_record where gmt_modified between ? and ? group by account_id order by minute desc limit ?`
    let rankListQuery = [dayjs().subtract(7, 'day').format('YYYY-MM-DD'), dayjs().format('YYYY-MM-DD'), 20]

    // 1.查询前七日学习的用户
    let rankList = await DB.sequelize.query(rankSql, { replacements: rankListQuery, type: QueryTypes.SELECT })

    // 2.查询每个用户的信息
    let idList = rankList.map((item) => item.account_id)
    let userInfoList = await DB.Account.findAll({
      attributes: ['id', 'username', 'head_img'],
      where: { id: { [Op.in]: idList } },
      raw: true
    })

    // 3.将用户观看时间和信息合并
    userInfoList.map((item) => {
      rankList.map((subItem) => {
        if (subItem.account_id == item.id) {
          item['minute'] = subItem.minute
          return item
        }
      })
    })

    // 4.兜底数据 取前15项
    userInfoList = userInfoList.concat(rankDuration.list).slice(0, 15)
    set('rank:duration', JSON.stringify(userInfoList), 604800)

    return BackCode.buildSuccessAndData({ data: userInfoList })
  },

}
module.exports = RankService


