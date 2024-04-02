const huaWeiYun = require('../config/huaweiCloud')
const SecretTool = require('../utils/SecretTool')
const CodeEnum = require('../utils/CodeEnum')
const DB = require("../config/sequelize")
const BackCode = require('../utils/BackCode')

const getPlayUrlService = {
  get_play_url: async (req) => {
    const { episodeId, token } = req

    if (!(episodeId && token)) {
      return BackCode.buildError({ msg: '缺少必传参数' })
    }

    // 获取集数据
    let episode = await DB.Episode.findOne({ where: { id: episodeId }, raw: true })
    if (!episode) {
      return BackCode.buildResult(CodeEnum.COURSE_VIDEO_NO_EXIST)
    }
    // 获取用户信息
    let tokenUser = SecretTool.jwtVerify(token)

    // 判断当前集是否购买过
    let order = await DB.ProductOrder.findOne({ where: { account_id: tokenUser.id, product_id: episode.product_id } })
    if ((!order) && episode.free === 1) {
      return BackCode.buildResult(CodeEnum.COURSE_VIDEO_NO_PERMISSION)
    }

    // 更新播放记录 - 查看用户和课程是否有播放记录
    let playRecode = await DB.PlayRecord.findOne({ where: { account_id: tokenUser.id, product_id: episode.product_id } })
    if (playRecode) {
      // 有播放记录则更新播放集
      let isHas = playRecode.learn_ids.split(',').includes(episodeId.toString())
      let learn_ids = isHas ? playRecode.learn_ids : playRecode.learn_ids + ',' + episodeId
      await DB.PlayRecord.update({ learn_ids, current_episode_id: episodeId }, { where: { account_id: tokenUser.id, product_id: episode.product_id } })
    } else {
      // 无播放记录则新增
      let playInfo = { product_id: episode.product_id, current_episode_id: episodeId, account_id: tokenUser.id, learn_ids: episodeId, pay_status: order ? 'pay' : 'new' }
      await DB.PlayRecord.create(playInfo)
    }


    // 传入视频媒资id获取播放地址
    let url = await huaWeiYun.getVideoUrl(episode.hwyun_id)

    if (url.length < 0) {
      return BackCode.buildResult(CodeEnum.COURSE_VIDEO_NO_EXIST)
    }

    return BackCode.buildSuccessAndData({ data: { playResult: url[0], episodeId: episodeId } })
  }
}

module.exports = getPlayUrlService