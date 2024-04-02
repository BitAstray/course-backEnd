/**
 * @param {*} searchUser 获取全部用户
 */
const AdminService = require('../service/AdminService')

const BarrageController = {
  async searchUser(req, res) {
    let handleRes = await AdminService.searchUser()
    res.send(handleRes)
  },
}

module.exports = BarrageController