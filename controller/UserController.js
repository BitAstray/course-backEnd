/**
 * @param register 注册接口
 * @param forget 设置密码接口
 * @param login 登录接口
 * @param detail 用户信息接口
 * @param duration_record 上报学习时长
 * @param play_record 用户播放记录
 * @param update_img 更新个人头像
 * @param update 更新个人资料
 */


const UserService = require('../service/UserService.js')

const UserController = {
  register: async (req, res) => {
    let { phone, code } = req.body
    let handleRes = await UserService.register(phone, code)
    res.send(handleRes)
  },
  forget: async (req, res) => {
    let handleRes = await UserService.forget(req)
    res.send(handleRes)
  },
  login: async (req, res) => {
    let handleRes = await UserService.login(req)
    res.send(handleRes)
  },
  detail: async (req, res) => {
    let handleRes = await UserService.detail(req)
    res.send(handleRes)
  },
  duration_record: async (req, res) => {
    let handleRes = await UserService.duration_record(req)
    res.send(handleRes)
  },
  play_record: async (req, res) => {
    let handleRes = await UserService.play_record(req)
    res.send(handleRes)
  },
  update_img: async (req, res) => {
    let handleRes = await UserService.update_img(req)
    res.send(handleRes)
  },
  update: async (req, res) => {
    let handleRes = await UserService.update(req)
    res.send(handleRes)
  },
}

module.exports = UserController