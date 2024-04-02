// admin.js
const express = require('express')
const router = express.Router()
const AdminController = require('../controller/AdminController.js')

// 查询全部用户接口
router.get('/user', AdminController.searchUser)

module.exports = router